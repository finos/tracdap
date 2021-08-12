#  Copyright 2021 Accenture Global Solutions Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import trac.rt.impl.util as util
import trac.rt.exec.actors as actors
import unittest


# Dummy app-layer exception class
class AppException(Exception):
    pass


class ActorSystemTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        util.configure_logging()

    def test_actor_lifecycle(self):

        # The most basic test case:
        # Start one actor, it processes a single message and stops

        results = []

        class TestActor(actors.Actor):

            def on_start(self):
                results.append("on_start")

            def on_stop(self):
                results.append("on_stop")

            @actors.Message
            def sample_message(self, value):
                results.append("sample_message")
                results.append(value)
                self.actors().stop()

        root = TestActor()
        system = actors.ActorSystem(root)
        system.start()
        system.send("sample_message", 1)
        system.wait_for_shutdown()

        self.assertEqual(["on_start", "sample_message", 1, "on_stop"], results)

        # Make sure the system went down cleanly
        self.assertEqual(0, system.shutdown_code())

    def test_unknown_actor_ignored(self):

        # Messages sent to an unknown actor ID are silently dropped (there is a warning in the logs)

        results = []

        class TestActor(actors.Actor):

            def on_start(self):
                results.append("on_start")
                self.actors().send("/nonexistent/actor", "sample_message", 1)
                self.actors().send(self.actors().id, "sample_message", 1)

            def on_stop(self):
                results.append("on_stop")

            @actors.Message
            def sample_message(self, value):
                results.append("sample_message")
                results.append(value)
                self.actors().stop()

        root = TestActor()
        system = actors.ActorSystem(root)
        system.start()
        system.wait_for_shutdown()

        self.assertEqual(["on_start", "sample_message", 1, "on_stop"], results)
        self.assertEqual(0, system.shutdown_code())

    def test_bad_message_params(self):

        errors = []

        class TargetActor(actors.Actor):

            @actors.Message
            def sample_message(self, value: int):
                pass

            @actors.Message
            def sample_message_2(self, value: int, other: str = ''):
                pass

            @actors.Message
            def sample_with_default(self, value: int = 0):
                pass

        class TestActor(actors.Actor):

            def on_start(self):

                target_id = self.actors().spawn(TargetActor)

                # All these bad calls to send() should result in EBadActor being thrown

                try:
                    self.actors().send(target_id, "signal:STOP")
                except actors.EBadActor:
                    errors.append("illegal_message")

                try:
                    self.actors().send(target_id, "unknown_message")
                except actors.EBadActor:
                    errors.append("unknown_message")

                try:
                    self.actors().send(target_id, "sample_message_2", 1, unknown=2)
                except actors.EBadActor:
                    errors.append("unknown_param")

                try:
                    self.actors().send(target_id, "sample_message")
                except actors.EBadActor:
                    errors.append("missing_param")

                try:
                    self.actors().send(target_id, "sample_message", 1, 2)
                except actors.EBadActor:
                    errors.append("extra_param")

                try:
                    self.actors().send(target_id, "sample_message", "wrong_param_type")
                except actors.EBadActor:
                    errors.append("wrong_param_type")

                try:
                    self.actors().send(target_id, "sample_message", value="wrong_kw_param_type")
                except actors.EBadActor:
                    errors.append("wrong_kw_param_type")

                # Should not error, parameter 'value' should take the default
                self.actors().send(target_id, "sample_with_default")

                self.actors().stop()

        root = TestActor()
        system = actors.ActorSystem(root)
        system.start()
        system.wait_for_shutdown()

        self.assertEqual([
            "illegal_message", "unknown_message", "unknown_param",
            "missing_param", "extra_param",
            "wrong_param_type", "wrong_kw_param_type"],
            errors)

        # System should have gone down cleanly, errors are caught where they occur
        self.assertEqual(0, system.shutdown_code())

    def test_explicit_signals_not_allowed(self):

        # Actors cannot explicitly send signals (signals are system messages prefixed 'actor:')

        results = []

        class TestActor(actors.Actor):

            def on_start(self):

                try:
                    self.actors().send("/nonexistent/actor", "actor:any_signal")
                except Exception:  # noqa
                    results.append("explicit_signal_failed")

                self.actors().stop()

        root = TestActor()
        system = actors.ActorSystem(root)
        system.start()
        system.wait_for_shutdown()

        self.assertEqual(["explicit_signal_failed"], results)
        self.assertEqual(0, system.shutdown_code())

    def test_actor_failure_1(self):

        # Actor throws an error while processing a message

        results = []

        class TestActor(actors.Actor):

            def on_start(self):
                results.append("on_start")

            def on_stop(self):
                results.append("on_stop")

            @actors.Message
            def sample_message(self, value):
                results.append("sample_message")
                results.append(value)
                raise AppException("err_code_1")

        root = TestActor()
        system = actors.ActorSystem(root)
        system.start()
        system.send("sample_message", 1)
        system.wait_for_shutdown()

        # Actor should receive on_stop after raising the error
        self.assertEqual(["on_start", "sample_message", 1, "on_stop"], results)

        code = system.shutdown_code()
        error = system.shutdown_error()
        self.assertNotEqual(0, code)
        self.assertIsInstance(error, AppException)
        self.assertEqual("err_code_1", error.args[0])

    def test_actor_failure_2(self):

        # Actor throws an error during on_start

        results = []

        class TestActor(actors.Actor):

            def on_start(self):
                results.append("on_start")
                raise AppException("err_code_2")

            def on_stop(self):
                results.append("on_stop")

            @actors.Message
            def sample_message(self, value):
                results.append("sample_message")
                results.append(value)
                self.actors().stop()

        root = TestActor()
        system = actors.ActorSystem(root)
        system.start()
        system.send("sample_message", 1)
        system.wait_for_shutdown()

        # Actor should not receive on_stop if on_start fails
        self.assertEqual(["on_start"], results)

        code = system.shutdown_code()
        error = system.shutdown_error()
        self.assertNotEqual(0, code)
        self.assertIsInstance(error, AppException)
        self.assertEqual("err_code_2", error.args[0])

    def test_actor_failure_3(self):

        # Actor throws an error during on_stop

        results = []

        class TestActor(actors.Actor):

            def on_start(self):
                results.append("on_start")

            def on_stop(self):
                results.append("on_stop")
                raise AppException("err_code_3")

            @actors.Message
            def sample_message(self, value):
                results.append("sample_message")
                results.append(value)
                self.actors().stop()

        root = TestActor()
        system = actors.ActorSystem(root)
        system.start()
        system.send("sample_message", 1)
        system.wait_for_shutdown()

        self.assertEqual(["on_start", "sample_message", 1, "on_stop"], results)

        code = system.shutdown_code()
        error = system.shutdown_error()
        self.assertNotEqual(0, code)
        self.assertIsInstance(error, AppException)
        self.assertEqual("err_code_3", error.args[0])

    def test_child_lifecycle(self):

        # Parent creates one child and sends it a message
        # Child processes one message and stops, child.on_stop should be called
        # child stopped signal should be received in the parent

        results = []

        class ChildActor(actors.Actor):

            def on_start(self):
                results.append("child_start")
                self.actors().send_parent("child_started", self.actors().id)

            def on_stop(self):
                results.append("child_stop")

        class ParentActor(actors.Actor):

            def __init__(self):
                super().__init__()
                self.child_id = None

            def on_start(self):
                results.append("parent_start")
                self.child_id = self.actors().spawn(ChildActor)

            def on_stop(self):
                results.append("parent_stop")

            def on_signal(self, signal: actors.Signal) -> bool:

                if signal.sender == self.child_id:
                    results.append("parent_signal")
                    results.append(signal.message)

                self.actors().stop()
                return True

            @actors.Message
            def child_started(self, child_id):
                results.append("child_started")
                self.actors().stop(child_id)

        root = ParentActor()
        system = actors.ActorSystem(root)
        system.start()
        system.wait_for_shutdown()

        self.assertEqual([
            "parent_start",
            "child_start", "child_started",
            "child_stop", "parent_signal", "actor:stopped",
            "parent_stop"],
            results)

        # Make sure the system went down cleanly
        self.assertEqual(0, system.shutdown_code())

    def test_multiple_children(self):

        results = []

        class ChildActor(actors.Actor):

            def on_start(self):
                results.append("child_start")
                self.actors().send_parent("new_child")

            def on_stop(self):
                results.append("child_stop")

        class ParentActor(actors.Actor):

            def __init__(self):
                super().__init__()
                self.child_count = 0

            def on_start(self):
                results.append("parent_start")
                self.actors().spawn(ChildActor)

            def on_stop(self):
                results.append("parent_stop")

            @actors.Message
            def new_child(self):
                results.append("new_child")
                self.child_count += 1

                if self.child_count < 3:
                    self.actors().spawn(ChildActor)
                else:
                    self.actors().stop()

        root = ParentActor()
        system = actors.ActorSystem(root)
        system.start()
        system.wait_for_shutdown()

        self.assertEqual([
            "parent_start",
            "child_start", "new_child",
            "child_start", "new_child",
            "child_start", "new_child",
            "child_stop", "child_stop", "child_stop",
            "parent_stop"],
            results)

        # Make sure the system went down cleanly
        self.assertEqual(0, system.shutdown_code())

    def test_child_shutdown_order(self):

        results = []

        class Grandchild(actors.Actor):

            def __init__(self, root_id: actors.ActorId):
                super().__init__()
                self.root_id = root_id

            def on_start(self):
                results.append("grandchild_start")
                self.actors().send(self.root_id, "grandchild_started")

            def on_stop(self):
                results.append("grandchild_stop")

        class Child(actors.Actor):

            def on_start(self):
                results.append("child_start")
                self.actors().spawn(Grandchild, self.actors().parent)

            def on_stop(self):
                results.append("child_stop")

        class Parent(actors.Actor):

            def on_start(self):
                results.append("parent_start")
                self.actors().spawn(Child)

            def on_stop(self):
                results.append("parent_stop")

            @actors.Message
            def grandchild_started(self):
                results.append("grandchild_started")
                self.actors().stop()

        root = Parent()
        system = actors.ActorSystem(root)
        system.start()
        system.wait_for_shutdown()

        self.assertEqual([
            "parent_start", "child_start", "grandchild_start",
            "grandchild_started",
            "grandchild_stop", "child_stop", "parent_stop"],
            results)

        # Make sure the system went down cleanly
        self.assertEqual(0, system.shutdown_code())

    def test_stop_sibling_not_allowed(self):

        # Actors are only allowed to stop themselves or their direct children

        results = []

        class ChildActor(actors.Actor):

            def __init__(self, other_id):
                super().__init__()
                self.other_id = other_id

            def on_start(self):

                if self.other_id:
                    try:
                        self.actors().stop(self.other_id)
                    except Exception:  # noqa
                        results.append("stop_other_failed")

                self.actors().send_parent("child_up")

        class ParentActor(actors.Actor):

            def __init__(self):
                super().__init__()
                self.child_count = 0

            def on_start(self):
                child1 = self.actors().spawn(ChildActor, None)
                self.actors().spawn(ChildActor, child1)

            @actors.Message
            def child_up(self):
                self.child_count += 1
                if self.child_count == 2:
                    self.actors().stop()

        root = ParentActor()
        system = actors.ActorSystem(root)
        system.start()
        system.wait_for_shutdown()

        self.assertEqual(["stop_other_failed"], results)
        self.assertEqual(0, system.shutdown_code())

    def test_child_failure_1(self):

        # Child throws an error while processing a message

        results = []

        class ChildActor(actors.Actor):

            def on_start(self):
                results.append("child_start")

            def on_stop(self):
                results.append("child_stop")

            @actors.Message
            def sample_message(self, value):
                results.append("sample_message")
                results.append(value)
                raise AppException("err_code_1")

        class ParentActor(actors.Actor):

            def on_start(self):

                results.append("parent_start")

                child_id = self.actors().spawn(ChildActor)
                self.actors().send(child_id, "sample_message", 1)

            def on_stop(self):
                results.append("parent_stop")

        root = ParentActor()
        system = actors.ActorSystem(root)
        system.start()
        system.wait_for_shutdown()

        # Both parent and child should receive stop after child errors
        self.assertEqual([
            "parent_start", "child_start",
            "sample_message", 1,
            "child_stop", "parent_stop"], results)

        # Error info should propagate up
        code = system.shutdown_code()
        error = system.shutdown_error()
        self.assertNotEqual(0, code)
        self.assertIsInstance(error, AppException)
        self.assertEqual("err_code_1", error.args[0])

    def test_child_failure_2(self):

        # Child throws an error in on_start

        results = []

        class ChildActor(actors.Actor):

            def on_start(self):
                results.append("child_start")
                raise AppException("err_code_2")

            def on_stop(self):
                results.append("child_stop")

            @actors.Message
            def sample_message(self, value):
                results.append("sample_message")
                results.append(value)

        class ParentActor(actors.Actor):

            def on_start(self):

                results.append("parent_start")

                child_id = self.actors().spawn(ChildActor)
                self.actors().send(child_id, "sample_message", 1)

            def on_stop(self):
                results.append("parent_stop")

        root = ParentActor()
        system = actors.ActorSystem(root)
        system.start()
        system.wait_for_shutdown()

        # Child does not receive a stop message because it did not start successfully
        # Parent still receives stop, because it does not handle failure of the child
        self.assertEqual([
            "parent_start", "child_start",
            "parent_stop"], results)

        # Error info should propagate up
        code = system.shutdown_code()
        error = system.shutdown_error()
        self.assertNotEqual(0, code)
        self.assertIsInstance(error, AppException)
        self.assertEqual("err_code_2", error.args[0])

    def test_child_failure_3(self):

        # Child throws an error in on_stop

        results = []

        class ChildActor(actors.Actor):

            def on_start(self):
                results.append("child_start")

            def on_stop(self):
                results.append("child_stop")
                raise AppException("err_code_3")

            @actors.Message
            def sample_message(self, value):
                results.append("sample_message")
                results.append(value)
                self.actors().send_parent("stop_child")

        class ParentActor(actors.Actor):

            def __init__(self):
                super().__init__()
                self.child_id = None

            def on_start(self):

                results.append("parent_start")

                self.child_id = self.actors().spawn(ChildActor)
                self.actors().send(self.child_id, "sample_message", 1)

            def on_stop(self):
                results.append("parent_stop")

            @actors.Message
            def stop_child(self):
                results.append("stop_child")
                self.actors().stop(self.child_id)

        root = ParentActor()
        system = actors.ActorSystem(root)
        system.start()
        system.wait_for_shutdown()

        # Parent still receives a stop signal
        # Child failure in on_stop still raises a failure signal, which is not handled in the parent
        self.assertEqual([
            "parent_start", "child_start",
            "sample_message", 1, "stop_child",
            "child_stop", "parent_stop"], results)

        # Error info should propagate up
        code = system.shutdown_code()
        error = system.shutdown_error()
        self.assertNotEqual(0, code)
        self.assertIsInstance(error, AppException)
        self.assertEqual("err_code_3", error.args[0])

    def test_child_failure_signals(self):

        results = []

        class ChildActor(actors.Actor):

            def on_start(self):
                results.append("child_start")

            def on_stop(self):
                results.append("child_stop")

            @actors.Message
            def sample_message(self, value):
                results.append("sample_message")
                results.append(value)
                raise AppException("expected_error")

        class ParentActor(actors.Actor):

            def __init__(self):
                super().__init__()
                self.child_id = None

            def on_start(self):

                results.append("parent_start")

                self.child_id = self.actors().spawn(ChildActor)
                self.actors().send(self.child_id, "sample_message", 1)

            def on_stop(self):
                results.append("parent_stop")

            def on_signal(self, signal: actors.Signal):

                if signal.sender == self.child_id:
                    results.append("child_signal")
                    results.append(signal.message)

                self.actors().stop()

                # Intercept the signal - prevents propagation
                return True

        root = ParentActor()
        system = actors.ActorSystem(root)
        system.start()
        system.wait_for_shutdown()

        self.assertEqual([
            "parent_start", "child_start",
            "sample_message", 1,
            "child_stop", "child_signal", "actor:failed",
            "parent_stop"], results)

        # Since the failure signal was handled, there should be a clean shutdown
        self.assertEqual(0, system.shutdown_code())
