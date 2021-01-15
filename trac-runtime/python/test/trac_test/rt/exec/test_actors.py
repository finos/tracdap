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

    def test_bad_message_params(self):

        # What happens if a message is sent with the wrong parameters
        # Can this be an error at the sending site, if the target actor / message can be looked up?

        self.fail("not implemented")

    def test_actor_failure_1(self):

        # Actor throws an error while processing a message

        results = []

        class TestActor(actors.Actor):

            def on_start(self):
                results.append("on_start")

            def on_stop(self):
                results.append("on_stop")
                raise RuntimeError("err_code_1")

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

        # Actor should receive on_stop after raising the error
        self.assertEqual(["on_start", "sample_message", 1, "on_stop"], results)

        code = system.shutdown_code()
        error = system.shutdown_error()
        self.assertNotEqual(0, code)
        self.assertIsInstance(error, RuntimeError)
        self.assertEqual("err_code_1", error.args[0])

    def test_actor_failure_2(self):

        # Actor throws an error during on_start

        results = []

        class TestActor(actors.Actor):

            def on_start(self):
                results.append("on_start")
                raise RuntimeError("err_code_2")

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
        self.assertIsInstance(error, RuntimeError)
        self.assertEqual("err_code_2", error.args[0])

    def test_actor_failure_3(self):

        # Actor throws an error during on_stop

        results = []

        class TestActor(actors.Actor):

            def on_start(self):
                results.append("on_start")

            def on_stop(self):
                results.append("on_stop")
                raise RuntimeError("err_code_3")

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
        self.assertIsInstance(error, RuntimeError)
        self.assertEqual("err_code_3", error.args[0])

    @unittest.skip
    def test_child_lifecycle(self):

        # Parent creates one child and sends it a message
        # Child processes one message and stops, child.on_stop should be called
        # child stopped signal should be received in the parent

        results = []

        class ParentActor(actors.Actor):

            def __init__(self):
                super().__init__()
                self.child_id = None

            def on_start(self):
                self.child_id = self.actors().spawn(ChildActor)

            def on_signal(self, actor_id, signal):

                if actor_id == self.child_id:
                    results.append("parent_notified_signal")
                    results.append(signal)

                self.actors().stop()

            @actors.Message
            def child_started(self, child_id):
                results.append("parent_notified_start")
                self.actors().stop(child_id)

        class ChildActor(actors.Actor):

            def on_start(self):
                results.append("child_start")
                self.actors().send_parent("child_started", self.actors().id)

            def on_stop(self):
                results.append("child_stop")

        root = ParentActor()
        system = actors.ActorSystem(root)
        system.start()
        system.wait_for_shutdown()

        self.assertEqual([
            "child_start", "parent_notified_start",
            "child_stop", "parent_notified_signal", "actor:stopped"],
            results)

    def test_child_shutdown_order(self):
        self.fail("not implemented")

    def test_child_failure_1(self):
        self.fail("not implemented")

    def test_child_failure_2(self):
        self.fail("not implemented")

    def test_child_failure_3(self):
        self.fail("not implemented")

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

    def test_unknown_message_ignored(self):

        # Message types an actor does not know about are silently dropped (there is a warning in the logs)

        results = []

        class TestActor(actors.Actor):

            def on_start(self):
                results.append("on_start")
                self.actors().send(self.actors().id, "unknown_message", 1)
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

    def test_stop_not_allowed(self):

        # Actors are only allowed to stop themselves or their direct children

        self.fail("not implemented")
