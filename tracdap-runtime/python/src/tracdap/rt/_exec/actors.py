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

from __future__ import annotations

import logging
import threading
import functools as func
import typing as tp
import enum
import dataclasses as dc
import inspect

import tracdap.rt._impl.util as util  # noqa
import tracdap.rt.exceptions as _ex


ActorId = str


class EBadActor(_ex.ETracInternal):

    """
    A bad request has been made to the actors subsystem, e.g. an invalid message or message params

    This error should only be raised where client code has made an invalid or illegal request to the actor system.
    Errors or inconsistencies in the actor module itself are internal bugs and should be marked by raising EUnexpected.
    """

    pass


class ActorState(enum.Enum):

    NOT_STARTED = 0
    STARTING = 1
    RUNNING = 2
    STOPPING = 3
    STOPPED = 4
    ERROR = 5
    FAILED = 6


@dc.dataclass(frozen=True)
class Msg:

    sender: ActorId
    target: ActorId
    message: str

    args: tp.List[tp.Any] = dc.field(default_factory=list)
    kwargs: tp.Dict[str, tp.Any] = dc.field(default_factory=dict)


@dc.dataclass(frozen=True)
class Signal(Msg):
    pass


@dc.dataclass(frozen=True)
class ErrorSignal(Signal):

    error: Exception = None
    origin: ActorId = None


class SignalNames:

    PREFIX = "actor:"

    START = "actor:start"
    STOP = "actor:stop"

    STARTED = "actor:started"
    STOPPED = "actor:stopped"
    FAILED = "actor:failed"

    CONTROL_SIGNALS = [START, STOP]


class EventLoop:

    def __init__(self):
        self.__msg_lock = threading.Condition()
        self.__msg_queue: tp.List[tp.Tuple[Msg, tp.Callable[[Msg], None]]] = []
        self.__shutdown = False
        self.__shutdown_now = False
        self.__log = util.logger_for_object(self)

    def post_message(self, msg: Msg, processor: tp.Callable[[Msg], None]):
        with self.__msg_lock:
            if self.__shutdown:
                raise EBadActor("System is already shutting down")
            else:
                self.__msg_queue.append((msg, processor))
                self.__msg_lock.notify()

    def shutdown(self, immediate: bool = False):
        with self.__msg_lock:
            self.__shutdown = True
            self.__shutdown_now = immediate
            self.__msg_lock.notify()

    def main(self):
        self._event_loop()

    def _event_loop(self):

        done = False

        while not done:

            with self.__msg_lock:

                self.__msg_lock.wait_for(lambda: len(self.__msg_queue) > 0 or self.__shutdown)
                event = self.__msg_queue.pop() if len(self.__msg_queue) > 0 else None

                if self.__shutdown_now:
                    break

                if self.__shutdown and event is None:
                    done = True

            if event is not None:
                try:
                    msg, processor = event
                    processor(msg)
                except Exception as e:
                    self.__log.error(f"Unhandled error on the event loop: {str(e)}")
                    self.__log.exception(e)

        print("Event loop exit")


class Message:

    def __init__(self, method):
        self.__method = method
        func.update_wrapper(self, method)

        params = inspect.signature(method).parameters
        self.params = list(params.values())[1:]  # skip 'self' parameter
        self.type_hints = tp.get_type_hints(method)

    def __call__(self, *args, **kwargs):
        self.__method(*args, **kwargs)


class Actor:

    __class_handlers: tp.Dict[type, tp.Dict[str, tp.Callable]] = dict()
    __log: tp.Optional[util.logging.Logger] = None

    def __init__(self):
        self.__handlers = self._inspect_handlers()
        self.__state = ActorState.NOT_STARTED
        self.__error: tp.Optional[Exception] = None
        self.__ctx: tp.Optional[ActorContext] = None

    def state(self) -> ActorState:
        return self.__state

    def error(self) -> tp.Optional[Exception]:
        return self.__error

    def actors(self) -> ActorContext:
        return self.__ctx

    def on_start(self):
        pass

    def on_stop(self):
        pass

    def on_signal(self, signal: Signal) -> tp.Optional[bool]:
        return None

    def _inspect_handlers(self) -> tp.Dict[str, tp.Callable]:

        known_handlers = Actor.__class_handlers.get(self.__class__)

        if known_handlers:
            return known_handlers

        handlers = dict()

        for member in self.__class__.__dict__.values():
            if isinstance(member, Message):
                handlers[member.__name__] = member

        Actor.__class_handlers[self.__class__] = handlers
        return handlers

    def _receive_message(self, node: ActorNode, ctx: ActorContext, msg: Msg):

        try:
            self.__ctx = ctx

            handler = self.__handlers.get(msg.message)

            if handler:
                handler(self, *msg.args, **msg.kwargs)
                self._check_for_fail(ctx)

            else:
                # Unhandled messages are dropped, with just a warning in the log
                log = util.logger_for_class(Actor)
                log.warning(f"Message ignored: [{msg.message}] {msg.sender} -> {msg.target}" +
                            f" (actor {self.__class__.__name__} does not support this message)")

        except Exception as error:

            self.__state = ActorState.ERROR
            self.__error = error
            node.report_error(msg.message, error)  # noqa

        finally:
            self.__ctx = None

    def _receive_signal(self, node: ActorNode, ctx: ActorContext, signal: Signal) -> tp.Optional[bool]:

        try:
            self.__ctx = ctx

            if signal.message == SignalNames.START:
                self._require_state([ActorState.NOT_STARTED, ActorState.STARTING])
                self.on_start()
                self._check_for_fail(ctx)
                self.__state = ActorState.RUNNING if self.__error is None else ActorState.FAILED
                return True

            elif signal.message == SignalNames.STOP:
                self._require_state([ActorState.RUNNING, ActorState.STOPPING, ActorState.ERROR])
                self.on_stop()
                self._check_for_fail(ctx)
                self.__state = ActorState.STOPPED if self.__error is None else ActorState.FAILED
                return True

            else:
                signal_result = self.on_signal(signal)
                self._check_for_fail(ctx)
                return signal_result

        except Exception as error:

            self.__state = ActorState.ERROR
            self.__error = error
            node.report_error(signal.message, error)  # noqa
            return None

        finally:
            self.__ctx = None

    def _check_for_fail(self, ctx: ActorContext):

        failure = ctx._ActorContext__error  # noqa
        if failure:
            self.__state = ActorState.ERROR
            self.__error = failure

    def _require_state(self, allowed_states: tp.List[ActorState]):

        # The actor system should prevent, reject or discard out-of-sequence lifecycle events
        # If one gets through this is an unexpected error

        if self.__state not in allowed_states:

            msg = "Actor lifecycle signal received out of sequence"
            self.__log.error(msg)
            raise _ex.EUnexpected(msg)


class ActorContext:

    def __init__(
            self, node: ActorNode, message: str,
            current_actor: ActorId, parent: ActorId, sender: tp.Optional[ActorId]):

        self.__node = node
        self.__message = message
        self.__id = current_actor
        self.__parent = parent
        self.__sender = sender
        self.__error: tp.Optional[Exception] = None

        self.id = current_actor
        self.parent = parent
        self.sender = sender

    def spawn(self, actor_class: Actor.__class__, *args, **kwargs) -> ActorId:
        return self.spawn_instance(actor_class(*args, **kwargs))

    def spawn_instance(self, actor: Actor) -> ActorId:
        return self.__node.spawn(actor)

    def send(self, target_id: ActorId, message: str, *args, **kwargs):
        self.__node.send_message(self.__id, target_id, message, args, kwargs)

    def send_parent(self, message: str, *args, **kwargs):
        self.__node.send_message(self.__id, self.__parent, message, args, kwargs)

    def reply(self, message: str, *args, **kwargs):
        self.__node.send_message(self.__id, self.__sender, message, args, kwargs)

    def stop(self, target_id: tp.Optional[ActorId] = None):

        if target_id:
            self.__node.send_signal(self.__id, target_id, SignalNames.STOP)
        else:
            self.__node.send_signal(self.__id, self.__id, SignalNames.STOP)

    def fail(self, error: Exception, cause: tp.Optional[Exception] = None):

        if not error.__cause__:
            error.__cause__ = cause

        self.__error = error

        if self.__message not in [SignalNames.START, SignalNames.STOP]:
            self.__node.send_signal(self.__id, self.__id, SignalNames.STOP)


class ActorNode:

    _log: tp.Optional[logging.Logger]

    def __init__(
            self, actor_id: ActorId, actor: Actor,
            parent: ActorNode, system: ActorSystem,
            event_loop: EventLoop):

        self.actor_id = actor_id
        self.actor = actor
        self.parent = parent
        self.system = system
        self.event_loop = event_loop

        self.children: tp.Dict[ActorId, ActorNode] = {}
        self.next_child_number: int = 0

    def spawn(self, child_actor: Actor):

        actor_class = type(child_actor)
        event_loop = self.system._allocate_event_loop(actor_class)  # noqa

        child_id = self._new_child_id(actor_class)
        child_node = ActorNode(child_id, child_actor, self, self.system, event_loop)
        self.children[child_id] = child_node

        child_node.send_signal(self.actor_id, child_id, SignalNames.START)

        return child_id

    def send_message(self, sender_id: ActorId, target_id: ActorId, message: str, args, kwargs):

        self._log.info(f"send_signal [{self.actor_id}]: [{message}] {sender_id} -> {target_id}")

        # Client code could try to send a signal string as a message, this counts as a bad actor

        if message.startswith(SignalNames.PREFIX):

            message = f"Invalid message: {sender_id} [{message}] -> {target_id}" \
                      + f" ([{message} looks like a signal, signals cannot be sent with send_message)"

            self._log.error(message)
            raise EBadActor(message)

        # TODO: Send-time type check

        # target = self._lookup_actor_node(target_id)
        # target_class = target.actor.__class__
        # self._check_message_signature(target_id, target_class, message, args, kwargs)

        msg = Msg(sender_id, target_id, message, args or [], kwargs or {})

        self._post_message(msg)

    def send_signal(self, sender_id: ActorId, target_id: ActorId, signal: str, error: tp.Optional[Exception] = None):

        self._log.info(f"send_signal [{self.actor_id}]: [{signal}] {sender_id} -> {target_id}")

        # Only the actor system can send signals, so a bad signal is an unexpected error

        if not signal.startswith(SignalNames.PREFIX):
            raise _ex.EUnexpected()

        # Client code could submit an invalid control request, this counts as a bad actor

        controllers = ["/system", target_id, ActorSystem._parent_id(target_id)]

        if signal in SignalNames.CONTROL_SIGNALS and sender_id not in controllers:

            message = f"Stop request rejected: [{SignalNames.STOP}] -> {target_id}" + \
                      f" ({sender_id} is not allowed to stop this actor)"

            self._log.error(message)
            raise EBadActor(message)

        if signal == SignalNames.FAILED:
            msg = ErrorSignal(sender_id, target_id, signal, error=error)
        else:
            msg = Signal(sender_id, target_id, signal)

        self._post_message(msg)

    def report_error(self, message: str, error: Exception):

        self._log.error(f"{self.actor_id} [{message}]: {str(error)}")
        self._log.exception(error)
        self._log.error(f"Actor failed: {self.actor_id} [{message}] (actor will be stopped)")

        # Dp not send STOP signal for errors that occur while processing START or STOP
        # In this case, directly set the state to FAILED and send a FAILED notification to the parent
        if message in [SignalNames.START, SignalNames.STOP]:
            self.actor._Actor__state = ActorState.FAILED
            if self.parent is not None:
                # TODO: Wrap for propagation?
                self.send_signal(self.actor_id, self.parent.actor_id, SignalNames.FAILED, error)

        # Otherwise stop the actor, a FAILED notification will be generated when the STOP signal is processed
        else:
            self.send_signal("/system", self.actor_id, SignalNames.STOP)

    def _post_message(self, msg: Msg):

        self._log.info(f"_post_message [{self.actor_id}]: [{msg.message}] {msg.sender} -> {msg.target}")

        if msg.target == self.actor_id:
            self._accept_message(msg)

        elif self.parent and msg.target == self.parent.actor_id:
            self.parent._accept_message(msg)

        elif msg.target in self.children:
            self.children[msg.target]._accept_message(msg)

        elif msg.target.startswith(self.actor_id + "/"):
            self._log.warning(f"Target actor not found: {msg.target}")  # todo

        elif self.parent:
            self.parent._post_message(msg)

        else:
            self._log.warning(f"Target actor not found: {msg.target}")  # todo

    def _accept_message(self, msg: Msg):

        if msg.message.startswith(SignalNames.PREFIX):
            self.event_loop.post_message(msg, self._process_signal)
        else:
            self.event_loop.post_message(msg, self._process_message)

    def _process_message(self, msg: Msg):

        self._log.info(f"_process_message [{self.actor_id}]: [{msg.message}] {msg.sender} -> {msg.target}")

        if not self._check_message(msg):
            return

        parent_id = self.parent.actor_id if self.parent else None
        ctx = ActorContext(self, msg.message, self.actor_id, parent_id, msg.sender)

        self.actor._receive_message(self, ctx, msg)  # noqa

    def _process_signal(self, signal: Msg):

        self._log.info(f"_process_signal [{self.actor_id}]: [{signal.message}] {signal.sender} -> {signal.target}")

        if not self._check_message(signal):
            return

        parent_id = self.parent.actor_id if self.parent else None
        ctx = ActorContext(self, signal.message, self.actor_id, parent_id, signal.sender)

        result = self.actor._receive_signal(self, ctx, signal)  # noqa

        # todo shutdown ordering

        # Propagate stop signals

        if signal == SignalNames.STOP:
            for child_id, child_node in self.children.items():
                child_node.send_signal(self.actor_id, child_id, SignalNames.STOP)

        # Generate lifecycle notifications

        if signal.message == SignalNames.START and self.parent is not None:
            if self.actor.state() == ActorState.RUNNING:
                self.send_signal(self.actor_id, self.parent.actor_id, SignalNames.STARTED)

        if signal.message == SignalNames.STOP and self.parent is not None:
            if self.actor.error():
                self.send_signal(self.actor_id, self.parent.actor_id, SignalNames.FAILED, self.actor.error())
            else:
                self.send_signal(self.actor_id, self.parent.actor_id, SignalNames.STOPPED)

        # Error propagation
        # When an actor dies due to an error, a FAILED signal is generated and sent to its direct parent
        # If the parent does not handle the error successfully, the parent also dies and the error propagates

        if isinstance(signal, ErrorSignal) and ActorSystem._parent_id(signal.sender) == self.actor_id:
            if result is not True:
                self.actor._Actor__error = signal.error  # TODO: Error could be wrapped to indicate propagation?
                self.send_signal("/system", self.actor_id, SignalNames.STOP)
                if self.parent is not None:
                    self.send_signal(self.actor_id, self.parent.actor_id, SignalNames.FAILED, signal.error)

        # Remove dead actors
        # If the actor is now stopped or failed, take it out of the registry

        if signal.message in [ActorState.STOPPED, ActorState.FAILED] and signal.sender in self.children:
            self.children.pop(signal.sender)

    def _check_message(self, msg: Msg):

        if msg.target != self.actor_id:

            message = f"Message ignored: [{msg.message}] -> {msg.target}" + \
                      f" ({self.actor_id} received this message by mistake)"

            self._log.warning(message)
            return False

        controllers = ["/system", self.actor_id, ActorSystem._parent_id(self.actor_id)]

        if msg.message in SignalNames.CONTROL_SIGNALS and msg.sender not in controllers:

            message = f"Control signal ignored: [{msg.message}] -> {self.actor_id}" + \
                      f" ({msg.sender} is not allowed to stop this actor)"

            self._log.warning(message)
            return False

        return True

    def _new_child_id(self, actor_class: Actor.__class__) -> ActorId:

        classname = actor_class.__name__.lower()
        child_name = f"{classname}-{self.next_child_number}"

        self.next_child_number += 1

        if self.actor_id == "/":  # TODO
            return f"/{child_name}"
        else:
            return f"{self.actor_id}/{child_name}"


class RootActor(Actor):

    def __init__(self, main_actor: Actor, started: threading.Event, stopped: threading.Event):
        super().__init__()
        self.main_id: tp.Optional[ActorId] = None
        self.main_actor = main_actor
        self._started = started
        self._stopped = stopped

    def on_start(self):
        self.main_id = self.actors().spawn_instance(self.main_actor)

    def on_stop(self):
        self._stopped.set()

    def on_signal(self, signal: Signal) -> tp.Optional[bool]:

        if signal.sender == self.main_id:

            if signal.message == SignalNames.STARTED:
                self._started.set()
                return True

            if signal.message == SignalNames.STOPPED:
                self.actors().stop(self.actors().id)
                return True

            if signal.message == SignalNames.FAILED and isinstance(signal, ErrorSignal):
                if not self._started.is_set():
                    self._started.set()
                self.actors().fail(signal.error)
                return True

        return False


class ActorSystem:

    __DELIMITER = "/"
    __ROOT_ID = __DELIMITER

    def __init__(self, main_actor: Actor, system_thread: str = "actor_system"):

        super().__init__()

        self._log = util.logger_for_object(self)

        # self.__actors: tp.Dict[ActorId, ActorNode] = {self.__ROOT_ID: ActorNode("", self.__ROOT_ID, None)}
        # self.__message_queue: tp.List[Msg] = list()

        self.__system_event_loop = EventLoop()
        self.__system_thread = threading.Thread(
            name=system_thread,
            target=self.__system_event_loop.main)

        self.__root_started = threading.Event()
        self.__root_stopped = threading.Event()
        self.__root_actor = RootActor(main_actor, self.__root_started, self.__root_stopped)
        self.__root_node = ActorNode(self.__ROOT_ID, self.__root_actor, None, self, self.__system_event_loop)

    # Public API

    def start(self, wait=True):

        self.__system_thread.start()
        self.__root_node.send_signal("/system", self.__ROOT_ID, SignalNames.START)

        if wait:
            self.__root_started.wait()  # TODO: Startup timeout

    def stop(self):

        self.__root_node.send_signal("/system", self.__ROOT_ID, SignalNames.STOP)

    def wait_for_shutdown(self):

        self.__root_stopped.wait()   # TODO: Timeout
        self.__system_event_loop.shutdown()
        self.__system_thread.join()

    def shutdown_code(self) -> int:

        if self.__root_actor.state() == ActorState.STOPPED and not self.__root_actor.error():
            return 0
        else:
            return -1

    def shutdown_error(self) -> tp.Optional[Exception]:

        return self.__root_actor.error()

    def send(self, message: str, *args, **kwargs):

        if self.__root_actor.main_id is None:
            raise EBadActor("System has not started yet")

        self.__root_node.send_message("/external", self.__root_actor.main_id, message, args, kwargs)

    def _allocate_event_loop(self, actor_class: Actor.__class__) -> EventLoop:

        return self.__system_event_loop

    @classmethod
    def _parent_id(cls, actor_id: ActorId) -> ActorId:

        parent_delim = actor_id.rfind(cls.__DELIMITER)
        parent_id = cls.__ROOT_ID if parent_delim == 0 else actor_id[:parent_delim]

        return parent_id

    def _check_message_signature(self, target_id: ActorId, target_class: Actor.__class__, message: str, args, kwargs):

        target_handler = Actor._Actor__class_handlers.get(target_class).get(message)  # noqa

        if target_handler is None:
            error = f"Invalid message: [{message}] -> {target_id} (unknown message '{message}')"
            self._log.error(error)
            raise EBadActor(error)

        target_params = target_handler.params
        type_hints = target_handler.type_hints

        if len(args) + len(kwargs) > len(target_params):
            error = f"Invalid message: [{message}] -> {target_id} (too many arguments)"
            self._log.error(error)
            raise EBadActor(error)

        pos_params = target_params[:len(args)]
        kw_params = target_params[len(args):]
        kw_param_names = set(map(lambda p: p.name, kw_params))

        # Missing params
        for param in kw_params:
            if param.default is inspect._empty and param.name not in kwargs:  # noqa
                error = f"Invalid message: [{message}] -> {target_id} (missing required parameter '{param.name}')"
                self._log.error(error)
                raise EBadActor(error)

        # Extra (unknown) kw params
        for param_name in kwargs.keys():
            if param_name not in kw_param_names:
                error = f"Invalid message: [{message}] -> {target_id} (unknown parameter '{param_name}')"
                self._log.error(error)
                raise EBadActor(error)

        # Positional arg types
        for pos_param, pos_arg in zip(pos_params, args):

            type_hint = type_hints.get(pos_param.name)

            # todo: support generics

            # If no type hint is available, allow anything through
            if type_hint is not None and not isinstance(pos_arg, type_hint):
                error = f"Invalid message: [{message}] -> {target_id} (wrong parameter type for '{pos_param.name}')"
                self._log.error(error)
                raise EBadActor(error)

        # Kw arg types
        for kw_param in kw_params:

            kw_arg = kwargs.get(kw_param.name)
            type_hint = type_hints.get(kw_param.name)

            # If param has taken a default value, no type check is needed
            if kw_arg is None:
                continue

            # If no type hint is available, allow anything through
            if type_hint is not None and not isinstance(kw_arg, type_hint):
                error = f"Invalid message: [{message}] -> {target_id} (wrong parameter type for '{kw_param.name}')"
                self._log.error(error)
                raise EBadActor(error)


# Static loggers can only be set after class Actor is declared
Actor._Actor__log = util.logger_for_class(Actor)
ActorNode._log = util.logger_for_class(ActorNode)
