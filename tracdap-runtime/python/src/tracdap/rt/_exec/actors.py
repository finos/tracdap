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

    def _receive_message(self, system: ActorSystem, ctx: ActorContext, msg: Msg):

        try:
            self.__ctx = ctx

            handler = self.__handlers.get(msg.message)

            if handler:
                handler(self, *msg.args, **msg.kwargs)
                self._check_for_fail(ctx)

            else:
                # Unhandled messages are dropped, with just a warning in the log
                log = util.logger_for_class(Actor)
                log.warning(f"Message ignored: [{msg.message}] -> {msg.target}" +
                            f" (actor {self.__class__.__name__} does not support this message)")

        except Exception as error:

            self.__state = ActorState.ERROR
            self.__error = error
            system._report_error(ctx.id, msg.message, error)  # noqa

        finally:
            self.__ctx = None

    def _receive_signal(self, system: ActorSystem, ctx: ActorContext, signal: Signal) -> tp.Optional[bool]:

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
            system._report_error(ctx.id, signal.message, error)  # noqa
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


# Static member __log can only be set after class Actor is declared
Actor._Actor__log = util.logger_for_class(Actor)


class ActorContext:

    def __init__(self, system: ActorSystem, message: str,
                 current_actor: ActorId, parent: ActorId, sender: tp.Optional[ActorId]):

        self.id = current_actor
        self.parent = parent
        self.sender = sender

        self.__system = system
        self.__message = message
        self.__id = current_actor
        self.__parent = parent
        self.__sender = sender
        self.__error: tp.Optional[Exception] = None

    def spawn(self, actor_class: Actor.__class__, *args, **kwargs) -> ActorId:
        return self.__system._spawn_actor(self.__id, actor_class, args, kwargs)  # noqa

    def send(self, target_id: ActorId, message: str, *args, **kwargs):
        self.__system._send_message(self.__id, target_id, message, args, kwargs)  # noqa

    def send_parent(self, message: str, *args, **kwargs):
        self.__system._send_message(self.__id, self.__parent, message, args, kwargs)  # noqa

    def reply(self, message: str, *args, **kwargs):
        self.__system._send_message(self.__id, self.__sender, message, args, kwargs)  # noqa

    def stop(self, target_id: tp.Optional[ActorId] = None):

        if target_id:
            self.__system._stop_actor(self.__id, target_id)  # noqa
        else:
            self.__system._stop_actor(self.__id, self.__id)  # noqa

    def fail(self, error: Exception, cause: tp.Optional[Exception] = None):

        if not error.__cause__:
            error.__cause__ = cause

        self.__error = error

        if self.__message not in [SignalNames.START, SignalNames.STOP]:
            self.__system._stop_actor(self.__id, self.__id)  # noqa


class Message:

    def __init__(self, method):
        self.__method = method
        func.update_wrapper(self, method)

        params = inspect.signature(method).parameters
        self.params = list(params.values())[1:]  # skip 'self' parameter
        self.type_hints = tp.get_type_hints(method)

    def __call__(self, *args, **kwargs):
        self.__method(*args, **kwargs)


class SignalNames:

    PREFIX = "actor:"

    START = "actor:start"
    STOP = "actor:stop"

    STARTED = "actor:started"
    STOPPED = "actor:stopped"
    FAILED = "actor:failed"


@dc.dataclass(frozen=True)
class ActorNode:

    parent_id: ActorId
    actor_id: ActorId
    actor: Actor

    children: tp.FrozenSet[ActorId] = dc.field(default_factory=frozenset)
    next_child_number: int = 0

    def with_child(self, child_id: ActorId):
        children = self.children.union([child_id])
        return ActorNode(self.parent_id, self.actor_id, self.actor, children, self.next_child_number + 1)

    def without_child(self, child_id: ActorId):
        children = frozenset(filter(lambda c: c != child_id, self.children))
        return ActorNode(self.parent_id, self.actor_id, self.actor, children, self.next_child_number)


class ActorSystem:

    __DELIMITER = "/"
    __ROOT_ID = __DELIMITER

    def __init__(self, main_actor: Actor, system_thread: str = "actor_system"):
        super().__init__()

        self._log = util.logger_for_object(self)

        self.__actors: tp.Dict[ActorId, ActorNode] = {self.__ROOT_ID: ActorNode("", self.__ROOT_ID, None)}
        self.__message_queue: tp.List[Msg] = list()

        self.__system_thread = threading.Thread(
            name=system_thread,
            target=self._actor_system_main)

        self.__system_lock = threading.Lock()
        self.__system_up = threading.Event()
        self.__system_msg = threading.Event()
        self.__system_error: tp.Optional[Exception] = None

        self.__main_id = self._register_actor(self.__ROOT_ID, main_actor, do_start=False)

    # Public API

    def start(self, wait=False):

        self.__system_thread.start()
        self._start_actor("/system", self.__main_id)

        if wait:
            self.__system_up.wait()  # TODO: Startup timeout

    def stop(self):

        self._stop_actor("/system", self.__main_id)

    def wait_for_shutdown(self):

        self.__system_thread.join()   # TODO: Timeout

    def shutdown_code(self) -> int:

        return 0 if self.__system_error is None else -1

    def shutdown_error(self) -> tp.Optional[Exception]:

        return self.__system_error

    def send(self, message: str, *args, **kwargs):

        self._send_message("/external", self.__main_id, message, args, kwargs)

    def _spawn_actor(self, parent_id: ActorId, actor_class: Actor.__class__, args, kwargs):

        actor = actor_class(*args, **kwargs)

        return self._register_actor(parent_id, actor, do_start=True)

    def _register_actor(self, parent_id: ActorId, actor: Actor, do_start: bool = True):

        actor_class = actor.__class__

        with self.__system_lock:

            # TODO: Parent already discarded, can that happen?
            parent_node = self.__actors.get(parent_id)
            actor_id = self._new_actor_id(parent_node, actor_class)

            parent_node = parent_node.with_child(actor_id)
            actor_node = ActorNode(parent_id, actor_id, actor)

            self.__actors[parent_id] = parent_node
            self.__actors[actor_id] = actor_node

        if do_start:
            self._start_actor(parent_id, actor_id)

        return actor_id

    def _start_actor(self, started_by_id: ActorId, actor_id: ActorId):

        self._send_signal(started_by_id, actor_id, SignalNames.START)

        return actor_id

    def _stop_actor(self, sender_id: ActorId, target_id: ActorId):

        if sender_id not in [target_id, self._parent_id(target_id), "/system"]:

            # Client code could submit an invalid stop request, this counts as a bad actor

            message = f"Stop request rejected: [{SignalNames.STOP}] -> {target_id}" + \
                      f" ({sender_id} is not allowed to stop this actor)"

            self._log.error(message)
            raise EBadActor(message)

        target = self._lookup_actor_node(target_id)

        if target is None:
            self._log.warning(
                f"Signal ignored: [{SignalNames.STOP}] -> {target_id}" +
                f" (target actor not found)")
            return

        for child_id in target.children:
            self._stop_actor(target_id, child_id)

        self._send_signal(sender_id, target_id, SignalNames.STOP)

    def _send_signal(self, sender_id: ActorId, target_id: ActorId, signal: str, error: tp.Optional[Exception] = None):

        # Only the actor system can send signals, so a bad signal is an unexpected error

        if not signal.startswith(SignalNames.PREFIX):
            raise _ex.EUnexpected()

        if signal == SignalNames.FAILED:
            msg = ErrorSignal(sender_id, target_id, signal, error=error)
        else:
            msg = Signal(sender_id, target_id, signal)

        self.__message_queue.append(msg)

    def _send_message(self, sender_id: ActorId, target_id: ActorId, message: str, args, kwargs):

        # Client code could try to send a signal string as a message, this counts as a bad actor

        if message.startswith(SignalNames.PREFIX):

            message = f"Invalid message: {sender_id} [{message}] -> {target_id}" \
                    + f" ([{message} looks like a signal, signals cannot be sent with send_message)"

            self._log.error(message)
            raise EBadActor(message)

        _args = args or []
        _kwargs = kwargs or {}

        target = self._lookup_actor_node(target_id)

        if target is not None:
            target_class = target.actor.__class__
            self._check_message_signature(target_id, target_class, message, args, kwargs)

        msg = Msg(sender_id, target_id, message, _args, _kwargs)
        self.__message_queue.append(msg)

    def _actor_system_main(self):

        self._message_loop()

    def _message_loop(self):

        self.__system_up.set()

        main_actor = self._lookup_actor(self.__main_id)

        while main_actor.state() not in [ActorState.STOPPED, ActorState.FAILED]:

            if len(self.__message_queue):
                next_msg = self.__message_queue.pop(0)
            else:
                next_msg = None

            if next_msg:
                if next_msg.message.startswith(SignalNames.PREFIX):
                    self._process_signal(next_msg)
                else:
                    self._process_message(next_msg)
            else:
                self.__system_msg.wait(0.01)

        self.__system_error = main_actor.error()

    def _process_message(self, msg: Msg):

        target = self._lookup_actor_node(msg.target)

        if target is None:
            # Unhandled messages are dropped, with just a warning in the log
            self._log.warning(f"Message ignored: [{msg.message}] -> {msg.target}  (target actor not found)")
            return

        if target.actor.state() != ActorState.RUNNING:
            self._log.warning(f"Message ignored: [{msg.message}] -> {msg.target}  (target actor not running)")
            return

        parent_id = target.parent_id
        ctx = ActorContext(self, msg.message, msg.target, parent_id, msg.sender)

        target.actor._receive_message(self, ctx, msg)  # noqa

    def _process_signal(self, signal: Msg):

        target = self._lookup_actor_node(signal.target)

        if target is None:

            # stopped / failed messages can arrive from a child actor after the parent has stopped
            # This is a normal condition and should be silently ignored (not be logged as a warning)
            if self._parent_id(signal.sender) == signal.target and \
                    signal.message in [SignalNames.STOPPED, SignalNames.FAILED]:
                return

            # Unhandled messages are dropped, with just a warning in the log
            self._log.warning(f"Signal ignored: [{signal.message}] -> {signal.target}  (target actor not found)")
            return

        parent_id = target.parent_id
        ctx = ActorContext(self, signal.message, signal.target, parent_id, signal.sender)
        result = target.actor._receive_signal(self, ctx, signal)  # noqa

        # Generate lifecycle notifications

        if signal.message == SignalNames.STOP:

            if target.actor.error():
                self._send_signal(signal.target, target.parent_id, SignalNames.FAILED, target.actor.error())
            else:
                self._send_signal(signal.target, target.parent_id, SignalNames.STOPPED)

        # Error propagation
        # When an actor dies due to an error, a FAILED signal is generated and sent to its direct parent
        # If the parent does not handle the error successfully, the parent also dies and the error propagates

        if isinstance(signal, ErrorSignal):
            if signal.target == self._parent_id(signal.sender) and result is not True:
                target.actor._Actor__error = signal.error  # TODO: Error could be wrapped to indicate propagation?
                self._stop_actor("/system", signal.target)
                self._send_signal(signal.sender, target.parent_id, SignalNames.FAILED, signal.error)

        # Remove dead actors
        # If the actor is now stopped or failed, take it out of the registry

        if target.actor.state() in [ActorState.STOPPED, ActorState.FAILED]:
            with self.__system_lock:
                self.__actors.pop(signal.target)
                parent = self.__actors.get(target.parent_id)
                if parent is not None:
                    parent = parent.without_child(signal.target)
                    self.__actors[target.parent_id] = parent

    def _report_error(self, actor_id: ActorId, message: str, error: Exception):

        actor_node = self._lookup_actor_node(actor_id)

        if not actor_node:
            message = f"Error ignored: [{SignalNames.STOP}] -> {actor_id} (failed actor not found)"
            self._log.warning(message)
            return

        self._log.error(f"{actor_id} [{message}]: {str(error)}")
        self._log.exception(error)
        self._log.error(f"Actor failed: {actor_id} [{message}] (actor will be stopped)")

        # Dp not send STOP signal for errors that occur while processing START or STOP
        # In this case, directly set the state to FAILED and send a FAILED notification to the parent
        if message in [SignalNames.START, SignalNames.STOP]:
            actor_node.actor._Actor__state = ActorState.FAILED
            self._send_signal(actor_id, actor_node.parent_id, SignalNames.FAILED, error)  # TODO: Wrap for propagation?

        # Otherwise stop the actor, a FAILED notification will be generated when the STOP signal is processed
        else:
            self._stop_actor("/system", actor_id)

    def _lookup_actor_node(self, actor_id: ActorId) -> tp.Optional[ActorNode]:

        with self.__system_lock:
            return self.__actors.get(actor_id)

    def _lookup_actor(self, actor_id: ActorId) -> tp.Optional[Actor]:

        with self.__system_lock:
            actor_node = self.__actors.get(actor_id)
            return actor_node.actor if actor_node is not None else None

    def _new_actor_id(self, parent_node: ActorNode, actor_class: Actor.__class__) -> ActorId:

        classname = actor_class.__name__.lower()

        if parent_node.actor_id == self.__ROOT_ID:
            return f"{self.__ROOT_ID}{classname}"
        else:
            return f"{parent_node.actor_id}{self.__DELIMITER}{classname}-{parent_node.next_child_number}"

    def _parent_id(self, actor_id: ActorId) -> ActorId:

        parent_delim = actor_id.rfind(self.__DELIMITER)
        parent_id = self.__ROOT_ID if parent_delim == 0 else actor_id[:parent_delim]

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
