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

import trac.rt.impl.util as util


ActorId = str


class ActorState(enum.Enum):

    NOT_STARTED = 0
    STARTING = 1
    RUNNING = 2
    STOPPING = 3
    STOPPED = 4
    ERROR = 5
    FAILED = 6


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

    def on_signal(self, signal: str) -> tp.Optional[bool]:
        return None

    def _inspect_handlers(self) -> tp.Dict[str, tp.Callable]:

        known_handlers = Actor.__class_handlers.get(self.__class__)

        if known_handlers:
            return known_handlers

        handlers = dict()

        for member in self.__class__.__dict__.values():
            if isinstance(member, Message):
                handlers[member.__name__] = func.partial(member, self)

        Actor.__class_handlers[self.__class__] = handlers
        return handlers

    def _receive_message(self, system: ActorSystem, ctx: ActorContext, msg: Msg):

        try:
            self.__ctx = ctx

            handler = self.__handlers.get(msg.message)

            if handler:
                handler(*msg.args, **msg.kwargs)

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

    def _receive_signal(self, system: ActorSystem, ctx: ActorContext, signal: Msg) -> tp.Optional[bool]:

        try:
            self.__ctx = ctx

            if signal.message == Signal.START:
                self._require_state([ActorState.NOT_STARTED, ActorState.STARTING])
                self.on_start()
                self.__state = ActorState.RUNNING
                return True

            elif signal.message == Signal.STOP:
                self._require_state([ActorState.RUNNING, ActorState.STOPPING, ActorState.ERROR])
                self.on_stop()
                self.__state = ActorState.STOPPED if self.__error is None else ActorState.FAILED
                return True

            else:
                return self.on_signal(signal.message)

        except Exception as error:

            self.__state = ActorState.ERROR
            self.__error = error
            system._report_error(ctx.id, signal.message, error)  # noqa
            return None

        finally:
            self.__ctx = None

    def _require_state(self, allowed_states: tp.List[ActorState]):

        if self.__state not in allowed_states:
            raise RuntimeError("Actor lifecycle error")  # TODO: Error


# Static member __log can only be set after class Actor is declared
Actor._Actor__log = util.logger_for_class(Actor)


class ActorContext:

    def __init__(self, system: ActorSystem, current_actor: ActorId, parent: ActorId, sender: tp.Optional[ActorId]):

        self.id = current_actor
        self.parent = parent
        self.sender = sender

        self.__system = system
        self.__id = current_actor
        self.__parent = parent
        self.__sender = sender

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


class Message:

    def __init__(self, method):
        self.__method = method
        func.update_wrapper(self, method)

        params = inspect.signature(method).parameters
        self.params = list(params.values())[1:]  # skip 'self' parameter
        self.type_hints = tp.get_type_hints(method)

    def __call__(self, *args, **kwargs):
        self.__method(*args, **kwargs)


class Signal:

    PREFIX = "actor:"

    START = "actor:start"
    STOP = "actor:stop"

    STARTED = "actor:started"
    STOPPED = "actor:stopped"
    FAILED = "actor:failed"


@dc.dataclass(frozen=True)
class Msg:

    sender: ActorId
    target: ActorId
    message: str

    args: tp.List[tp.Any] = dc.field(default_factory=list)
    kwargs: tp.Dict[str, tp.Any] = dc.field(default_factory=dict)


class ActorSystem:

    def __init__(self, main_actor: Actor, system_thread: str = "actor_system"):

        self._log = util.logger_for_object(self)

        self.__actors: tp.Dict[ActorId, Actor] = dict()
        self.__message_queue: tp.List[Msg] = list()

        self.__main_id = self._spawn_main_actor("/", main_actor)

        self.__system_thread = threading.Thread(
            name=system_thread,
            target=self._actor_system_main)

        self.__system_lock = threading.Lock()
        self.__system_up = threading.Event()
        self.__system_msg = threading.Event()

        self.__system_error: tp.Optional[Exception] = None

    # Public API

    def start(self, wait=False):

        self.__system_thread.start()
        self._start_actor("/system", self.__main_id)

        if wait:
            self.__system_up.wait()  # TODO: Startup timeout

    def stop(self):

        self._stop_actor("/system", self.__main_id)

    def wait_for_shutdown(self):

        self.__system_thread.join()

    def shutdown_code(self) -> int:

        return 0 if self.__system_error is None else -1

    def shutdown_error(self) -> tp.Optional[Exception]:

        return self.__system_error

    def send(self, message: str, *args, **kwargs):

        self._send_message("/external", self.__main_id, message, args, kwargs)

    def _spawn_actor(self, parent_id: ActorId, actor_class: Actor.__class__, args, kwargs):

        actor_id = self._new_actor_id(parent_id, actor_class)
        actor = actor_class(*args, **kwargs)

        self.__actors[actor_id] = actor
        self._start_actor(parent_id, actor_id)

        return actor_id

    def _spawn_main_actor(self, parent_id: ActorId, actor: Actor):

        actor_id = self._new_actor_id(parent_id, actor.__class__)
        self.__actors[actor_id] = actor

        return actor_id

    def _start_actor(self, started_by_id: ActorId, actor_id: ActorId):

        self._send_signal(started_by_id, actor_id, Signal.START)

        return actor_id

    def _stop_actor(self, sender_id: ActorId, target_id: ActorId):

        if not (sender_id == target_id or self._parent_id(target_id) == sender_id or sender_id == "/system"):
            self._log.warning(
                f"Signal ignored: [{Signal.STOP}] -> {target_id}" +
                f" ({sender_id} is not allowed to stop this actor)")
            return

        target_state = self.__actors.get(target_id)

        if not target_state:
            self._log.warning(
                f"Signal ignored: [{Signal.STOP}] -> {target_id}" +
                f" (target actor not found)")
            return

        # TODO: This could get expensive! Keep an explicit record of children
        for aid, actor in self.__actors.items():
            if self._parent_id(aid) == target_id:
                self._stop_actor(target_id, aid)

        self._send_signal(sender_id, target_id, Signal.STOP)

    def _send_signal(self, sender_id: ActorId, target_id: ActorId, signal: str):

        if not signal.startswith(Signal.PREFIX):
            raise RuntimeError("Invalid signal")  # TODO: Error

        msg = Msg(sender_id, target_id, signal)
        self.__message_queue.append(msg)

    def _send_message(self, sender_id: ActorId, target_id: ActorId, message: str, args, kwargs):

        if message.startswith(Signal.PREFIX):
            raise RuntimeError("Signals cannot be sent like messages")  # TODO: Error

        _args = args or []
        _kwargs = kwargs or {}

        actor = self.__actors.get(target_id)

        if actor is not None:
            target_class = actor.__class__
            self._check_message_signature(target_id, target_class, message, args, kwargs)

        msg = Msg(sender_id, target_id, message, _args, _kwargs)
        self.__message_queue.append(msg)

    def _actor_system_main(self):

        self._message_loop()

    def _message_loop(self):

        self.__system_up.set()

        main_actor = self.__actors.get(self.__main_id)

        while main_actor.state() not in [ActorState.STOPPED, ActorState.FAILED]:

            if len(self.__message_queue):
                next_msg = self.__message_queue.pop(0)
            else:
                next_msg = None

            if next_msg:
                if next_msg.message.startswith(Signal.PREFIX):
                    self._process_signal(next_msg)
                else:
                    self._process_message(next_msg)
            else:
                self.__system_msg.wait(0.01)

        self.__system_error = main_actor.error()

    def _process_message(self, msg: Msg):

        actor = self._lookup_actor(msg.target)

        if not actor:
            # Unhandled messages are dropped, with just a warning in the log
            self._log.warning(f"Message ignored: [{msg.message}] -> {msg.target}  (target actor not found)")
            return

        if actor.state() != ActorState.RUNNING:
            self._log.warning(f"Message ignored: [{msg.message}] -> {msg.target}  (target actor not running)")
            return

        parent_id = self._parent_id(msg.target)
        ctx = ActorContext(self, msg.target, parent_id, msg.sender)

        actor._receive_message(self, ctx, msg)  # noqa

    def _process_signal(self, signal: Msg):

        actor = self._lookup_actor(signal.target)

        if not actor:
            # Unhandled messages are dropped, with just a warning in the log
            self._log.warning(f"Signal ignored: [{signal.message}] -> {signal.target}  (target actor not found)")
            return

        parent_id = self._parent_id(signal.target)
        ctx = ActorContext(self, signal.target, parent_id, signal.sender)
        result = actor._receive_signal(self, ctx, signal)  # noqa

        # Notifications

        # TODO: If the error was reposted with _report_error, the parent will already have a FAILED signal
        if signal.message == Signal.STOP:
            if actor.error():
                self._send_signal(signal.target, self._parent_id(signal.target), Signal.FAILED)
            else:
                self._send_signal(signal.target, self._parent_id(signal.target), Signal.STOPPED)

        # Error propagation
        # When an actor dies due to an error, a FAILED signal is sent to its direct parent
        # If the parent does not handle the error successfully, the parent also dies and the error propagates

        if signal.message == Signal.FAILED:
            if signal.target == self._parent_id(signal.sender) and result is not True:
                actor._Actor__error = RuntimeError("propagation error")  # TODO: Needs to wrap the original error
                self._stop_actor("/system", signal.target)
                self._send_signal(signal.target, self._parent_id(signal.target), Signal.FAILED)

        if actor.state() in [ActorState.STOPPED, ActorState.FAILED]:
            self.__actors.pop(signal.target)

    def _report_error(self, actor_id: ActorId, message: str, error: Exception):

        actor = self.__actors.get(actor_id)

        if not actor:
            message = f"Error ignored: [{Signal.STOP}] -> {actor_id} (failed actor not found)"
            self._log.warning(message)
            return

        self._log.error(f"{actor_id} [{message}]: {str(error)}")
        self._log.error(f"Actor failed: {actor_id} [{message}] (actor will be stopped)")

        # Dp not send STOP signal if actor was not started successfully
        if message in [Signal.START, Signal.STOP]:
            actor._Actor__state = ActorState.FAILED
        else:
            self._stop_actor("/system", actor_id)

        # Notify the parent
        self._send_signal(actor_id, self._parent_id(actor_id), Signal.FAILED)

    def _lookup_actor(self, actor_id: ActorId) -> tp.Optional[Actor]:

        return self.__actors.get(actor_id)

    def _new_actor_id(self, parent_id: ActorId, actor_class: Actor.__class__) -> ActorId:

        if parent_id == "/":
            return "/" + actor_class.__name__.lower()

        return parent_id + "/" + actor_class.__name__.lower()

    def _parent_id(self, actor_id: ActorId) -> ActorId:

        parent_delim = actor_id.rfind("/")
        parent_id = actor_id[0] if parent_delim == 0 else actor_id[:actor_id.rfind("/")]

        return parent_id

    def _check_message_signature(self, target_id: ActorId, target_class: Actor.__class__, message: str, args, kwargs):

        target_handler = Actor._Actor__class_handlers.get(target_class).get(message)  # noqa

        if target_handler is None:
            error = f"Invalid message: [{message}] -> {target_id} (unknown message '{message}')"
            self._log.error(error)
            raise RuntimeError(error)

        target_params = target_handler.func.params
        type_hints = target_handler.func.type_hints

        if len(args) + len(kwargs) > len(target_params):
            error = f"Invalid message: [{message}] -> {target_id} (too many arguments)"
            self._log.error(error)
            raise RuntimeError(error)

        pos_params = target_params[:len(args)]
        kw_params = target_params[len(args):]
        kw_param_names = set(map(lambda p: p.name, kw_params))

        # Missing params
        for param in kw_params:
            if param.default is inspect._empty and param.name not in kwargs:  # noqa
                error = f"Invalid message: [{message}] -> {target_id} (missing required parameter '{param.name}')"
                self._log.error(error)
                raise RuntimeError(error)

        # Extra (unknown) kw params
        for param_name in kwargs.keys():
            if param_name not in kw_param_names:
                error = f"Invalid message: [{message}] -> {target_id} (unknown parameter '{param_name}')"
                self._log.error(error)
                raise RuntimeError(error)

        # Positional arg types
        for pos_param, pos_arg in zip(pos_params, args):

            type_hint = type_hints.get(pos_param.name)

            # If no type hint is available, allow anything through
            if type_hint is not None and not isinstance(pos_arg, type_hint):
                error = f"Invalid message: [{message}] -> {target_id} (wrong parameter type for '{pos_param.name}')"
                self._log.error(error)
                raise RuntimeError(error)

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
                raise RuntimeError(error)
