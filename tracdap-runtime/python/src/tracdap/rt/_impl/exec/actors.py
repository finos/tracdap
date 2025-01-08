#  Licensed to the Fintech Open Source Foundation (FINOS) under one or
#  more contributor license agreements. See the NOTICE file distributed
#  with this work for additional information regarding copyright ownership.
#  FINOS licenses this file to you under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with the
#  License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import threading
import functools as func
import typing as tp
import enum
import dataclasses as dc
import inspect
import queue
import time

import tracdap.rt._impl.core.logging as _logging
import tracdap.rt._impl.core.validation as _val
import tracdap.rt.exceptions as _ex


ActorId = str


class EBadActor(_ex.ETracInternal):

    """
    A bad request has been made to the actor subsystem, e.g. an invalid message or message params

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

    def __init__(self):
        self.__ctx: tp.Optional[ActorContext] = None

    def state(self) -> ActorState:
        return self.__ctx.get_state()

    def error(self) -> tp.Optional[Exception]:
        return self.__ctx.get_error()

    def actors(self) -> "ActorContext":
        return self.__ctx

    def on_start(self):
        pass

    def on_stop(self):
        pass

    def on_signal(self, signal: Signal) -> tp.Optional[bool]:
        return None


class ActorContext:

    def __init__(
            self, node: "ActorNode", message: str,
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

    def spawn(self, actor: Actor) -> ActorId:
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

    def get_state(self):
        return self.__node.state

    def get_error(self) -> tp.Optional[Exception]:
        return self.__error or self.__node.error


class ThreadsafeActor(Actor):

    def __init__(self):
        super().__init__()
        self.__threadsafe: tp.Optional[ThreadsafeContext] = None

    def threadsafe(self) -> "ThreadsafeContext":
        return self.__threadsafe


class ThreadsafeContext:

    def __init__(self, node: "ActorNode"):
        self.__node = node
        self.__id = node.actor_id
        self.__parent = node.parent.actor_id if node.parent is not None else None

    def spawn(self, actor: Actor):
        self.__node.event_loop.post_message(
            None, lambda _:
            self.__node.spawn(actor) and None)

    def send(self, target_id: ActorId, message: str, *args, **kwargs):
        self.__node.event_loop.post_message(
            None, lambda _:
            self.__node.send_message(self.__id, target_id, message, args, kwargs))

    def send_parent(self, message: str, *args, **kwargs):
        self.__node.event_loop.post_message(
            None, lambda _:
            self.__node.send_message(self.__id, self.__parent, message, args, kwargs))

    def stop(self):
        self.__node.event_loop.post_message(
            None, lambda _:
            self.__node.send_signal(self.__id, self.__id, SignalNames.STOP))

    def fail(self, error: Exception):
        self.__node.event_loop.post_message(
            None, lambda _:
            self.__node.send_signal(self.__id, self.__id, SignalNames.STOP, error))


class EventLoop:

    _T_MSG = tp.TypeVar("_T_MSG")

    def __init__(self):
        self.__msg_lock = threading.Condition()
        self.__msg_queue: queue.Queue[tp.Tuple[Msg, tp.Callable[[Msg], None]]] = queue.Queue()
        self.__shutdown = False
        self.__shutdown_now = False
        self.__done = False
        self.__log = _logging.logger_for_object(self)

    def post_message(self, msg: _T_MSG, processor: tp.Callable[[_T_MSG], None]) -> bool:
        with self.__msg_lock:
            if self.__done:
                return False
            self.__msg_queue.put((msg, processor))
            self.__msg_lock.notify()
            return True

    def shutdown(self, immediate: bool = False):
        with self.__msg_lock:
            self.__shutdown = True
            self.__shutdown_now = immediate
            self.__msg_lock.notify()

    def main(self):
        self._event_loop()

    def _event_loop(self):

        while not self.__done:

            with self.__msg_lock:

                self.__msg_lock.wait_for(lambda: not self.__msg_queue.empty() or self.__shutdown)
                event = self.__msg_queue.get() if not self.__msg_queue.empty() else None

                if self.__shutdown_now:
                    self.__done = True
                    break

                if self.__shutdown and event is None:
                    self.__done = True

            if event is not None:
                try:
                    msg, processor = event
                    processor(msg)
                except Exception as e:
                    self.__log.error(f"Unhandled error on the event loop: {str(e)}")
                    self.__log.exception(e)


class EventLoopPool:

    def __init__(self, name: str, size: int, daemon: bool = False):

        self.__loops: tp.List[EventLoop] = []
        self.__threads: tp.List[threading.Thread] = []
        self.__lock = threading.Lock()
        self.__size = size
        self.__next = 0

        for i in range(size):
            loop = EventLoop()
            thread = threading.Thread(name=f"{name}-{i}", target=loop.main, daemon=daemon)
            self.__loops.append(loop)
            self.__threads.append(thread)

    def start(self):
        for i in range(self.__size):
            self.__threads[i].start()

    def shutdown(self, immediate: bool = False):
        for loop in self.__loops:
            loop.shutdown(immediate)

    def get_loop(self) -> EventLoop:
        # simple round-robin for now (we could look at which loops are idle)
        with self.__lock:
            loop_index = self.__next
            self.__next = (self.__next + 1) % self.__size
        return self.__loops[loop_index]

    def join(self, timeout=None):

        start = time.monotonic()
        remaining = timeout

        for i in range(self.__size):

            loop = self.__loops[i]
            thread = self.__threads[i]

            if remaining is None or remaining > 0:
                thread.join(remaining)
            if thread.is_alive():
                loop.shutdown(True)

            if timeout is not None:
                elapsed = time.monotonic() - start
                remaining = timeout - elapsed


class FunctionCache:

    __cache: tp.Dict[type, tp.Dict[str, tp.Callable]] = dict()
    __lock = threading.Lock()

    @classmethod
    def inspect_message_functions(cls, actor_class: Actor.__class__):

        handlers = dict()

        for member_name, member in inspect.getmembers(actor_class):
            if isinstance(member, Message):
                handlers[member_name] = member

        with cls.__lock:
            if actor_class in cls.__cache:
                return cls.__cache[actor_class]
            else:
                cls.__cache[actor_class] = handlers
                return handlers

    @classmethod
    def lookup_message_function(cls, actor_class: Actor.__class__, message: str):

        with cls.__lock:
            handlers = cls.__cache.get(actor_class)

        if handlers is None:
            handlers = cls.inspect_message_functions(actor_class)

        return handlers.get(message)


class ActorNode:

    _log = _logging.logger_for_class(Actor)

    def __init__(
            self, actor_id: ActorId, actor: Actor,
            parent: "tp.Optional[ActorNode]",
            system: "ActorSystem",
            event_loop: EventLoop):

        self.actor_id = actor_id
        self.actor = actor
        self.parent = parent
        self.system = system
        self.event_loop = event_loop

        self.children: tp.Dict[ActorId, ActorNode] = {}
        self.next_child_number: int = 0

        self.state: ActorState = ActorState.NOT_STARTED
        self.error: tp.Optional[Exception] = None

    def spawn(self, child_actor: Actor) -> ActorId:

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug(f"spawn [{self.actor_id}]: [{type(child_actor)}]")

        actor_class = type(child_actor)
        child_id = self._new_child_id(actor_class)

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug(f"spawn [{self.actor_id}]: [{type(child_actor)}] {child_id}")

        event_loop = self.system._allocate_event_loop(actor_class)  # noqa
        child_node = ActorNode(child_id, child_actor, self, self.system, event_loop)
        self.children[child_id] = child_node

        # If this is a threadsafe actor, set up the threadsafe context
        if isinstance(child_actor, ThreadsafeActor):
            threadsafe = ThreadsafeContext(child_node)
            child_actor._ThreadsafeActor__threadsafe = threadsafe

        child_node.send_signal(self.actor_id, child_id, SignalNames.START)

        return child_id

    def send_message(self, sender_id: ActorId, target_id: ActorId, message: str, args, kwargs):

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug(f"send_signal [{self.actor_id}]: [{message}] {sender_id} -> {target_id}")

        # Client code could try to send a signal string as a message, this counts as a bad actor

        if message.startswith(SignalNames.PREFIX):
            message = f"Invalid message: {sender_id} [{message}] -> {target_id}" \
                      + f" ([{message} looks like a signal, signals cannot be sent with send_message)"
            self._log.error(message)
            raise EBadActor(message)

        # Look up the target node, signals to unknown targets are silently dropped

        target_node = self._lookup_node(target_id)

        if target_node is None:
            warning = f"Message dropped (target actor not found): [{message}] {sender_id} -> {target_id}"
            self._log.warning(warning)
            return

        # Check the target message signature - errors will be raised in the sender

        target_class = target_node.actor.__class__
        target_handler = FunctionCache.lookup_message_function(target_class, message)
        self._check_message_signature(target_id, target_handler, message, args, kwargs)

        # Send the message

        msg = Msg(sender_id, target_id, message, args or [], kwargs or {})

        target_node._accept(msg)

    def send_signal(self, sender_id: ActorId, target_id: ActorId, signal: str, error: tp.Optional[Exception] = None):

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug(f"send_signal [{self.actor_id}]: [{signal}] {sender_id} -> {target_id}")

        # Only the actor system can send signals, so a bad signal is an unexpected error

        if not signal.startswith(SignalNames.PREFIX):
            raise _ex.EUnexpected()

        # Client code could submit an invalid control request, this counts as a bad actor

        controllers = ["/system", target_id, self._parent_id(target_id)]

        if signal in SignalNames.CONTROL_SIGNALS and sender_id not in controllers:

            message = f"Stop signal rejected: [{SignalNames.STOP}] -> {target_id}" + \
                      f" ({sender_id} is not allowed to stop this actor)"

            self._log.error(message)
            raise EBadActor(message)

        # Look up the target node, signals to unknown targets are silently dropped

        target_node = self._lookup_node(target_id)

        if target_node is None:
            warning = f"Signal dropped (target actor not found): [{signal}] {sender_id} -> {target_id}"
            self._log.warning(warning)
            return

        # Send the signal

        if signal == SignalNames.FAILED:
            msg = ErrorSignal(sender_id, target_id, signal, error=error)
        else:
            msg = Signal(sender_id, target_id, signal)

        target_node._accept(msg)

    def _lookup_node(self, target_id: ActorId) -> "tp.Optional[ActorNode]":

        # Check self first

        if target_id == self.actor_id:
            return self

        # Check direct parent and children

        elif self.parent is not None and target_id == self.parent.actor_id:
            return self.parent

        elif target_id in self.children:
            # Child may be removed, use .get() not []
            return self.children.get(target_id)

        # Otherwise we need to start searching the tree

        if self.actor_id == ActorSystem.ROOT_ID:
            child_namespace = ActorSystem.ROOT_ID
        else:
            child_namespace = self.actor_id + ActorSystem.DELIMITER

        # If the target is in a child namespace for this node, try to go down a level

        if target_id.startswith(child_namespace):

            child_id_sep = target_id.find(ActorSystem.DELIMITER, len(child_namespace))
            if child_id_sep < 0:
                return None
            child_id = target_id[:child_id_sep]
            child_node = self.children.get(child_id)
            if child_node is not None:
                return child_node._lookup_node(target_id)
            else:
                return None

        # Otherwise try to go up a level

        else:
            if self.parent is not None:
                return self.parent._lookup_node(target_id)
            else:
                return None

    def _accept(self, msg: Msg):

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug(f"_accept [{self.actor_id}]: [{msg.message}] {msg.sender} -> {msg.target}")

        if msg.target != self.actor_id:
            err = f"Message delivery failed [{self.actor_id}]: [{msg.message}] {msg.sender} -> {msg.target}" + \
                  f" (delivered to the wrong address)"
            self._log.error(err)
            raise EBadActor(err)

        if isinstance(msg, Signal):
            posted = self.event_loop.post_message(msg, self._process_signal)
        else:
            posted = self.event_loop.post_message(msg, self._process_message)

        if not posted:
            warning = f"Message dropped [{self.actor_id}]: [{msg.message}] {msg.sender} -> {msg.target}" + \
                      f" (actor thread has already stopped)"
            self._log.warning(warning)

    def _process_message(self, msg: Msg):

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug(f"_process_message [{self.actor_id}]: [{msg.message}] {msg.sender} -> {msg.target}")

        # Only accept legal signals where the target is this actor

        if not self._check_message_target(msg):
            return

        # Only process messages while the actor is running (this also implies no errors)
        # This is different from signals, which can be processed at other stages of the actor lifecycle

        if self.state != ActorState.RUNNING:
            warning = f"Message ignored: [{msg.message}] {msg.sender} -> {self.actor_id}" + \
                      f" (actor is in {self.state.name} state)"
            self._log.warning(warning)
            return

        # CAll the message receiver function to handle the message

        parent_id = self.parent.actor_id if self.parent else None

        ctx = ActorContext(self, msg.message, self.actor_id, parent_id, msg.sender)
        self._receive_message(ctx, msg)

        # Send error notifications if the actor has gone into an error state on this message

        if self.state == ActorState.ERROR:
            self.send_signal(self.actor_id, self.actor_id, SignalNames.STOP)
            if self.parent is not None:
                self.send_signal(self.actor_id, self.parent.actor_id, SignalNames.FAILED, self.error)

    def _process_signal(self, signal: Signal):

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug(f"_process_signal [{self.actor_id}]: [{signal.message}] {signal.sender} -> {signal.target}")

        # Only accept legal signals where the target is this actor

        if not self._check_message_target(signal):
            return

        # Do not process signals after the actor has stopped
        # This is common with e.g. STOP signals that propagate up and down the tree

        if self.state in [ActorState.STOPPED, ActorState.FAILED]:
            return

        # Call the signal receiver function
        # This gives the actor a chance to respond to the signal

        parent_id = self.parent.actor_id if self.parent else None
        prior_state = self.state

        ctx = ActorContext(self, signal.message, self.actor_id, parent_id, signal.sender)
        result = self._receive_signal(ctx, signal)

        # Handle actors that have gone into error state
        # If the error occurs during start / stop, the actor is considered down (so don't send stop signal)
        # If the actor was already in error state, the error signal was already sent (so don't send it again)

        if self.state == ActorState.ERROR:
            if signal.message in [SignalNames.START, SignalNames.STOP]:
                self.state = ActorState.FAILED
            elif prior_state != ActorState.ERROR:
                self.send_signal(self.actor_id, self.actor_id, SignalNames.STOP)

        # Generate lifecycle notifications

        if signal.message == SignalNames.START and self.parent is not None:
            if self.state == ActorState.RUNNING:
                self.send_signal(self.actor_id, self.parent.actor_id, SignalNames.STARTED)

        if signal.message == SignalNames.STOP and self.parent is not None:
            if self.state == ActorState.STOPPED:
                self.send_signal(self.actor_id, self.parent.actor_id, SignalNames.STOPPED)

        if self.state in [ActorState.ERROR, ActorState.FAILED] and self.parent is not None:
            if prior_state != ActorState.ERROR:
                self.send_signal(self.actor_id, self.parent.actor_id, SignalNames.FAILED, self.error)

        # Propagate stop signals

        if signal.message == SignalNames.STOP:
            for child_id, child_node in self.children.items():
                if child_node.state in [ActorState.RUNNING, ActorState.STARTING]:  # noqa
                    child_node.send_signal(self.actor_id, child_id, SignalNames.STOP)

        # Propagate errors
        # When an actor dies due to an error, a FAILED signal is generated and sent to its direct parent
        # If the parent does not handle the error successfully, the parent also dies and the error propagates
        # Errors are propagated as-is, i.e. with no wrapping

        if isinstance(signal, ErrorSignal) and signal.sender in self.children and result is not True:
            self.state = ActorState.ERROR
            self.error = signal.error
            self.send_signal(self.actor_id, self.actor_id, SignalNames.STOP)
            if self.parent is not None:
                self.send_signal(self.actor_id, self.parent.actor_id, SignalNames.FAILED, signal.error)

        # Remove dead actors
        # If the actor is now stopped or failed, take it out of the registry

        if signal.message in [SignalNames.STOPPED, SignalNames.FAILED] and signal.sender in self.children:
            self.children.pop(signal.sender)

    def _receive_message(self, ctx: ActorContext, msg: Msg):

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug(f"_receive_message [{self.actor_id}]: [{msg.message}] {msg.sender} -> {msg.target}")

        try:
            self.actor._Actor__ctx = ctx

            handler = FunctionCache.lookup_message_function(self.actor.__class__, msg.message)

            if handler is None:
                # Unhandled messages are dropped, with just a warning in the log
                warning = f"Message ignored: [{msg.message}] {msg.sender} -> {msg.target}" + \
                          f" (actor {self.actor.__class__.__name__} does not support this message)"
                logging.warning(warning)
                return

            handler(self.actor, *msg.args, **msg.kwargs)

            if ctx.get_error():
                self.state = ActorState.ERROR
                self.error = ctx.get_error()

        except Exception as error:
            self.state = ActorState.ERROR
            self.error = error

        finally:
            self.actor._Actor__ctx = None

    def _receive_signal(self, ctx: ActorContext, signal: Signal) -> tp.Optional[bool]:

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug(f"_receive_signal [{self.actor_id}]: [{signal.message}] {signal.sender} -> {signal.target}")

        try:
            self.actor._Actor__ctx = ctx

            if signal.message == SignalNames.START:
                if not self._check_current_state([ActorState.NOT_STARTED, ActorState.STARTING], signal):
                    return False
                self.actor.on_start()
                if self.error is None and ctx.get_error() is None:
                    self.state = ActorState.RUNNING
                else:
                    self.state = ActorState.ERROR
                    self.error = self.error or ctx.get_error()
                return True

            elif signal.message == SignalNames.STOP:
                # Ignore duplicate stop signals
                if self.state in [ActorState.STOPPED, ActorState.FAILED]:
                    return True
                if not self._check_current_state([ActorState.RUNNING, ActorState.STOPPING, ActorState.ERROR], signal):
                    return False
                self.actor.on_stop()
                if self.error is None and ctx.get_error() is None:
                    self.state = ActorState.STOPPED
                else:
                    self.state = ActorState.ERROR
                    self.error = self.error or ctx.get_error()
                return True

            else:
                signal_result = self.actor.on_signal(signal)
                if ctx.get_error():
                    self.state = ActorState.ERROR
                    self.error = ctx.get_error()
                return signal_result

        except Exception as error:
            self.state = ActorState.ERROR
            self.error = self.error or error
            return None

        finally:
            self.actor._Actor__ctx = None

    def _check_current_state(self, allowed_states: tp.List[ActorState], signal: Signal) -> bool:

        # Duplicate life-cycle events are possible, reject them with a warning

        if self.state not in allowed_states:
            warning = f"Signal dropped: [{signal.message}] {signal.sender} -> {self.actor_id}" + \
                      f" (signal out of sequence, current state is {self.state.name})"
            self._log.warning(warning)
            return False

        return True

    def _new_child_id(self, actor_class: Actor.__class__) -> ActorId:

        classname = actor_class.__name__.lower()
        child_name = f"{classname}-{self.next_child_number}"

        self.next_child_number += 1

        if self.actor_id == ActorSystem.ROOT_ID:
            return f"{ActorSystem.DELIMITER}{child_name}"
        else:
            return f"{self.actor_id}{ActorSystem.DELIMITER}{child_name}"

    @classmethod
    def _parent_id(cls, actor_id: ActorId) -> ActorId:

        parent_delim = actor_id.rfind(ActorSystem.DELIMITER)
        parent_id = ActorSystem.ROOT_ID if parent_delim == 0 else actor_id[:parent_delim]

        return parent_id

    def _check_message_target(self, msg: Msg):

        if msg.target != self.actor_id:

            message = f"Message ignored: [{msg.message}] -> {msg.target}" + \
                      f" ({self.actor_id} received this message by mistake)"

            self._log.warning(message)
            return False

        controllers = ["/system", self.actor_id, self._parent_id(self.actor_id)]

        if msg.message in SignalNames.CONTROL_SIGNALS and msg.sender not in controllers:

            message = f"Control signal ignored: [{msg.message}] -> {self.actor_id}" + \
                      f" ({msg.sender} is not allowed to stop this actor)"

            self._log.warning(message)
            return False

        return True

    def _check_message_signature(self, target_id: ActorId, target_handler: tp.Callable, message: str, args, kwargs):

        if target_handler is None:
            error = f"Invalid message: [{message}] -> {target_id} (unknown message '{message}')"
            self._log.error(error)
            raise EBadActor(error)

        target_params = target_handler.params  # noqa
        type_hints = target_handler.type_hints  # noqa

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

            # If no type hint is available, allow anything through
            # Otherwise, reuse the validator logic to type check individual args
            type_hint = type_hints.get(pos_param.name)
            type_check = type_hint is None or _val.check_type(type_hint, pos_arg)

            if not type_check:
                error = f"Invalid message: [{message}] -> {target_id} (wrong parameter type for '{pos_param.name}')"
                self._log.error(error)
                raise EBadActor(error)

        # Kw arg types
        for kw_param in kw_params:

            kw_arg = kwargs.get(kw_param.name)

            # If param has taken a default value, no type check is needed
            if kw_arg is None:
                continue

            # Otherwise use the same type-validation logic as positional args
            type_hint = type_hints.get(kw_param.name)
            type_check = type_hint is None or _val.check_type(type_hint, kw_arg)

            if not type_check:
                error = f"Invalid message: [{message}] -> {target_id} (wrong parameter type for '{kw_param.name}')"
                self._log.error(error)
                raise EBadActor(error)


class RootActor(Actor):

    def __init__(self, main_actor: Actor, started: threading.Event, stopped: threading.Event):
        super().__init__()
        self.main_id: tp.Optional[ActorId] = None
        self.main_actor = main_actor
        self._started = started
        self._stopped = stopped

    def on_start(self):
        self.main_id = self.actors().spawn(self.main_actor)

    def on_stop(self):
        self._stopped.set()

    def on_signal(self, signal: Signal) -> tp.Optional[bool]:

        if signal.sender == self.main_id:

            if signal.message == SignalNames.STARTED:
                self._started.set()
                return True

            if signal.message == SignalNames.STOPPED:
                if self.state() in [ActorState.RUNNING, ActorState.STARTING]:
                    self.actors().stop(self.actors().id)
                return True

            if signal.message == SignalNames.FAILED and isinstance(signal, ErrorSignal):
                if not self._started.is_set():
                    self._started.set()
                self.actors().fail(signal.error)
                return True

        return False


class ActorSystem:

    DELIMITER = "/"
    ROOT_ID = DELIMITER

    __SHUTDOWN_TIME = 0.01

    def __init__(
            self, main_actor: Actor, system_thread: str = "actor_system",
            thread_pools: tp.Dict[str, int] = None,
            thread_pool_mapping: tp.Dict[type, str] = None):

        super().__init__()

        self._log = _logging.logger_for_object(self)

        # self.__actors: tp.Dict[ActorId, ActorNode] = {self.__ROOT_ID: ActorNode("", self.__ROOT_ID, None)}
        # self.__message_queue: tp.List[Msg] = list()

        self.__system_event_loop = EventLoop()
        self.__system_thread = threading.Thread(
            name=system_thread,
            target=self.__system_event_loop.main)

        self.__pools: tp.Dict[str, EventLoopPool] = {}
        self.__pool_mapping = thread_pool_mapping or {}

        if thread_pools is not None:
            self._setup_event_loops(thread_pools)

        self.__root_started = threading.Event()
        self.__root_stopped = threading.Event()

        self.__root_actor = RootActor(main_actor, self.__root_started, self.__root_stopped)
        self.__root_node = ActorNode(self.ROOT_ID, self.__root_actor, None, self, self.__system_event_loop)

    # Public API

    def main_id(self) -> ActorId:
        if not self.__root_started.is_set():
            raise EBadActor("System has not started yet")
        return self.__root_actor.main_id

    def start(self, wait=True):

        self.__system_thread.start()
        self.__root_node.send_signal("/system", self.ROOT_ID, SignalNames.START)

        for pool in self.__pools.values():
            pool.start()

        if wait:
            # TODO: Startup timeout
            self.__root_started.wait()

    def stop(self):

        self.__root_node.send_signal("/system", self.ROOT_ID, SignalNames.STOP)

    def wait_for_shutdown(self, timeout: tp.Optional[float] = None):

        self.__root_stopped.wait()

        # This short delay lets message processing finish before stopping the event loops
        # The real solution for this is to wait for all children to stop, before considering an actor STOPPED
        # There would need to be a timeout, including a root timeout

        time.sleep(self.__SHUTDOWN_TIME)

        for pool in self.__pools.values():
            pool.shutdown()
            pool.join(timeout)

        self.__system_event_loop.shutdown()
        self.__system_thread.join(timeout)

    def shutdown_code(self) -> int:

        if self.__root_node.state == ActorState.STOPPED and not self.__root_node.error:
            return 0
        else:
            return -1

    def shutdown_error(self) -> tp.Optional[Exception]:

        return self.__root_node.error

    def spawn_agent(self, agent: Actor) -> ActorId:

        if not self.__root_started.is_set():
            raise EBadActor("System has not started yet")

        return self.__root_node.spawn(agent)

    def send_main(self, message: str, *args, **kwargs):

        if self.__root_actor.main_id is None:
            raise EBadActor("System has not started yet")

        self.__root_node.send_message("/external", self.__root_actor.main_id, message, args, kwargs)  # TODO

    def send(self, actor_id: ActorId, message: str, *args, **kwargs):

        if not self.__root_started.is_set():
            raise EBadActor("System has not started yet")

        self.__root_node.send_message("/external", actor_id, message, args, kwargs)

    def _setup_event_loops(self, thread_pools: tp.Dict[str, int]):

        for pool_name, pool_size in thread_pools.items():
            self.__pools[pool_name] = EventLoopPool(pool_name, pool_size)

    def _allocate_event_loop(self, actor_class: Actor.__class__) -> EventLoop:

        if actor_class == RootActor.__class__ or actor_class == self.__root_actor.main_actor.__class__:
            return self.__system_event_loop

        preferred_pool = self.__pool_mapping.get(actor_class)

        if preferred_pool is not None and preferred_pool in self.__pools:
            return self.__pools[preferred_pool].get_loop()

        # Fall back to the system loop for now
        # We could allow setting an alternative default pool

        return self.__system_event_loop
