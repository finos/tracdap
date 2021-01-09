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

import trac.rt.impl.util as util

ActorId = str


class ActorContext:

    def __init__(self, system: ActorSystem,
                 current_actor: ActorId, parent: ActorId, sender: tp.Optional[ActorId],
                 send_func: tp.Callable, spawn_func: tp.Callable, stop_func: tp.Callable):

        self.id = current_actor
        self.parent = parent
        self.sender = sender

        self.__system = system
        self.__send_func = send_func
        self.__spawn_func = spawn_func
        self.__stop_func = stop_func

    def spawn(self, actor_class: Actor.__class__, *args, **kwargs) -> ActorId:
        return self.__spawn_func(actor_class, args, kwargs)

    def send(self, target_id: ActorId, message: str, *args, **kwargs):
        self.__send_func(target_id, message, args, kwargs)

    def send_parent(self, message: str, *args, **kwargs):
        self.__send_func(self.parent, message, args, kwargs)

    def reply(self, message: str, *args, **kwargs):
        self.__send_func(self.sender, message, args, kwargs)

    def stop(self, target_id: tp.Optional[ActorId] = None):

        if target_id:
            self.__stop_func(target_id)
        else:
            self.__stop_func(self.id)


class Actor:

    START = "actor:start"
    STOP = "actor:stop"

    __class_handlers: tp.Dict[type, tp.Dict[str, tp.Callable]] = dict()
    __log: tp.Optional[util.logging.Logger] = None

    def __init__(self):
        self.__handlers = self._inspect_handlers()
        self.__ctx: tp.Optional[ActorContext] = None

    def actors(self) -> ActorContext:
        return self.__ctx

    def on_start(self):
        pass

    def on_stop(self):
        pass

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

    def _receive(self, msg: Msg, ctx: ActorContext):

        try:
            self.__ctx = ctx

            if msg.message == Actor.START:
                self.on_start()

            elif msg.message == Actor.STOP:
                self.on_stop()

            else:
                handler = self.__handlers.get(msg.message)

                if handler:
                    handler(*msg.args, **msg.kwargs)

                else:
                    # Unhandled messages are dropped, with just a warning in the log
                    log = util.logger_for_class(Actor)
                    log.warning(f"Message ignored: [{msg.message}] -> {msg.target}" +
                                f" (actor {self.__class__.__name__} does not support this message)")

        finally:
            self.__ctx = None


# Static member __log can only be set after class Actor is declared
Actor._Actor__log = util.logger_for_class(Actor)


class Message:  # noqa

    def __init__(self, method):
        self.__method = method
        func.update_wrapper(self, method)

    def __call__(self, *args, **kwargs):
        self.__method(*args, **kwargs)


class Msg:

    def __init__(self, sender: ActorId, target: ActorId, message: str,
                 args: tp.List[tp.Any], kwargs: tp.Dict[str, tp.Any]):

        self.sender = sender
        self.target = target
        self.message = message
        self.args = args
        self.kwargs = kwargs


class ActorSystem:

    def __init__(self, main_actor: Actor, system_thread: str = "actor_system"):

        self._log = util.logger_for_object(self)

        self.__actors: tp.Dict[ActorId, Actor] = dict()
        self.__message_queue: tp.List[Msg] = list()

        self.__main_id = "/engine"
        self.__actors[self.__main_id] = main_actor

        self.__system_thread = threading.Thread(
            name=system_thread,
            target=self._actor_system_main)

        self.__system_lock = threading.Lock()
        self.__system_up = threading.Event()

    def start(self, wait=False):

        self.__system_thread.start()
        self._start_actor(self.__main_id)

        if wait:
            self.__system_up.wait()  # TODO: Startup timeout

    def stop(self):
        pass

    def wait_for_shutdown(self):

        self.__system_thread.join()

    def send(self, message: str, *args, **kwargs):

        self._send_message("/external", self.__main_id, message, args, kwargs)

    def _spawn_actor(self, parent_id: ActorId, actor_class: Actor.__class__, args, kwargs):

        actor_id = self._new_actor_id(parent_id, actor_class)
        actor = actor_class(*args, **kwargs)

        self.__actors[actor_id] = actor
        self._send_message(parent_id, actor_id, Actor.START, [], {})

        return actor_id

    def _start_actor(self, actor_id: ActorId):

        self._send_message("/system", actor_id, 'actor:start', [], {})

    def _stop_actor(self, sender_id: ActorId, target_id: ActorId):

        self._send_message(sender_id, target_id, Actor.STOP, [], {})

    def _send_message(self, sender_id: ActorId, target_id: ActorId, message: str, args, kwargs):

        _args = args or []
        _kwargs = kwargs or {}

        msg = Msg(sender_id, target_id, message, _args, _kwargs)
        self.__message_queue.append(msg)

    def _actor_system_main(self):

        self._message_loop()

    def _message_loop(self):

        self._log.info("Begin normal operations")
        self.__system_up.set()

        while True:

            if len(self.__message_queue):
                next_msg = self.__message_queue.pop(0)
            else:
                next_msg = None

            if next_msg:
                self._process_message(next_msg)

    def _process_message(self, msg: Msg):

        actor = self._lookup_actor(msg.target)

        if not actor:
            # Unhandled messages are dropped, with just a warning in the log
            self._log.warning(f"Message ignored: [{msg.message}] -> {msg.target}  (target actor not found)")
            return

        parent_id = self._parent_id(msg.target)
        send_func = func.partial(self._send_message, msg.target)
        spawn_func = func.partial(self._spawn_actor, msg.target)
        stop_func = func.partial(self._stop_actor, msg.target)

        ctx = ActorContext(self, msg.target, parent_id, msg.sender, send_func, spawn_func, stop_func)

        actor._receive(msg, ctx)

    def _lookup_actor(self, actor_id: ActorId) -> tp.Optional[Actor]:

        return self.__actors.get(actor_id)

    def _new_actor_id(self, parent_id: ActorId, actor_class: Actor.__class__) -> ActorId:

        return parent_id + "/" + actor_class.__name__.lower()

    def _parent_id(self, actor_id: ActorId) -> ActorId:

        parent_delim = actor_id.rfind("/")
        parent_id = actor_id[0] if parent_delim == 0 else actor_id[:actor_id.rfind("/")]

        return parent_id
