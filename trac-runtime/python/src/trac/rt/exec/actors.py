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


T = tp.TypeVar('T')

ActorId = str


class ActorContext:

    def __init__(self, system: ActorSystem,
                 current_actor: ActorId, parent: ActorId, sender: tp.Optional[ActorId],
                 send_func: tp.Callable, spawn_func: tp.Callable):

        self.self = current_actor
        self.parent = parent
        self.sender = sender

        self.__system = system
        self.__send_func = send_func
        self.__spawn_func = spawn_func

    def spawn(self, actor_class: Actor.__class__, *args, **kwargs) -> ActorId:
        return self.__spawn_func(actor_class, args, kwargs)

    def send(self, target_id: ActorId, message: str, *args, **kwargs):
        self.__send_func(target_id, message, args, kwargs)

    def send_parent(self, message: str, *args, **kwargs):
        self.__send_func(self.parent, message, args, kwargs)

    def reply(self, message: str, *args, **kwargs):
        self.__send_func(self.sender, message, args, kwargs)


class Actor:

    START = "actor:start"
    STOP = "actor:stop"

    __class_handlers: tp.Dict[type, tp.Dict[str, tp.Callable]] = dict()

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

        if msg.message == Actor.START:
            handler = self.on_start

        elif msg.message == Actor.STOP:
            handler = self.on_stop

        else:
            handler = self.__handlers.get(msg.message)

        if not handler:
            # TODO: Notify unhandled messages
            print(f"Unhandled message: {self.__class__.__name__} {msg.message}")
            return

        self.__ctx = ctx
        handler(*msg.args, **msg.kwargs)
        self.__ctx = None


# class ActorRef:
#
#     def __init__(self, actor_id: ActorId, actor: self.
#         self.__system = system
#         self.__actor_id = actor_id
#
#     def send(self, target: tp.Union[ActorId, ActorRef], message, *args, **kwargs):
#
#         target_id = target if target is ActorId else target.__actor_id
#
#         self.__system._send_message(self.__actor_id, target_id, message, args, kwargs)


class Message:  # noqa

    def __init__(self, method):
        self.__method = method
        func.update_wrapper(self, method)

    def __call__(self, *args, **kwargs):
        self.__method(*args, **kwargs)


class Msg:

    def __init__(self, sender: ActorId, target: ActorId, message: str, args: tp.List[tp.Any], kwargs: tp.Dict[str, tp.Any]):
        self.sender = sender
        self.target = target
        self.message = message
        self.args = args
        self.kwargs = kwargs




class ActorMapping:

    def __init__(self, actor: Actor, children: tp.Dict[str, ActorMapping] = None):
        self.actor = actor
        self.children = children or {}


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

        actor_id = parent_id + "/" + actor_class.__name__.lower()
        actor = actor_class(*args, **kwargs)

        self.__actors[actor_id] = actor
        self._send_message(parent_id, actor_id, Actor.START, [], {})

        return actor_id

    def _start_actor(self, actor_id: ActorId):

        self._send_message("/system", actor_id, 'actor:start', [], {})

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
        parent_id = msg.target[-msg.target.rfind("/")]

        send_func = func.partial(self._send_message, msg.target)
        spawn_func = func.partial(self._spawn_actor, msg.target)

        ctx = ActorContext(self, msg.target, parent_id, msg.sender, send_func, spawn_func)

        actor._receive(msg, ctx)

    def _lookup_actor(self, actor_id: ActorId) -> Actor:

        actor = self.__actors.get(actor_id)

        if not actor:
            raise RuntimeError()  # TODO: Error

        return actor
