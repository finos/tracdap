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
import typing as tp

import trac.rt.impl.util as util


T = tp.TypeVar('T')

ActorId = str


class ActorContext:

    def __init__(self, current_actor: ActorId, sender: ActorId, system: ActorSystem):
        self.__current_actor = current_actor
        self.__sender = sender
        self.__system = system

    def spawn(self, actor_class: Actor.__class__, *args, **kwargs) -> ActorRef:

        actor = actor_class(*args, **kwargs)
        return actor.ref

    def send(self, target_id: ActorId, message: str, *args, **kwargs):

        self.__system._send_message(self.__current_actor, target_id, message, *args, **kwargs)


class Actor:

    def __init__(self):
        self.ref: ActorRef = None
        self._parent: ActorRef = None
        self._system: ActorSystem = None
        self.__handlers: tp.Dict[str, tp.Callable] = dict()

    def _receive(self, msg: 'Msg', ctx: ActorContext):

        # Handle system messages
        if msg.message == 'actor:start':
            self.on_start()

        elif msg.message == 'actor:stop':
            self.on_stop()

        else:

            handler = self.__handlers.get(msg.message)

            if handler:
                handler(ctx, *msg.args, **msg.kwargs)

            else:
                # TODO: Notify unhandled messages
                print(f"Unhandled message: {self.__class__.__name__} {msg.message}")

    def actors(self) -> ActorContext:
        pass

    def on_start(self):
        pass

    def on_stop(self):
        pass


class ActorRef:

    def __init__(self, actor: Actor, system: ActorSystem):
        self._system = system

    def send(self, signal: callable, *args, **kwargs):
        pass


class Message:  # noqa

    def __init__(self, method):
        self.__method = method

    def __call__(self, *args, **kwargs):
        self.__method(*args, **kwargs)


class Msg:

    def __init__(self, sender: ActorId, target: ActorId, message: str, *args, **kwargs):
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

    def start(self):

        self.__system_thread.start()
        self._start_actor(self.__main_id)

    def stop(self):
        pass

    def wait_for_shutdown(self):

        self.__system_thread.join()

    def _spawn_actor(self, parent_id: ActorId, actor_class: Actor.__class__, *args, **kwargs):

        pass

    def _start_actor(self, actor_id: ActorId):

        self._send_message("/system", actor_id, 'actor:start')

    def _send_message(self, sender_id: ActorId, target_id: ActorId, message: str, *args, **kwargs):

        msg = Msg(sender_id, target_id, message, args, kwargs)
        self.__message_queue.append(msg)

    def _actor_system_main(self):

        self._message_loop()

    def _message_loop(self):

        self._log.info("Begin normal operations")

        while True:

            if len(self.__message_queue):
                next_msg = self.__message_queue.pop(0)
            else:
                next_msg = None

            if next_msg:
                self._process_message(next_msg)

    def _process_message(self, msg: Msg):

        actor = self._lookup_actor(msg.target)
        ctx = ActorContext(msg.target, msg.sender, self)

        actor._receive(msg, ctx)

    def _lookup_actor(self, actor_id: ActorId) -> Actor:

        actor = self.__actors.get(actor_id)

        if not actor:
            raise RuntimeError()  # TODO: Error

        return actor

    def spawn(self, actor: Actor.__class__, **actor_args) -> ActorRef:

        _actor = actor(**actor_args)
        _ref = ActorRef(_actor, self)

        return _ref
