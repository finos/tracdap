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

import threading
import inspect
import typing as tp

import trac.rt.impl.util as util


T = tp.TypeVar('T')


class Actor(tp.Generic[T]):

    def __init__(self, ctx: T):
        self._ctx = ctx
        self._parent: 'ActorRef' = None
        self._system: 'ActorSystem' = None
        self.__messages = {}

    def start(self):
        print("Actor start method")

    def stop(self):
        pass

    def receive(self, message: callable, *args, **kwargs):

        if message in self.__messages:
            method = self.__messages[message]
            method.__call__(*args, **kwargs)

        else:
            raise RuntimeError()  # TODO: Error

    def become(self, ctx: T):
        self._ctx = ctx


class Message:  # noqa

    def __init__(self, method):
        self.__method = method

    def __call__(self, **kwargs):
        self.__method(**kwargs)


class Msg:

    def __init__(self, ref: tp.List[str], method: tp.Callable, *args: tp.List[tp.Any]):
        self.ref = ref
        self.method: tp.Callable = method
        self.args = args


class ActorRef:

    def __init__(self, actor: Actor, system: 'ActorSystem'):
        self._system = system

    def send(self, signal: callable, *args, **kwargs):
        pass


class ActorMapping:

    def __init__(self, actor: Actor, children: tp.Dict[str, 'ActorMapping'] = None):
        self.actor = actor
        self.children = children or {}


class ActorSystem:

    def __init__(self, main_actor: Actor, system_thread: str = "actor_system"):

        self._log = util.logger_for_object(self)

        self.__system_thread = threading.Thread(
            name=system_thread,
            target=self._actor_system_main)

        self.__ref_map = ActorMapping(None)
        self.__message_queue = []

        main_actor_mapping = ActorMapping(main_actor)
        self.__ref_map.children['main'] = main_actor_mapping

    def start(self):

        self.__system_thread.start()

        main_actor = self._lookup_actor(['main'])
        self.send(['main'], type(main_actor).start)

    def stop(self):
        pass

    def wait_for_shutdown(self):

        self.__system_thread.join()

    def spawn(self):

        pass

    def send(self, ref: tp.List[str], method: tp.Callable, *args):

        msg = Msg(ref, method, *args)
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

        actor = self._lookup_actor(msg.ref)
        self._execute_message(actor, msg)

    def _execute_message(self, actor: Actor, msg: Msg):

        new_ctx = msg.method(actor, *msg.args)

        if new_ctx is not None:
            actor._ctx = new_ctx

    def _lookup_actor(self, ref: tp.List[str]) -> Actor:

        ref_map = self.__ref_map

        for segment in ref:

            child_map = ref_map.children.get(segment)

            if child_map:
                ref_map = child_map
            else:
                raise RuntimeError()  # TODO: Error

        return ref_map.actor

    def spawn(self, actor: Actor.__class__, **actor_args) -> ActorRef:

        _actor = actor(**actor_args)
        _ref = ActorRef(_actor, self)

        return _ref
