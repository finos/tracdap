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

import typing as tp

T = tp.TypeVar('T')


class Actor(tp.Generic[T]):

    def __init__(self, ctx: T):
        self._ctx = ctx
        self._parent: 'ActorRef' = None
        self._system: 'ActorSystem' = None
        self.__messages = {}

    def start(self):
        pass

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


class ActorRef:

    def __init__(self, actor: Actor, system: 'ActorSystem'):
        self._system = system

    def send(self, signal: callable, *args, **kwargs):
        pass


class ActorSystem:

    def __init__(self):
        self.__queue = []

    def spawn(self, actor: Actor.__class__, **actor_args) -> ActorRef:

        _actor = actor(**actor_args)
        _ref = ActorRef(_actor, self)

        return _ref
