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

import trac.rt.exec.actor as actor
import unittest


class ActorSystemTest(unittest.TestCase):

    def test_basic_example(self):

        class PlusActor(actor.Actor[int]):

            def __init__(self, ctx: int):
                super().__init__(ctx)

            @actor.Message
            def add(self, inc):
                new_value = self._ctx + inc
                self._parent.send('new_value', new_value)
                self.become(new_value)

        class RootActor(actor.Actor[actor.ActorRef]):

            def __init__(self):
                super().__init__(None)

            @actor.Message
            def start(self):
                plus = self._system.spawn(PlusActor, value=0)
                plus.send('start')
                self.become(plus)

            @actor.Message
            def new_value(self, new_value):
                print(new_value)
                if new_value < 10:
                    self._ctx.send('add')
                else:
                    self.stop()

        root = RootActor()
        system = actor.ActorSystem()
