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

import trac.rt.exec.actors as actors
import unittest


class ActorSystemTest(unittest.TestCase):

    def test_basic_example(self):

        class PlusActor(actors.Actor):

            def __init__(self, value):
                super().__init__()
                self.value = value

            @actors.Message
            def add(self, ctx: actors.ActorContext, inc):

                new_value = self.value + inc
                self.value = new_value

                ctx.send_parent('new_value', new_value)

        class RootActor(actors.Actor):

            def __init__(self):
                super().__init__()

            @actors.Message
            def start(self):
                plus = self._system.spawn(PlusActor, value=0)
                plus.send('start')

            @actors.Message
            def new_value(self, new_value):
                print(new_value)
                if new_value < 10:
                    self._ctx.send('add')
                else:
                    self.stop()

        root = RootActor()
        system = actors.ActorSystem()
