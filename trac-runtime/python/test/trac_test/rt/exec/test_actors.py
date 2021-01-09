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

import trac.rt.impl.util as util
import trac.rt.exec.actors as actors
import unittest


class ActorSystemTest(unittest.TestCase):

    def setUp(self):
        util.configure_logging()

    def test_basic_example(self):

        program_output = {
            'root_id': None,
            'plus_id': None,
            'result': None,
            'stop_seq': []
        }

        class PlusActor(actors.Actor):

            def __init__(self, value):
                super().__init__()
                self.value = value

            @actors.Message
            def add(self, inc):

                new_value = self.value + inc
                self.value = new_value

                self.actors().send_parent('new_value', new_value)

            def on_start(self):
                program_output['plus_id'] = self.actors().id

            def on_stop(self):
                print("Plus actor got a stop message")
                program_output['result'] = self.value
                program_output['stop_seq'].append(self.actors().id)

        class RootActor(actors.Actor):

            def __init__(self):
                super().__init__()
                self.plus = None

            def on_start(self):
                print("Start root")
                program_output['root_id'] = self.actors().id
                self.plus = self.actors().spawn(PlusActor, 0)
                self.actors().send(self.plus, "add", 1)

            def on_stop(self):
                print("Root actor got a stop message")
                program_output['stop_seq'].append(self.actors().id)

            @actors.Message
            def new_value(self, new_value):
                print(new_value)
                if new_value < 10:
                    self.actors().send(self.plus, 'add', 1)
                else:
                    self.actors().stop()

        root = RootActor()
        system = actors.ActorSystem(root)
        system.start(wait=True)
        system.wait_for_shutdown()

        self.assertEqual(program_output['result'], 10)

        self.assertIsNotNone(program_output['root_id'])
        self.assertIsNotNone(program_output['plus_id'])

        expected_stop_seq = [program_output['plus_id'], program_output['root_id']]
        self.assertEqual(program_output['stop_seq'], expected_stop_seq)
