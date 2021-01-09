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

import trac.rt.exec.runtime as runtime


def launch_cli():
    pass


def launch_session(sys_config):
    pass


def launch_job(job_config, sys_config):

    with runtime.TracRuntime(sys_config, job_config, dev_mode=True) as rt:
        rt.submit_batch()
        rt.wait_for_shutdown()


def launch_model(model_class, job_config, sys_config):

    # TODO: Put model class into job config

    with runtime.TracRuntime(sys_config, job_config, dev_mode=True) as rt:
        rt.submit_batch()
        rt.wait_for_shutdown()
