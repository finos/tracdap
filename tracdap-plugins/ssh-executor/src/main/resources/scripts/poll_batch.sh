#!/bin/sh

# Copyright 2023 Accenture Global Solutions Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

batch_admin_dir="${BATCH_ADMIN_DIR}"

pid=`cat "${batch_admin_dir}/pid"`
echo "pid: ${pid}"

ps -p $pid >/dev/null 2>&1
running=$?
echo "running: ${running}"

if [ ${running} -ne 0 ]; then
  if [ -e "${batch_admin_dir}/exit_code" ]; then
    exit_code=`cat "${batch_admin_dir}/exit_code"`
  else
    exit_code=1
  fi
  echo "exit_code: ${exit_code}"
fi

echo "trac_poll_ok: ok"
