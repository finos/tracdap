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
batch_stdout="${BATCH_STDOUT}"
batch_stderr="${BATCH_STDERR}"

if [ -n "${batch_stdout}" ] && [ -n "${batch_stderr}" ]; then
  ($@ >"${batch_stdout}" 2>"${batch_stderr}" </dev/null && echo 0 > "${batch_admin_dir}/exit_code") &
else
  ($@ >/dev/null 2>/dev/null </dev/null && echo 0 > "${batch_admin_dir}/exit_code") &
fi

pid=$!
echo $pid > "${batch_admin_dir}/pid"
