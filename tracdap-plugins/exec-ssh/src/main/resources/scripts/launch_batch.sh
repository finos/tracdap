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

batch_admin_dir="${TRAC_BATCH_ADMIN_DIR}"
log_out="${TRAC_BATCH_STDOUT}"
log_err="${TRAC_BATCH_STDERR}"

$@ >"${log_out}" 2>"${log_err}" </dev/null &

pid=$!
echo $pid > "${batch_admin_dir}/pid"

wait $pid

exit_code=$?
echo $exit_code > "${batch_admin_dir}/exit_code"
