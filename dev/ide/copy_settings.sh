#!/usr/bin/env sh

# Copyright 2020 Accenture Global Solutions Limited
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

echo Updating settings for IntelliJ IDEA...

copyRecursive() {

  source_dir=$1
  target_dir=$2

  for source_sub_dir in `find "${source_dir}" -type d`; do
    target_sub_dir=`echo ${source_sub_dir} | sed "s#${source_dir}#${target_dir}#"`
    mkdir -p $target_sub_dir
  done

  for source_config_file in `find "${source_dir}" -type f`; do
    target_config_file=`echo ${source_config_file} | sed "s#${source_dir}#${target_dir}#"`
    echo "Updating -> ${source_config_file}"
    cp "${source_config_file}" "${target_config_file}"
  done
}

ide_source_dir=`dirname "$0"`/idea
ide_target_dir=`dirname "$0"`/../../.idea

copyRecursive "${ide_source_dir}" "${ide_target_dir}"
