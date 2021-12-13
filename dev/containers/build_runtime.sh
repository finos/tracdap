#!/usr/bin/env sh

# Copyright 2021 Accenture Global Solutions Limited
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

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
TRAC_VERSION=DEVELOPMENT

# TODO: Docker tags complain about version called XXX+devYY
# $("${SCRIPT_DIR}/../version.sh")

git fetch --tags
git remote | grep upstream >/dev/null && git fetch upstream --tags

docker run --mount "type=bind,source=${SCRIPT_DIR}/../..,target=/mnt/trac" python:3.10 /mnt/trac/dev/containers/build_runtime_inner.sh
docker build -t "trac/runtime-python:${TRAC_VERSION}" "${SCRIPT_DIR}/../../trac-runtime/python"
