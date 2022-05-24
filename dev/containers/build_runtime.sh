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

REPO_DIR=$(cd "$(dirname "$0")/../.." && pwd)

BUILD_IMAGE=python:3.10
TRAC_IMAGE=tracdap/runtime-python


# Look up TRAC version
echo Looking up TRAC version...

git fetch --tags
git remote | grep upstream >/dev/null && git fetch upstream --tags

TRAC_VERSION=$("${REPO_DIR}/dev/version.sh")

# Docker cannot handle "+dev" version suffixes
# For anything that is not a tagged release, use version DEVELOPMENT for the image tag
if test "${TRAC_VERSION#*+dev}" != "${TRAC_VERSION}"
then
  TRAC_VERSION=DEVELOPMENT
fi

echo TRAC version = ${TRAC_VERSION}


set -x
docker run --mount "type=bind,source=${REPO_DIR},target=/mnt/tracdap" ${BUILD_IMAGE} /mnt/tracdap/dev/containers/build_runtime_inner.sh
docker build -t "${TRAC_IMAGE}:${TRAC_VERSION}" "${REPO_DIR}/tracdap-runtime/python"
