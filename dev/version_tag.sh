#!/usr/bin/env sh

# Copyright 2022 Accenture Global Solutions Limited
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

# For NPM Publish, we need to specify a release tag
# This ia a quick way to generate the tag based on the full version string
# The version number should be as output from version.sh

VERSION=$1

if echo $VERSION | grep -qv "^[0-9]\+\.[0-9]\+\.[0-9]\+"; then
  echo "invalid"
  exit -1
elif echo $VERSION | grep -q "^[0-9]\+\.[0-9]\+\.[0-9]\+$"; then
  echo "latest"
elif echo $VERSION | grep -q "dev"; then
  echo "dev"
elif echo $VERSION | grep -q "alpha"; then
  echo "alpha"
elif  echo $VERSION | grep -q "beta"; then
  echo "beta"
elif echo $VERSION | grep -q "rc"; then
  echo "rc"
else
  echo "invalid"
  exit -1
fi
