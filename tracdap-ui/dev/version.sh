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


# Extract semantic version number from the latest version tag in Git
# Version tags are prefixed with the letter 'v'. This is used in GitHub actions
# to tag release artefacts with the right version number.

exact_version_tag=`git describe --tags --match "v[0-9]*" --exact-match 2>/dev/null`
exact_version_found=$?

build_version_tag=`git describe --tags --match "v[0-9]*" 2>/dev/null`
prior_version_tag=`git describe --tags --match "v[0-9]*" --abbrev=0 2>/dev/null`
prior_version_found=$?


if [ ${exact_version_found} = 0 ]; then

  version_number=`echo ${exact_version_tag} | sed s/^v//`

elif [ ${prior_version_found} = 0 ]; then

  prior_version=`echo ${prior_version_tag} | sed s/^v//`
  build_version=`echo ${build_version_tag} | sed s/^v//`
  build_suffix=`echo ${build_version} | sed "s/${prior_version}-//"`
  version_number="${prior_version}+dev${build_suffix}"

else

  version_number="DEVELOPMENT"

fi

echo "${version_number}"
