#!/usr/bin/env sh

# Licensed to the Fintech Open Source Foundation (FINOS) under one or
# more contributor license agreements. See the NOTICE file distributed
# with this work for additional information regarding copyright ownership.
# FINOS licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with the
# License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Extract semantic version number from the latest version tag in Git
# Version tags are prefixed with the letter 'v'

exact_version_tag=`git describe --tags --match "v[0-9]*" --exact-match 2>/dev/null`
exact_version_found=$?

build_version_tag=`git describe --tags --match "v[0-9]*" 2>/dev/null`
prior_version_tag=`git describe --tags --match "v[0-9]*" --abbrev=0 2>/dev/null`
prior_version_found=$?

if [ -n "${TRAC_EXPLICIT_VERSION_NUMBER}" ]; then

  # Explicit version number supplied externally
  version_number=${TRAC_EXPLICIT_VERSION_NUMBER}

elif [ ${exact_version_found} = 0 ]; then

  # Exact version number found from a tag on the current commit
  version_number=`echo ${exact_version_tag} | sed s/^v//`

elif [ ${prior_version_found} = 0 ]; then

  # Generate a -SNAPSHOT version for the next primary version number

  prior_version=`echo ${prior_version_tag} | sed s/^v//`

  if [ `echo "${prior_version}" | grep -E "^[0-9]+\.[0-9]+\.[0-9]+$"` ]; then
      # Whole version number, bump patch for next patch version
      next_patch_version=`echo "${prior_version}" | awk -F. '{$NF=$NF+1; print}' OFS=.`
  else
      # Pre-release, e.g. -rc.1, remove suffix for next patch version
      next_patch_version=`echo "${prior_version}" | sed "s/-.*$//"`
  fi

  # Next patch version exists but is not in the revision history -> release branch exists
  if [ `git tag | grep "^v${next_patch_version}$"` ]; then
      next_minor_version=`echo "${next_patch_version}" | awk -F. '{$2=$2+1; $3=0; print}' OFS=.`
      version_number=${next_minor_version}-SNAPSHOT
  else
      version_number=${next_patch_version}-SNAPSHOT
  fi

else

  version_number="DEVELOPMENT"

fi

echo "${version_number}"
