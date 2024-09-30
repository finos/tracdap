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
# Version tags are prefixed with the letter 'v'

$exact_version_tag = ((git describe --tags --match "v[0-9]*" --exact-match 2>$null) | Out-String).Trim()
$exact_version_found = $(git describe --tags --match "v[0-9]*" --exact-match 2>$null; $?)

$build_version_tag = ((git describe --tags --match "v[0-9]*" 2>$null) | Out-String).Trim()
$prior_version_tag = ((git describe --tags --match "v[0-9]*" --abbrev=0 2>$null) | Out-String).Trim()
$prior_version_found = $(git describe --tags --match "v[0-9]*" --abbrev=0 2>$null; $?)


if ($exact_version_found) {

    $version_number = ((echo ${exact_version_tag}) -replace '^v' | Out-String).Trim()
}
elseif ($prior_version_found) {

    $prior_version = ((echo ${prior_version_tag}) -replace '^v' | Out-String).Trim()
    $build_version = ((echo ${build_version_tag}) -replace '^v' | Out-String).Trim()
    $build_suffix = ((echo ${build_version}) -replace "${prior_version}-" | Out-String).Trim()
    $version_number = ((echo "${prior_version}+dev${build_suffix}") | Out-String).Trim()
}
else {

    $version_number = "DEVELOPMENT"
}

echo "${version_number}"