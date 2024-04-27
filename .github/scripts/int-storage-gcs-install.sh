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

GCLOUD_VERSION=473.0.0-linux-x86_64

apt-get update
apt-get install -y curl

curl "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-${GCLOUD_VERSION}.tar.gz" -o goocle-cloud-cli.tar.gz
tar -xzf goocle-cloud-cli.tar.gz
./google-cloud-sdk/install.sh --path-update=true --rc-path=~./gcp_paths --usage-reporting=false
