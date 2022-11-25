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

#
# This is tracdap-svc-orch with local Python executor.
#
# In a future tracdap-svc-orch will be able to run a separate container
# with executor - in that case tracdap-svc-orch.Dockerfile will be
# recommended.
#

FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless python3.10-venv && \
    rm -rf /var/lib/apt/lists/*

COPY tracdap-platform/tracdap-svc-orch /opt/trac/current

RUN python3 -m venv /opt/trac/current/venv && \
    /opt/trac/current/venv/bin/pip3 install "tracdap-runtime == 0.5.0" && \
    /opt/trac/current/venv/bin/pip3 cache purge

ENTRYPOINT ["/opt/trac/current/bin/tracdap-svc-orch", "run"]
