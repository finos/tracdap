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

"""
Check that the exact (==) dependency pins declared in setup.cfg match the
corresponding pins in the requirements files.

setup.cfg's install_requires (and the extras) is what the published
tracdap-runtime wheel declares, and therefore what gets installed into
release artifacts. requirements.txt / requirements_plugins.txt are used for
development and CI. If a version is bumped in one but not the other (for
example a security fix applied to requirements.txt only), the release
artifact silently ships the old version - and the compliance checks, which
install from requirements.txt, will not notice.

This script fails if any package pinned with '==' in both setup.cfg and the
requirements files has mismatched versions.
"""

import configparser
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).parent

SETUP_CFG = HERE.joinpath("setup.cfg")
REQUIREMENTS_FILES = [
    HERE.joinpath("requirements.txt"),
    HERE.joinpath("requirements_plugins.txt"),
]

PIN_PATTERN = re.compile(r"^\s*([A-Za-z0-9_.\-]+)\s*==\s*([0-9][^\s#;,]*)")


def normalize(name):
    return name.lower().replace("_", "-")


def parse_pins(lines):
    pins = {}
    for line in lines:
        match = PIN_PATTERN.match(line)
        if match:
            pins[normalize(match.group(1))] = match.group(2)
    return pins


def setup_cfg_pins():

    cfg = configparser.ConfigParser()
    cfg.read(SETUP_CFG)

    lines = []

    if cfg.has_option("options", "install_requires"):
        lines += cfg.get("options", "install_requires").splitlines()

    if cfg.has_section("options.extras_require"):
        for _, value in cfg.items("options.extras_require"):
            lines += value.splitlines()

    return parse_pins(lines)


def requirements_pins():

    pins = {}
    for req_file in REQUIREMENTS_FILES:
        if req_file.exists():
            pins.update(parse_pins(req_file.read_text().splitlines()))
    return pins


def main():

    setup_pins = setup_cfg_pins()
    req_pins = requirements_pins()

    mismatches = []
    for pkg, setup_version in sorted(setup_pins.items()):
        req_version = req_pins.get(pkg)
        if req_version is not None and req_version != setup_version:
            mismatches.append((pkg, setup_version, req_version))

    if mismatches:
        print("Dependency pin mismatch between setup.cfg and the requirements files:")
        print()
        print(f"  {'package':24} {'setup.cfg':16} {'requirements':16}")
        print(f"  {'-' * 24} {'-' * 16} {'-' * 16}")
        for pkg, setup_version, req_version in mismatches:
            print(f"  {pkg:24} {setup_version:16} {req_version:16}")
        print()
        print("setup.cfg governs the versions installed into release artifacts, so it")
        print("must be kept in sync with the requirements files. Update the mismatched")
        print("pins so both agree.")
        return 1

    print(f"Dependency pins are consistent ({len(setup_pins)} pins checked in setup.cfg).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
