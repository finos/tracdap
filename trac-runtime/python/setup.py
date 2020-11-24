#!/usr/bin/env python

#  Copyright 2020 Accenture Global Solutions Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import setuptools
import subprocess as sp
import platform


def get_trac_version():

    if platform.system().lower().startswith("win"):
        command = ['powershell', '-ExecutionPolicy', 'Bypass', '-File', '..\\..\\dev\\version.ps1']
    else:
        command = ['../../dev/version.sh']

    process = sp.Popen(command, stdout=sp.PIPE)
    (output, err) = process.communicate()
    exit_code = process.wait()

    if exit_code != 0:
        raise RuntimeError('Failed to get TRAC version')

    return output.decode('utf-8').strip()


trac_version = get_trac_version()
print(f'TRAC version: {trac_version}')

# Use trac. as a namespace, so models will import from trac.rt
# Allows for future packages e.g. trac.web
trac_rt_packages = setuptools.find_namespace_packages('src', include=['trac.*'])

# Map the metadata packages to generated output locations
trac_rt_package_dir = {
    '': 'src',
    'trac.rt.metadata': 'generated/trac_gen/domain/trac/metadata',
    'trac.rt.metadata.search': 'generated/trac_gen/domain/trac/metadata/search'
}

# Runtime dependencies
# Protoc is not required, it is only needed for codegen
trac_rt_dependencies = [
    'protobuf',
    'pandas',
    'pyspark']


setuptools.setup(
    name='trac-runtime',
    version=trac_version,
    description='TRAC Model Runtime for Python',
    url='https://github.com/accenture/trac',
    license='http://www.apache.org/licenses/LICENSE-2.0',
    platforms=['any'],
    python_requires='>=3.6',
    packages=trac_rt_packages,
    package_dir=trac_rt_package_dir,
    namespace_packages=['trac'],
    install_requires=trac_rt_dependencies)
