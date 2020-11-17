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


# Code gen creates outputs under artificial locations:
#
#   trac_gen.domain.trac.metadata - TRAC metadata domain objects for use in the API
#   trac_gen.proto.trac.metadata - Output of Google's native protoc for Python
#
# The generator respects the package layout of the source .proto files, however
# having the top level trac namespace defined in two source roots means only one
# set of packages gets imported. When the TRAC package is distributed we can get
# around this by copying the output of the generator under the correct locations
# in the package, which will be as follows:
#
#   trac_gen.domain.trac.metadata -> trac.metadata
#   trac_gen.proto.trac.metadata -> trac.impl.proto
#
# This proxy import means the API will work as expected during development
# without needing to run packaging jobs.

from trac_gen.domain.trac.metadata import *
