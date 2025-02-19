#  Licensed to the Fintech Open Source Foundation (FINOS) under one or
#  more contributor license agreements. See the NOTICE file distributed
#  with this work for additional information regarding copyright ownership.
#  FINOS licenses this file to you under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with the
#  License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
TRAC model API for Python
"""

# Make metadata classes available to client code when importing the API package
# Remove this import when generating docs, so metadata classes are only documented once
from tracdap.rt.metadata import *  # noqa DOCGEN_REMOVE

# static_api overrides some metadata types for backwards compatibility with pre-0.8 versions
# Make sure it is last in the list
from .constants import *
from .model_api import *
from .static_api import *

