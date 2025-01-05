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
from .file_types import *
from .model_api import *
from .static_api import *

# Map basic types into the root of the API package

BOOLEAN = BasicType.BOOLEAN
"""Synonym for :py:attr:`BasicType.BOOLEAN <tracdap.rt.metadata.BasicType.BOOLEAN>`"""

INTEGER = BasicType.INTEGER
"""Synonym for :py:attr:`BasicType.INTEGER <tracdap.rt.metadata.BasicType.INTEGER>`"""

FLOAT = BasicType.FLOAT
"""Synonym for :py:attr:`BasicType.FLOAT <tracdap.rt.metadata.BasicType.FLOAT>`"""

DECIMAL = BasicType.DECIMAL
"""Synonym for :py:attr:`BasicType.DECIMAL <tracdap.rt.metadata.BasicType.DECIMAL>`"""

STRING = BasicType.STRING
"""Synonym for :py:attr:`BasicType.STRING <tracdap.rt.metadata.BasicType.STRING>`"""

DATE = BasicType.DATE
"""Synonym for :py:attr:`BasicType.DATE <tracdap.rt.metadata.BasicType.DATE>`"""

DATETIME = BasicType.DATETIME
"""Synonym for :py:attr:`BasicType.DATETIME <tracdap.rt.metadata.BasicType.DATETIME>`"""
