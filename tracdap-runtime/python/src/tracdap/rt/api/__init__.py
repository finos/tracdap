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


from .model_api import *
from .static_api import *

# Make metadata classes available to client code when importing the API package
# Remove this import when generating docs, so metadata classes are only documented once
from tracdap.rt.metadata import *  # noqa DOCGEN_REMOVE

# Map basic types into the root of the API package
BOOLEAN = BasicType.BOOLEAN
INTEGER = BasicType.INTEGER
FLOAT = BasicType.FLOAT
DECIMAL = BasicType.DECIMAL
STRING = BasicType.STRING
DATE = BasicType.DATE
DATETIME = BasicType.DATETIME
