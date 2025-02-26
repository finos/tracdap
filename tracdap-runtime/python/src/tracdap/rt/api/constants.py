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

import tracdap.rt.metadata


class CommonFileTypes:

    """
    A collection of common :py:class:`FileTypes <tracdap.rt.metadata.FileType>` to use as model inputs and outputs
    """

    TXT = tracdap.rt.metadata.FileType("txt", "text/plain")

    JPG = tracdap.rt.metadata.FileType("jpg", "image/jpeg")
    PNG = tracdap.rt.metadata.FileType("png", "image/png")
    SVG  = tracdap.rt.metadata.FileType("svg", "image/svg+xml")

    WORD = tracdap.rt.metadata.FileType("docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    EXCEL = tracdap.rt.metadata.FileType("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    POWERPOINT = tracdap.rt.metadata.FileType("pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation")


# Map basic types into the root of the API package

BOOLEAN = tracdap.rt.metadata.BasicType.BOOLEAN
"""Synonym for :py:attr:`BasicType.BOOLEAN <tracdap.rt.metadata.BasicType.BOOLEAN>`"""

INTEGER = tracdap.rt.metadata.BasicType.INTEGER
"""Synonym for :py:attr:`BasicType.INTEGER <tracdap.rt.metadata.BasicType.INTEGER>`"""

FLOAT = tracdap.rt.metadata.BasicType.FLOAT
"""Synonym for :py:attr:`BasicType.FLOAT <tracdap.rt.metadata.BasicType.FLOAT>`"""

DECIMAL = tracdap.rt.metadata.BasicType.DECIMAL
"""Synonym for :py:attr:`BasicType.DECIMAL <tracdap.rt.metadata.BasicType.DECIMAL>`"""

STRING = tracdap.rt.metadata.BasicType.STRING
"""Synonym for :py:attr:`BasicType.STRING <tracdap.rt.metadata.BasicType.STRING>`"""

DATE = tracdap.rt.metadata.BasicType.DATE
"""Synonym for :py:attr:`BasicType.DATE <tracdap.rt.metadata.BasicType.DATE>`"""

DATETIME = tracdap.rt.metadata.BasicType.DATETIME
"""Synonym for :py:attr:`BasicType.DATETIME <tracdap.rt.metadata.BasicType.DATETIME>`"""
