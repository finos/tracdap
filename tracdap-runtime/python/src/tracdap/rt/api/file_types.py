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

from tracdap.rt.metadata import *  # DOCGEN_REMOVE


class CommonFileTypes:

    TXT = FileType("txt", "text/plain")

    JPG = FileType("jpg", "image/jpeg")
    PNG = FileType("png", "image/png")
    SVG = FileType("svg", "image/svg+xml")

    WORD = FileType("docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    EXCEL = FileType("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    POWERPOINT = FileType("pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation")
