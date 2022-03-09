/*
 * Copyright 2022 Accenture Global Solutions Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.validation;

import java.util.List;
import java.util.regex.Pattern;


public class ValidationConstants {

    public static final Pattern MIME_TYPE = Pattern.compile("\\A\\w+/[-.\\w]+(?:\\+[-.\\w]+)?\\Z");

    public static final List<String> REGISTERED_MIME_TYPES = List.of(
            "application", "audio", "font", "example",
            "image", "message", "model", "multipart",
            "text", "video");

    public static final Pattern FILENAME_ILLEGAL_CHARS = Pattern.compile(".*[<>:\"/\\\\|?*].*");
    public static final Pattern FILENAME_ILLEGAL_WHITESPACE = Pattern.compile(".*[\\s\\vR&&[^ ]].*");
    public static final Pattern FILENAME_ILLEGAL_CTRL = Pattern.compile(".*\\p{Cntrl}.*");
    public static final Pattern FILENAME_ILLEGAL_START = Pattern.compile("\\A[ ].*");
    public static final Pattern FILENAME_ILLEGAL_ENDING = Pattern.compile(".*[. ]\\Z");
    public static final Pattern FILENAME_RESERVED = Pattern.compile(
            "\\A(COM\\d*|LPT\\d*|PRN|AUX|NUL)(\\..*)?\\Z", Pattern.CASE_INSENSITIVE);


}
