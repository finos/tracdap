/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.data.validation;

import java.util.List;
import java.util.regex.Pattern;


public class ValidationConstants {

    public static final Pattern VALID_IDENTIFIER = Pattern.compile("\\A[a-zA-Z]\\w*\\Z");

    // Identifiers starting trac_ are reserved for use by the TRAC platform

    public static final Pattern TRAC_RESERVED_IDENTIFIER = Pattern.compile(
            "\\Atrac_.*", Pattern.CASE_INSENSITIVE);

    public static final Pattern MIME_TYPE = Pattern.compile("\\A\\w+/[-.\\w]+(?:\\+[-.\\w]+)?\\Z");

    public static final List<String> REGISTERED_MIME_TYPES = List.of(
            "application", "audio", "font", "example",
            "image", "message", "model", "multipart",
            "text", "video");
}
