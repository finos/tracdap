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

    public static final Pattern PATH_ILLEGAL_CHARS = Pattern.compile(".*[<>\"|?*].*");
    public static final Pattern PATH_ILLEGAL_WHITESPACE = Pattern.compile(".*[\\s\\vR&&[^ ]].*");
    public static final Pattern PATH_ILLEGAL_CTRL = Pattern.compile(".*\\p{Cntrl}.*");

    public static final Pattern PATH_SEPARATORS = Pattern.compile("[/\\\\]");
    public static final String UNIX_PATH_SEPARATOR = "/";
    public static final String WINDOWS_PATH_SEPARATOR = "\\";

    public static final Pattern PATH_SINGLE_DOT = Pattern.compile("\\A\\.\\Z");
    public static final Pattern PATH_DOUBLE_DOT = Pattern.compile("\\A\\.\\.\\Z");

    public static final Pattern FILENAME_ILLEGAL_CHARS = Pattern.compile(".*[:/\\\\].*");
    public static final Pattern FILENAME_ILLEGAL_START = Pattern.compile("\\A[ ].*");
    public static final Pattern FILENAME_ILLEGAL_ENDING = Pattern.compile(".*[. ]\\Z");
    public static final Pattern FILENAME_RESERVED = Pattern.compile(
            "\\A(COM\\d*|LPT\\d*|PRN|AUX|NUL)(\\..*)?\\Z", Pattern.CASE_INSENSITIVE);

    // Relative path constraints
    // Since : is illegal, C: and file: are illegal
    // Anything starting with a slash is absolute
    // UNC-style paths and empty path segments are picked up by the double slash
    public static final Pattern RELATIVE_PATH_ILLEGAL_CHARS = Pattern.compile(".*[:].*");
    public static final Pattern RELATIVE_PATH_IS_ABSOLUTE = Pattern.compile("\\A[/\\\\]");
    public static final Pattern RELATIVE_PATH_DOUBLE_SLASH = Pattern.compile("[/\\\\]{2}");

    // Data item key is a logical key not a path
    // But a path-like structure is used to create unique, deterministic keys
    // Segments are limited alphanumeric, underscore, dot and hyphen, with the unix slash as the separator
    public static final Pattern DATA_ITEM_KEY = Pattern.compile("\\A\\w[\\w-.]*(/\\w[\\w-.]*)*\\Z");

    // Opaque part key is deterministic based on the full part key definition
    // There are three types of part key, single (root), part by range and part by value
    public static final Pattern OPAQUE_PART_KEY = Pattern.compile("\\Apart-(root|range-.*|value-.*)\\Z");

    // Model entry point is a class in a package structure
    // Package and class names are identifiers, separated by dots
    // Any depth of package structure is allowed, including zero
    // So pkg.sub_pkg.ModelClass and ModelClass are both valid
    public static final Pattern MODEL_ENTRY_POINT = Pattern.compile("\\A[a-zA-Z]\\w*(\\.[a-zA-Z]\\w*)*\\Z");

    // Version can be a release version number or a commit hash
    // Common version schemes allow hyphen and period, we also allow underscore
    // E.g. v1.3-beta.1 or v1.3-dev_suffix
    // Commit hash might start with a number
    // If these criteria are too restrictive we made need to relax this later
    public static final Pattern MODEL_VERSION = Pattern.compile("\\A\\p{Alnum}[\\w-.]*\\Z");
}
