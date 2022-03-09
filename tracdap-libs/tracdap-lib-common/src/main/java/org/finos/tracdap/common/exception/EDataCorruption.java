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

package org.finos.tracdap.common.exception;


/**
 * Data is considered corrupt when it cannot be understood according to its own format
 *
 * How corrupt data is detected depends on the data format. For example, for JSON text data,
 * a data stream might be corrupt if it contains an invalid Unicode sequence, or if the JSON
 * fails to parse, for example because of a missing brace. For binary formats can occur in the
 * structure of the binary data stream and will be detected if that data stream cannot be
 * understood.
 *
 * Data corruption does not include data constraints, such as non-null or range constraints,
 * which are reported as EDataConstraint.
 */
public class EDataCorruption extends EData {

    public EDataCorruption(String message, Throwable cause) {
        super(message, cause);
    }

    public EDataCorruption(String message) {
        super(message);
    }
}
