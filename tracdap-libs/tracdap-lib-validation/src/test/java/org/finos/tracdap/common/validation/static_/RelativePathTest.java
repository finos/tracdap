/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.validation.static_;

import org.finos.tracdap.common.validation.test.BaseValidatorTest;
import org.finos.tracdap.metadata.ExternalLocation;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


class RelativePathTest extends BaseValidatorTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "loans.csv",
            "data/loans.csv",
            "data/2026/loans.csv",
            "data\\loans.csv"})
    void relativePath_ok(String path) {

        expectValid(location(path));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "/loans.csv",
            "/data/loans.csv",
            "\\data\\loans.csv",
            "data//loans.csv",
            "data\\\\loans.csv",
            "//server/share/loans.csv",
            "../loans.csv",
            "data/../../loans.csv",
            "C:\\loans.csv",
            "data/loans\\2026.csv"})
    void relativePath_invalid(String path) {

        expectInvalid(location(path));
    }

    private static ExternalLocation location(String path) {

        return ExternalLocation.newBuilder()
                .setStorageKey("external_storage")
                .setStoragePath(path)
                .build();
    }
}
