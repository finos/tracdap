/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.meta.dal.jdbc.dialects;

import com.accenture.trac.svc.meta.exception.TracInternalError;
import com.accenture.trac.svc.meta.dal.jdbc.JdbcDialect;
import com.accenture.trac.svc.meta.dal.jdbc.JdbcErrorCode;
import com.accenture.trac.svc.meta.dal.jdbc.JdbcException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


public abstract class Dialect implements IDialect {

    public static IDialect dialectFor(JdbcDialect dialect) {

        switch (dialect) {

            case MYSQL: return new MySqlDialect();
            case H2: return new H2SqlDialect();

            default: throw new RuntimeException("Unsupported JDBC dialect: " + dialect.name());
        }
    }


    private final Map<Integer, JdbcErrorCode> syntheticErrorCodes;

    protected Dialect() {

        syntheticErrorCodes = new HashMap<>();

        for (var error : JdbcErrorCode.values())
            syntheticErrorCodes.put(error.ordinal(), error);
    }

    @Override
    public final JdbcErrorCode mapErrorCode(SQLException error) {

        if (JdbcException.SYNTHETIC_ERROR.equals(error.getSQLState()))
            return syntheticErrorCodes.getOrDefault(error.getErrorCode(), JdbcErrorCode.UNKNOWN_ERROR_CODE);
        else
            return mapDialectErrorCode(error);
    }

    protected abstract JdbcErrorCode mapDialectErrorCode(SQLException error);

    protected String loadKeyMappingDdl(String keyMappingDdl) {

        var classLoader = getClass().getClassLoader();

        try (var stream = classLoader.getResourceAsStream(keyMappingDdl)) {

            if (stream == null) {
                var message = String.format("Internal startup error (DAL, JDBC, %s - missing DDL resource for key mapping", dialectCode());
                throw new TracInternalError(message);
            }

            try (var rawReader = new InputStreamReader(stream); var reader = new BufferedReader(rawReader)) {
                return reader.lines().collect(Collectors.joining(System.lineSeparator()));
            }
        }
        catch (IOException e) {
            var message = String.format("Internal startup error (DAL, JDBC, %s - error preparing DDL resource for key mapping", dialectCode());
            throw new TracInternalError(message);
        }
    }
}
