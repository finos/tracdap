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

package org.finos.tracdap.common.db;

import org.finos.tracdap.common.db.dialects.Dialect;
import org.finos.tracdap.common.db.dialects.IDialect;
import org.finos.tracdap.common.exception.ETrac;
import org.finos.tracdap.common.exception.ETracInternal;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;


public class JdbcBaseDal {

    private static final String UNHANDLED_ERROR = "Unhandled SQL Error code: {0}";

    private final DataSource source;
    private final JdbcErrorHandler errorHandler;

    protected final IDialect dialect;

    protected JdbcBaseDal(DataSource source, JdbcDialect dialect) {
        this.source = source;
        this.errorHandler = new FallbackErrorHandler();
        this.dialect = Dialect.dialectFor(dialect);
    }

    protected void executeDirect(JdbcAction func) throws SQLException {

        try (var conn = source.getConnection()) {

            conn.setAutoCommit(false);

            func.apply(conn);
            conn.commit();
        }
    }

    protected <TResult> TResult wrapTransaction(JdbcFunction<TResult> func) {

        return wrapTransaction(func, this.errorHandler);
    }

    protected <TResult> TResult wrapTransaction(JdbcFunction<TResult> func, JdbcErrorHandler errorHandler) {

        try (var conn = source.getConnection()) {

            conn.setAutoCommit(false);

            var result = func.apply(conn);
            conn.commit();

            return result;
        }
        catch (SQLException error) {

            var errorCode = dialect.mapErrorCode(error);
            throw errorHandler.handle(error, errorCode);
        }
    }

    protected void wrapTransaction(JdbcAction func) {

        wrapTransaction(conn -> {
            func.apply(conn);
            return null;
        });
    }

    @FunctionalInterface
    protected interface JdbcFunction <TResult> {

        TResult apply(Connection conn) throws SQLException;
    }

    @FunctionalInterface
    protected interface JdbcAction {

        void apply(Connection conn) throws SQLException;
    }

    @FunctionalInterface
    protected interface JdbcErrorHandler {

        ETrac handle(SQLException error, JdbcErrorCode errorCode);
    }

    private static class FallbackErrorHandler implements JdbcErrorHandler {

        @Override
        public ETrac handle(SQLException error, JdbcErrorCode errorCode) {
            var message = MessageFormat.format(UNHANDLED_ERROR, errorCode.name());
            return new ETracInternal(message, error);
        }
    }
}
