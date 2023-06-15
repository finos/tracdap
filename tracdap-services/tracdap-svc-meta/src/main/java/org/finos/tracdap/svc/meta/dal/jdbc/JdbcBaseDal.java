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

package org.finos.tracdap.svc.meta.dal.jdbc;

import org.finos.tracdap.common.db.JdbcDialect;
import org.finos.tracdap.svc.meta.dal.jdbc.dialects.Dialect;
import org.finos.tracdap.svc.meta.dal.jdbc.dialects.IDialect;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;


class JdbcBaseDal {

    private final DataSource source;
    protected final IDialect dialect;

    JdbcBaseDal(DataSource source, JdbcDialect dialect) {
        this.source = source;
        this.dialect = Dialect.dialectFor(dialect);
    }

    void prepareMappingTable(Connection conn) throws SQLException {
        dialect.prepareMappingTable(conn);
    }

    void executeDirect(JdbcAction func) throws SQLException {

        try (var conn = source.getConnection()) {

            conn.setAutoCommit(false);

            func.apply(conn);
            conn.commit();
        }
    }

    <TResult> TResult wrapTransaction(JdbcFunction<TResult> func) {

        try (var conn = source.getConnection()) {

            conn.setAutoCommit(false);

            var result = func.apply(conn);
            conn.commit();

            return result;
        }
        catch (SQLException error) {

            throw JdbcError.catchAll(error, dialect);
        }
    }

    void wrapTransaction(JdbcAction func) {

        wrapTransaction(conn -> {
            func.apply(conn);
            return null;
        });
    }

    @FunctionalInterface
    interface JdbcFunction <TResult> {

        TResult apply(Connection conn) throws SQLException;
    }

    @FunctionalInterface
    interface JdbcAction {

        void apply(Connection conn) throws SQLException;
    }

    static class KeyedItem<TItem> {

        final long key;
        final int version;
        final Instant timestamp;
        final TItem item;
        final boolean isLatest;

        KeyedItem(long key, int version, Instant timestamp, TItem item, boolean isLatest) {
            this.key = key;
            this.version = version;
            this.timestamp = timestamp;
            this.item = item;
            this.isLatest = isLatest;
        }

        KeyedItem(long key, TItem item) {
            this(key, 0, null, item, false);
        }
    }

    static class KeyedItems<TItem> {

        final long[] keys;
        final int[] versions;
        final Instant[] timestamps;
        final TItem[] items;
        final boolean[] isLatest;

        KeyedItems(long[] keys, int[] versions, Instant[] timestamps, TItem[] items, boolean[] isLatest) {
            this.keys = keys;
            this.versions = versions;
            this.timestamps = timestamps;
            this.items = items;
            this.isLatest = isLatest;
        }

        KeyedItems(long[] keys, int[] versions, TItem[] items) {
            this(keys, versions, null, items, null);
        }

        KeyedItems(long[] keys, TItem[] items) {
            this(keys, null, items);
        }
    }
}
