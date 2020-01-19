package trac.svc.meta.dal.jdbc;

import trac.svc.meta.dal.jdbc.dialects.Dialect;
import trac.svc.meta.dal.jdbc.dialects.IDialect;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;


class JdbcBaseDal {

    private final IDialect dialect;
    private final DataSource source;
    private final Executor executor;

    JdbcBaseDal(JdbcDialect dialect, DataSource source, Executor executor) {
        this.dialect = Dialect.dialectFor(dialect);
        this.source = source;
        this.executor = executor;
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

    <TResult> CompletableFuture<TResult>
    wrapTransaction(JdbcFunction<TResult> func, JdbcErrorHandler... errorHandlers) {

        return CompletableFuture.supplyAsync(() -> {

            try (var conn = source.getConnection()) {

                conn.setAutoCommit(false);

                var result = func.apply(conn);
                conn.commit();

                return result;
            }
            catch (SQLException error) {

                // TODO: Logging?

                var code = dialect.mapErrorCode(error);

                // If the error code is not recognised, throw an internal error type
                JdbcError.handleUnknownError(error, code, dialect);

                for (JdbcErrorHandler handler: errorHandlers)
                    handler.handle(error, code);

                // If the error code is not handled, throw an internal error type
                throw JdbcError.unhandledError(error, code);
            }

        }, executor);
    }

    CompletableFuture<Void>
    wrapTransaction(JdbcAction func, JdbcErrorHandler... errorHandlers) {

        return wrapTransaction(conn -> {func.apply(conn); return null;}, errorHandlers);
    }

    @FunctionalInterface
    interface JdbcFunction <TResult> {

        TResult apply(Connection conn) throws SQLException;
    }

    @FunctionalInterface
    interface JdbcAction {

        void apply(Connection conn) throws SQLException;
    }

    @FunctionalInterface
    interface JdbcErrorHandler {

        void handle(SQLException error, JdbcErrorCode code);
    }

    static class KeyedItem<TItem> {

        final long key;
        final int version;
        final TItem item;

        KeyedItem(long key, int version, TItem item) {
            this.key = key;
            this.version = version;
            this.item = item;
        }

        KeyedItem(long key, TItem item) {
            this.key = key;
            this.version = 0;
            this.item = item;
        }
    }

    static class KeyedItems<TItem> {

        final long[] keys;
        final int[] versions;
        final TItem[] items;

        KeyedItems(long[] keys, int[] versions, TItem[] items) {
            this.keys = keys;
            this.versions = versions;
            this.items = items;
        }

        KeyedItems(long[] keys, TItem[] items) {
            this.keys = keys;
            this.versions = null;
            this.items = items;
        }
    }
}
