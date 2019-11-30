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

    void executeDirect(JdbcAction func) throws SQLException {

        try (var conn = source.getConnection()) {

            conn.setAutoCommit(false);

            func.apply(conn);
            conn.commit();
        }
    }

    <TResult> CompletableFuture<TResult>
    wrapTransaction(JdbcFunction<TResult> func, JdbcErrorHandler... jdbcHandlers) {

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

                for (JdbcErrorHandler handler: jdbcHandlers)
                    handler.handle(error, code);

                // If the error code is not handled, throw an internal error type
                throw JdbcError.unhandledError(error, code);
            }

        }, executor);
    }

    CompletableFuture<Void>
    wrapTransaction(JdbcAction func, JdbcErrorHandler... jdbcHandlers) {

        return wrapTransaction(conn -> {func.apply(conn); return null;}, jdbcHandlers);
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
        final TItem item;

        KeyedItem(long key, TItem item) {
            this.key = key;
            this.item = item;
        }
    }

    static class KeyedItems<TItem> {

        final long[] keys;
        final TItem[] items;

        KeyedItems(long[] keys, TItem[] items) {
            this.keys = keys;
            this.items = items;
        }
    }
}
