package trac.svc.meta.dal.jdbc;

import trac.svc.meta.dal.jdbc.dialects.Dialect;
import trac.svc.meta.dal.jdbc.dialects.IDialect;
import trac.svc.meta.exception.TracError;
import trac.svc.meta.exception.TracInternalError;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
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
    wrapTransaction(JdbcFunction<TResult> func, JdbcErrorHandler jdbcHandler, TracErrorHandler tracHandler) {

        return CompletableFuture.supplyAsync(() -> {

            try (var conn = source.getConnection()) {

                conn.setAutoCommit(false);

                var result = func.apply(conn);
                conn.commit();

                return result;
            }
            catch (SQLException error) {

                // TODO: Standardise logging and error handling

                JdbcErrorCode code = dialect.mapErrorCode(error);

                // If the error code is not recognised, throw an internal error type
                if (code == JdbcErrorCode.UNKNOWN_ERROR_CODE) {

                    var message = MessageFormat.format(
                            "Unrecognised SQL Error code: {} {}",
                            dialect.dialectCode(), error.getErrorCode());

                    throw new TracInternalError(message, error);
                }

                if (jdbcHandler != null)
                    jdbcHandler.handle(error, code);

                // If the error code is not handled, throw an internal error type
                var message = MessageFormat.format("Unhandled SQL Error code: {}", code.name());
                throw new TracInternalError(message, error);
            }
            catch (TracError error) {

                if (tracHandler != null)
                    tracHandler.handle(error);

                // If there is no explicit handler, re-throw the original error
                // This should happen when an original error was handled lower down
                throw error;
            }

        }, executor);
    }

    <TResult> CompletableFuture<TResult>
    wrapTransaction(JdbcFunction<TResult> func, JdbcErrorHandler jdbcHandler) {

        return wrapTransaction(func, jdbcHandler, null);
    }

    <TResult> CompletableFuture<TResult>
    wrapTransaction(JdbcFunction<TResult> func) {

        return wrapTransaction(func, null, null);
    }

    CompletableFuture<Void>
    wrapTransaction(JdbcAction func, JdbcErrorHandler jdbcHandler, TracErrorHandler tracHandler) {

        return wrapTransaction(conn -> {
            func.apply(conn);
            return null;
        },
        jdbcHandler, tracHandler);
    }

    CompletableFuture<Void>
    wrapTransaction(JdbcAction func, JdbcErrorHandler jdbcHandler) {

        return wrapTransaction(func, jdbcHandler, null);
    }

    CompletableFuture<Void>
    wrapTransaction(JdbcAction func) {

        return wrapTransaction(func, null, null);
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

    @FunctionalInterface
    interface TracErrorHandler {

        void handle(TracError error);
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
