package trac.svc.meta.dal.jdbc;

import trac.svc.meta.exception.TracError;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;


class JdbcBaseDal {

    private final DataSource source;
    private final Executor executor;

    JdbcBaseDal(DataSource source, Executor executor) {
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
                throw new TracError("", error);
            }
            catch (TracError error) {
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

        return CompletableFuture.runAsync(() -> {

            try (var conn = source.getConnection()) {

                conn.setAutoCommit(false);

                func.apply(conn);
                conn.commit();
            }
            catch (SQLException error) {
                throw new TracError("", error);
            }
            catch (TracError error) {
                throw error;
            }

        }, executor);
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

        void handle(TracError error, JdbcErrorCode code);
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
