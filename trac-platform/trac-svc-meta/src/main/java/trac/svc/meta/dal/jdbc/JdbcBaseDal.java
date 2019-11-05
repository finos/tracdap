package trac.svc.meta.dal.jdbc;

import trac.svc.meta.exception.TracException;

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

    <TResult> CompletableFuture<TResult>
    wrapTransaction(JdbcFunction<TResult> func, JdbcErrorHandler jdbcHandler, TracErrorHandler tracHandler) {

        return CompletableFuture.supplyAsync(() -> {

            try (Connection conn = source.getConnection()) {

                conn.setAutoCommit(false);

                TResult result = func.apply(conn);
                conn.commit();

                return result;
            }
            catch (SQLException error) {
                throw new TracException("");
            }
            catch (TracException error) {
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

            try (Connection conn = source.getConnection()) {

                conn.setAutoCommit(false);

                func.apply(conn);
                conn.commit();
            }
            catch (SQLException error) {
                throw new TracException("");
            }
            catch (TracException error) {
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

        void handle(TracException error, JdbcErrorCode code);
    }
}
