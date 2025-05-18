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

package org.finos.tracdap.common.cache.jdbc;

import org.finos.tracdap.common.cache.CacheEntry;
import org.finos.tracdap.common.cache.CacheHelpers;
import org.finos.tracdap.common.cache.CacheTicket;
import org.finos.tracdap.common.cache.IJobCache;
import org.finos.tracdap.common.db.JdbcBaseDal;
import org.finos.tracdap.common.db.JdbcDialect;
import org.finos.tracdap.common.db.JdbcErrorCode;
import org.finos.tracdap.common.db.JdbcException;
import org.finos.tracdap.common.exception.*;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class JdbcJobCache <TValue extends Serializable> extends JdbcBaseDal implements IJobCache<TValue> {

    private static final int NEW_ENTRY_REVISION = 0;

    private final String cacheName;

    // Only JDBC cache manager can create instances
    JdbcJobCache(DataSource dataSource, JdbcDialect dialect, String cacheName) {
        super(dataSource, dialect);
        this.cacheName = cacheName;
    }

    @Override
    public CacheTicket openNewTicket(String key, Duration duration) {

        CacheHelpers.checkKey(key);
        CacheHelpers.checkDuration(duration);
        CacheHelpers.checkMaxDuration(key, duration);

        return wrapTransaction(conn -> {
            return openTicket(key, NEW_ENTRY_REVISION, duration, true, conn);
        });
    }

    @Override
    public CacheTicket openTicket(String key, int revision, Duration duration) {

        CacheHelpers.checkKey(key);
        CacheHelpers.checkRevision(revision);
        CacheHelpers.checkDuration(duration);
        CacheHelpers.checkMaxDuration(key, duration);

        return wrapTransaction(conn -> {
            return openTicket(key, revision, duration, false, conn);
        });
    }

    private CacheTicket openTicket(String key, int revision, Duration duration, boolean isNew, Connection conn) throws SQLException {

        // Clear any expired tickets, so they will not prevent a new grant
        clearExpiredTickets(conn);

        var query =
                "insert into cache_ticket (\n" +
                "  cache_name,\n" +
                "  entry,\n" +
                "  revision,\n" +
                "  grant_time,\n" +
                "  expiry_time\n" +
                ")\n" +
                "values (?, ?, ?, ?, ?)";

        var grantTime = Instant.now();
        var expiryTime = grantTime.plus(duration);

        long ticketPk;
        JdbcCacheTicket ticket;

        try (var stmt = dialect.supportsGeneratedKeys()
                ? conn.prepareStatement(query, new String[] { "ticket_pk" })
                : conn.prepareStatement(query)) {

            var sqlGrantTime = java.sql.Timestamp.from(grantTime);
            var sqlExpiryTime = java.sql.Timestamp.from(expiryTime);

            stmt.setString(1, cacheName);
            stmt.setString(2, key);
            stmt.setInt(3, revision);
            stmt.setTimestamp(4, sqlGrantTime);
            stmt.setTimestamp(5, sqlExpiryTime);

            stmt.executeUpdate();

            ticketPk = ticketPk(conn, stmt, key, revision);
            ticket = JdbcCacheTicket.forDuration(this, key, revision, grantTime, duration, ticketPk);
        }
        catch (SQLException error) {

            var code = dialect.mapErrorCode(error);

            if (code == JdbcErrorCode.INSERT_DUPLICATE)
                return CacheTicket.supersededTicket(key, revision, grantTime);

            throw error;
        }

        var entryQuery = "select entry_pk, revision from cache_entry where cache_name = ? and entry = ?";
        long entryPk;

        try (var stmt = conn.prepareStatement(entryQuery)) {

            stmt.setString(1, cacheName);
            stmt.setString(2, key);

            try (var rs = stmt.executeQuery()) {

                if (isNew) {
                    if (rs.next()) {
                        closeTicket(ticket, conn);
                        return CacheTicket.supersededTicket(key, revision, grantTime);
                    }
                    else {
                        return ticket;
                    }
                }

                if (!rs.next()){
                    closeTicket(ticket, conn);
                    return CacheTicket.missingEntryTicket(key, revision, grantTime);
                }

                var entryRevision = rs.getInt(2);

                if (entryRevision != revision) {
                    closeTicket(ticket, conn);
                    if (entryRevision > revision)
                        return CacheTicket.supersededTicket(key, revision, grantTime);
                    else
                        return CacheTicket.missingEntryTicket(key, revision, grantTime);
                }

                entryPk = rs.getLong(1);;
            }
        }

        var fkQuery = "update cache_ticket set entry_fk = ? where ticket_pk = ?";

        try (var fkStmt = conn.prepareStatement(fkQuery)) {
            fkStmt.setLong(1, entryPk);
            fkStmt.setLong(2, ticketPk);
            fkStmt.executeUpdate();
        }

        return ticket;
    }

    @Override
    public void closeTicket(CacheTicket ticket) {

        CacheHelpers.checkTicket(ticket);

        // Missing / superseded tickets do not have o DB record to close
        if (!(ticket instanceof JdbcCacheTicket))
            return;

        wrapTransaction(conn -> {
            closeTicket(jdbcTicket(ticket), conn);
        });
    }

    private void closeTicket(JdbcCacheTicket ticket, Connection conn) throws SQLException {

        var query =
            "delete from cache_ticket \n" +
            "where ticket_pk = ?";

        try (var stmt = conn.prepareStatement(query)) {
            stmt.setLong(1, ticket.ticketPk());
            stmt.executeUpdate();
        }
    }

    private void clearExpiredTickets(Connection conn) throws SQLException {

        var query =
                "delete from cache_ticket \n" +
                "where expiry_time < ?";

        try (var stmt = conn.prepareStatement(query)) {

            var sqlExpiry = java.sql.Timestamp.from(Instant.now());

            stmt.setTimestamp(1, sqlExpiry);
            stmt.executeUpdate();
        }
    }

    @Override
    public int createEntry(CacheTicket ticket, String status, TValue value) {

        var commitTime = Instant.now();

        CacheHelpers.checkValidTicket(ticket, "create", commitTime);
        CacheHelpers.checkValidStatus(ticket, status);
        CacheHelpers.checkValidValue(ticket, value);

        return wrapTransaction(conn -> {
            return createEntry(jdbcTicket(ticket), status, value, conn);
        });
    }

    private int createEntry(JdbcCacheTicket ticket, String status, TValue value, Connection conn) throws SQLException {

        checkJdbcTicket(ticket, conn);

        var encodedValue = CacheHelpers.encodeValue(value);
        var newRevision = ticket.revision() + 1;

        var query =
                "insert into cache_entry (\n" +
                "  cache_name,\n" +
                "  entry,\n" +
                "  revision,\n" +
                "  status,\n" +
                "  encoded_value\n" +
                ")\n" +
                "values (?, ?, ?, ?, ?)";

        long entryPk;

        try (var stmt = dialect.supportsGeneratedKeys()
                ? conn.prepareStatement(query, new String[] { "entry_pk" })
                : conn.prepareStatement(query)) {

            stmt.setString(1, cacheName);
            stmt.setString(2, ticket.key());
            stmt.setInt(3, newRevision);
            stmt.setString(4, status);
            stmt.setBytes(5, encodedValue);

            stmt.executeUpdate();

            entryPk = entryPk(conn, stmt, ticket.key());
        }
        catch (SQLException error) {

            var code = dialect.mapErrorCode(error);

            if (code == JdbcErrorCode.INSERT_DUPLICATE) {
                var message = String.format("Duplicate cache entry for %s [%s]", ticket.key(), cacheName);
                throw new ECacheDuplicate(message, error);
            }

            throw error;
        }

        var ticketUpdate = "update cache_ticket set entry_fk = ? where ticket_pk = ?";

        try (var stmt = conn.prepareStatement(ticketUpdate)) {

            stmt.setLong(1, entryPk);
            stmt.setLong(2, ticket.ticketPk());
            stmt.executeUpdate();
        }

        return newRevision;
    }

    @Override
    public int updateEntry(CacheTicket ticket, String status, TValue value) {

        var commitTime = Instant.now();
        CacheHelpers.checkValidTicket(ticket, "update", commitTime);
        CacheHelpers. checkValidStatus(ticket, status);
        CacheHelpers.checkValidValue(ticket, value);

        return wrapTransaction(conn -> {
            return updateEntry(jdbcTicket(ticket), status, value, conn);
        });
    }

    private int updateEntry(JdbcCacheTicket ticket, String status, TValue value, Connection conn) throws SQLException {

        checkJdbcTicket(ticket, conn);

        var query =
                "update cache_entry set\n" +
                "  revision = ?,\n" +
                "  status = ?,\n" +
                "  encoded_value = ?\n" +
                "where entry_pk = (select entry_fk from cache_ticket where ticket_pk = ?)\n" +
                "and revision = ?";

        var encodedValue = CacheHelpers.encodeValue(value);
        var newRevision = ticket.revision() + 1;

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setInt(1, newRevision);
            stmt.setString(2, status);
            stmt.setBytes(3, encodedValue);

            stmt.setLong(4, ticket.ticketPk());
            stmt.setInt(5, ticket.revision());

            var nRows = stmt.executeUpdate();

            if (nRows == 0) {
                var message = String.format("Cache entry not found for %s [%s]", ticket.key(), cacheName);
                throw new ECacheNotFound(message);
            }

            return newRevision;
        }
    }

    @Override
    public void deleteEntry(CacheTicket ticket) {

        var commitTime = Instant.now();
        CacheHelpers.checkValidTicket(ticket, "remove", commitTime);

        wrapTransaction(conn -> {
            deleteEntry(jdbcTicket(ticket), conn);
        });
    }

    private void deleteEntry(JdbcCacheTicket ticket, Connection conn) throws SQLException {

        checkJdbcTicket(ticket, conn);

        var query =
                "delete from cache_entry\n" +
                "where entry_pk = (select entry_fk from cache_ticket where ticket_pk = ?)\n" +
                "and revision = ?";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setLong(1, ticket.ticketPk());
            stmt.setInt(2, ticket.revision());

            var nRows = stmt.executeUpdate();

            if (nRows == 0) {
                var message = String.format("Cache entry not found for %s [%s]", ticket.key(), cacheName);
                throw new ECacheNotFound(message);
            }
        }
    }


    @Override
    public CacheEntry<TValue> readEntry(CacheTicket ticket) {

        var accessTime = Instant.now();
        CacheHelpers.checkValidTicket(ticket, "get", accessTime);

        return wrapTransaction(conn -> {
            return readEntry(jdbcTicket(ticket), conn);
        });
    }

    private CacheEntry<TValue> readEntry(JdbcCacheTicket ticket, Connection conn) throws SQLException {

        checkJdbcTicket(ticket, conn);

        var query =
                "select revision, status, encoded_value\n" +
                "from cache_entry\n" +
                "where entry_pk = (select entry_fk from cache_ticket where ticket_pk = ?)\n";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setLong(1, ticket.ticketPk());

            try (var rs = stmt.executeQuery()) {

                if (!rs.next()) {
                    var message = String.format("Cache entry not found for %s [%s]", ticket.key(), cacheName);
                    throw new ECacheNotFound(message);
                }

                var revision = rs.getInt(1);
                var status = rs.getString(2);
                var encodedValue = rs.getBytes(3);
                var value = encodedValue != null ? CacheHelpers.<TValue>decodeValue(encodedValue) : null;

                if (rs.next())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

                return CacheEntry.forValue(ticket.key(), revision, status, value);
            }
        }
    }

    @Override
    public Optional<CacheEntry<TValue>> queryKey(String key) {

        return wrapTransaction(conn -> {
            return queryKey(key, conn);
        });
    }

    private Optional<CacheEntry<TValue>> queryKey(String key, Connection conn) throws SQLException {

        var query =
                "select revision, status, encoded_value\n" +
                "from cache_entry\n" +
                "where cache_name = ?\n" +
                "and entry = ?";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setString(1, cacheName);
            stmt.setString(2, key);

            try (var rs = stmt.executeQuery()) {

                if (!rs.next())
                    return Optional.empty();

                var revision = rs.getInt(1);
                var status = rs.getString(2);
                var encodedValue = rs.getBytes(3);
                var value = encodedValue != null ?  CacheHelpers.<TValue>decodeValue(encodedValue) : null;

                if (rs.next())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

                var entry = CacheEntry.forValue(key, revision, status, value);

                return Optional.of(entry);
            }
        }
    }

    @Override
    public List<CacheEntry<TValue>> queryStatus(List<String> statuses) {

        return queryStatus(statuses, false);
    }

    @Override
    public List<CacheEntry<TValue>> queryStatus(List<String> statuses, boolean includeOpenTickets) {

        return wrapTransaction(conn -> {
            return queryStatus(statuses, includeOpenTickets, conn);
        });
    }

    private List<CacheEntry<TValue>> queryStatus(List<String> statuses, boolean includeOpenTickets, Connection conn) throws SQLException {

        var placeholders = IntStream.range(0, statuses.size())
                .mapToObj(i -> "?")
                .collect(Collectors.joining(", "));

        var query =
                "select entry, revision, status, encoded_value\n" +
                "from cache_entry\n" +
                "where cache_name = ?\n" +
                "and status in (" + placeholders + ")\n";

        var entries = new ArrayList<CacheEntry<TValue>>();

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setString(1, cacheName);

            for (int i = 0; i < statuses.size(); i++)
                stmt.setString(i + 2, statuses.get(i));

            try (var rs = stmt.executeQuery()) {
                while (rs.next()) {

                    var key = rs.getString(1);
                    var revision = rs.getInt(2);
                    var status = rs.getString(3);
                    var encodedValue = rs.getBytes(4);
                    var value = encodedValue != null ? CacheHelpers.<TValue>decodeValue(encodedValue) : null;

                    var entry = CacheEntry.forValue(key, revision, status, value);
                    entries.add(entry);
                }
            }
        }

        if (includeOpenTickets)
            return entries;

        var openTicketQuery = "select entry from cache_ticket";
        var openTickets = new HashSet<String>();

        try (var stmt = conn.prepareStatement(openTicketQuery); var rs = stmt.executeQuery()) {
            while (rs.next()) {

                var key = rs.getString(1);
                openTickets.add(key);
            }
        }

        return entries.stream()
                .filter(e -> !openTickets.contains(e.key()))
                .collect(Collectors.toList());
    }

    private void checkJdbcTicket(JdbcCacheTicket ticket, Connection conn) throws SQLException {

        var query =
                "select cache_name, entry, revision, entry_fk\n" +
                "from cache_ticket\n" +
                "where ticket_pk = ?";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setLong(1, ticket.ticketPk());

            try (var rs = stmt.executeQuery()) {

                if (!rs.next())
                    throw new ECacheTicket("Cache ticket is no longer valid");

                var cacheName = rs.getString(1);
                var entry = rs.getString(2);
                var revision = rs.getInt(3);

                if (rs.next())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

                if (!this.cacheName.equals(cacheName) || !ticket.key().equals(entry) || ticket.revision() != revision)
                    throw new ECacheTicket("Cache ticket is no longer valid");
            }
        }
    }

    private long entryPk(Connection conn, Statement stmt, String entry) throws SQLException {

        if (dialect.supportsGeneratedKeys())
            return generatedPk(stmt);
        else
            return entryPk(conn, entry);
    }

    private long entryPk(Connection conn, String entry) throws SQLException {

        var query = "select entry_pk from cache_entry where entry = ?";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setString(1, entry);

            try (var rs = stmt.executeQuery()) {
                return readPk(rs);
            }
        }
    }

    private long ticketPk(Connection conn, Statement stmt, String entry, int revision) throws SQLException {

        if (dialect.supportsGeneratedKeys())
            return generatedPk(stmt);
        else
            return ticketPk(conn, entry, revision);
    }

    private long ticketPk(Connection conn, String entry, int revision) throws SQLException {

        var query =
                "select ticket_pk from cache_ticket\n" +
                "where cache_name = ?\n" +
                "and entry = ?\n" +
                "and revision = ?";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setString(1, cacheName);
            stmt.setString(1, entry);
            stmt.setInt(2, revision);

            try (var rs = stmt.executeQuery()) {
                return readPk(rs);
            }
        }
    }

    private long generatedPk(Statement stmt) throws SQLException {

        try (ResultSet rs = stmt.getGeneratedKeys()) {
            return readPk(rs);
        }
    }

    private long readPk(ResultSet rs) throws SQLException {

        rs.next();
        long key = rs.getLong(1);

        if (rs.next())
            throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);

        return key;
    }

    private JdbcCacheTicket jdbcTicket(CacheTicket ticket) {

        if (!(ticket instanceof JdbcCacheTicket))
            throw new ECacheTicket("Invalid ticket for JDBC job cache");

        return (JdbcCacheTicket) ticket;
    }
}
