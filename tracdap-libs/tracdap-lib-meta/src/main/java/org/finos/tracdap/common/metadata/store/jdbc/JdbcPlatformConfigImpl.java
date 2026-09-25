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

package org.finos.tracdap.common.metadata.store.jdbc;

import org.finos.tracdap.common.exception.EMetadataDuplicate;
import org.finos.tracdap.common.exception.EMetadataNotFound;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.store.PlatformConfigRecord;
import org.finos.tracdap.metadata.MetadataFormat;
import org.finos.tracdap.metadata.MetadataVersion;
import org.finos.tracdap.metadata.PlatformConfigEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;


class JdbcPlatformConfigImpl {

    private static final String MISSING_ENTRY = "Platform config not found for [{0}] [{1}]";
    private static final String DUPLICATE_ENTRY = "Platform config already exists for [{0}] [{1}]";
    private static final String PRIOR_ENTRY_MISMATCH = "Prior platform config entry [{0}] [{1}] has been superseded (expected version {2}, found version {3})";

    private final Logger log = LoggerFactory.getLogger(getClass());

    PlatformConfigEntry createPlatformConfigEntry(
            Connection conn, String configClass, String configKey,
            Instant timestamp, byte[] value) throws SQLException {

        var current = selectCurrentRow(conn, configClass, configKey);

        if (current != null && !current.deleted) {
            var message = MessageFormat.format(DUPLICATE_ENTRY, configClass, configKey);
            throw new EMetadataDuplicate(message);
        }

        var newVersion = current == null ? 1 : current.version + 1;
        var newEntry = buildEntry(configClass, configKey, newVersion, timestamp, false);

        if (current != null)
            closeCurrentRow(conn, configClass, configKey, timestamp);

        insertRow(conn, newEntry, timestamp, value);

        return newEntry;
    }

    PlatformConfigEntry updatePlatformConfigEntry(
            Connection conn, PlatformConfigEntry priorEntry,
            Instant timestamp, byte[] value) throws SQLException {

        var current = requireCurrentRow(conn, priorEntry);

        var newVersion = current.version + 1;
        var newEntry = buildEntry(priorEntry.getConfigClass(), priorEntry.getConfigKey(), newVersion, timestamp, false);

        closeCurrentRow(conn, priorEntry.getConfigClass(), priorEntry.getConfigKey(), timestamp);
        insertRow(conn, newEntry, timestamp, value);

        return newEntry;
    }

    PlatformConfigEntry deletePlatformConfigEntry(
            Connection conn, PlatformConfigEntry priorEntry, Instant timestamp) throws SQLException {

        var current = requireCurrentRow(conn, priorEntry);

        var newVersion = current.version + 1;
        var newEntry = buildEntry(priorEntry.getConfigClass(), priorEntry.getConfigKey(), newVersion, timestamp, true);

        closeCurrentRow(conn, priorEntry.getConfigClass(), priorEntry.getConfigKey(), timestamp);
        insertRow(conn, newEntry, timestamp, current.value);

        return newEntry;
    }

    PlatformConfigRecord loadPlatformConfigEntry(
            Connection conn, String configClass, String configKey, boolean includeDeleted) throws SQLException {

        var current = selectCurrentRow(conn, configClass, configKey);

        if (current == null || (current.deleted && !includeDeleted)) {
            var message = MessageFormat.format(MISSING_ENTRY, configClass, configKey);
            throw new EMetadataNotFound(message);
        }

        var entry = buildEntry(configClass, configKey, current.version, current.timestamp, current.deleted);

        return new PlatformConfigRecord(entry, current.value);
    }

    List<PlatformConfigRecord> loadPlatformConfigEntries(
            Connection conn, List<PlatformConfigEntry> configKeys, boolean includeDeleted) throws SQLException {

        var records = new ArrayList<PlatformConfigRecord>(configKeys.size());

        for (var key : configKeys) {
            var record = loadPlatformConfigEntry(conn, key.getConfigClass(), key.getConfigKey(), includeDeleted);
            records.add(record);
        }

        return records;
    }

    List<PlatformConfigEntry> listPlatformConfigEntries(
            Connection conn, String configClass, boolean includeDeleted) throws SQLException {

        var query = includeDeleted
                ? "select config_key, config_version, config_timestamp, config_deleted \n" +
                  "from platform_config where config_class = ? and config_is_latest = ? order by config_key"
                : "select config_key, config_version, config_timestamp, config_deleted \n" +
                  "from platform_config where config_class = ? and config_is_latest = ? and config_deleted = ? order by config_key";

        if (log.isDebugEnabled())
            log.debug("QUERY listPlatformConfigEntries: \n{}", query);

        var entries = new ArrayList<PlatformConfigEntry>();

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setString(1, configClass);
            stmt.setBoolean(2, true);

            if (!includeDeleted)
                stmt.setBoolean(3, false);

            try (var rs = stmt.executeQuery()) {

                while (rs.next()) {

                    var configKey = rs.getString(1);
                    var version = rs.getInt(2);
                    var timestamp = rs.getTimestamp(3).toInstant();
                    var deleted = rs.getBoolean(4);

                    entries.add(buildEntry(configClass, configKey, version, timestamp, deleted));
                }
            }
        }

        return entries;
    }

    private CurrentRow requireCurrentRow(Connection conn, PlatformConfigEntry priorEntry) throws SQLException {

        var configClass = priorEntry.getConfigClass();
        var configKey = priorEntry.getConfigKey();

        var current = selectCurrentRow(conn, configClass, configKey);

        if (current == null || current.deleted) {
            var message = MessageFormat.format(MISSING_ENTRY, configClass, configKey);
            throw new EMetadataNotFound(message);
        }

        if (current.version != priorEntry.getConfigVersion()) {
            var message = MessageFormat.format(
                    PRIOR_ENTRY_MISMATCH, configClass, configKey,
                    priorEntry.getConfigVersion(), current.version);
            throw new EMetadataDuplicate(message);
        }

        return current;
    }

    private CurrentRow selectCurrentRow(Connection conn, String configClass, String configKey) throws SQLException {

        // Platform config reads always fetch the latest entry
        // There is currently no support for reading a specific prior version or an as-of timestamp

        var query =
                "select config_version, config_timestamp, config_deleted, config_value \n" +
                "from platform_config where config_class = ? and config_key = ? and config_is_latest = ?";

        if (log.isDebugEnabled())
            log.debug("QUERY selectCurrentRow (platform_config): \n{}", query);

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setString(1, configClass);
            stmt.setString(2, configKey);
            stmt.setBoolean(3, true);

            try (var rs = stmt.executeQuery()) {

                if (!rs.next())
                    return null;

                var version = rs.getInt(1);
                var timestamp = rs.getTimestamp(2).toInstant();
                var deleted = rs.getBoolean(3);
                var value = rs.getBytes(4);

                return new CurrentRow(version, timestamp, deleted, value);
            }
        }
    }

    private void insertRow(Connection conn, PlatformConfigEntry entry, Instant timestamp, byte[] value) throws SQLException {

        var query =
                "insert into platform_config (\n" +
                "  config_class, config_key, config_version, config_timestamp, config_is_latest, config_deleted,\n" +
                "  meta_format, meta_version, config_value\n" +
                ")\n" +
                "values (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        if (log.isDebugEnabled())
            log.debug("QUERY insertRow (platform_config): \n{}", query);

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setString(1, entry.getConfigClass());
            stmt.setString(2, entry.getConfigKey());
            stmt.setInt(3, entry.getConfigVersion());
            stmt.setTimestamp(4, Timestamp.from(timestamp));
            stmt.setBoolean(5, true);
            stmt.setBoolean(6, entry.getConfigDeleted());
            stmt.setInt(7, MetadataFormat.PROTO.getNumber());
            stmt.setInt(8, MetadataVersion.CURRENT.getNumber());
            stmt.setBytes(9, value);

            stmt.executeUpdate();
        }
    }

    private void closeCurrentRow(Connection conn, String configClass, String configKey, Instant timestamp) throws SQLException {

        var query =
                "update platform_config set\n" +
                "  config_superseded = ?,\n" +
                "  config_is_latest = ?\n" +
                "where config_class = ? and config_key = ? and config_is_latest = ?";

        if (log.isDebugEnabled())
            log.debug("QUERY closeCurrentRow (platform_config): \n{}", query);

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setTimestamp(1, Timestamp.from(timestamp));
            stmt.setBoolean(2, false);
            stmt.setString(3, configClass);
            stmt.setString(4, configKey);
            stmt.setBoolean(5, true);

            stmt.executeUpdate();
        }
    }

    private PlatformConfigEntry buildEntry(
            String configClass, String configKey, int version, Instant timestamp, boolean deleted) {

        return PlatformConfigEntry.newBuilder()
                .setConfigClass(configClass)
                .setConfigKey(configKey)
                .setConfigVersion(version)
                .setConfigTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setConfigDeleted(deleted)
                .build();
    }

    private static class CurrentRow {

        final int version;
        final Instant timestamp;
        final boolean deleted;
        final byte[] value;

        CurrentRow(int version, Instant timestamp, boolean deleted, byte[] value) {
            this.version = version;
            this.timestamp = timestamp;
            this.deleted = deleted;
            this.value = value;
        }
    }
}
