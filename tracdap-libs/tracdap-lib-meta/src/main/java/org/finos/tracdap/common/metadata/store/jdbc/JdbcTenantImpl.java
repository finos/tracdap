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

import org.finos.tracdap.common.db.JdbcErrorCode;
import org.finos.tracdap.common.db.JdbcException;
import org.finos.tracdap.common.exception.ETenantNotFound;
import org.finos.tracdap.metadata.TenantInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class JdbcTenantImpl {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private Map<String, Short> tenantMap;
    private final Object tenantLock;

    JdbcTenantImpl() {
        tenantMap = new HashMap<>();
        tenantLock = new Object();
    }

    void loadTenantMap(Connection conn) throws SQLException {

        var query = "select tenant_id, tenant_code from tenant";

        try (var stmt = conn.prepareStatement(query); var rs = stmt.executeQuery()) {

            var newTenantMap = new HashMap<String, Short>();

            while (rs.next()) {
                var tenantId = rs.getShort(1);
                var tenantCode = rs.getString(2);
                newTenantMap.put(tenantCode, tenantId);
            }

            synchronized (tenantLock) {
                tenantMap = newTenantMap;
            }
        }
    }

    short getTenantId(Connection conn, String tenant) throws SQLException {

        Map<String, Short> currentTenantMap;

        synchronized (tenantLock) {
            currentTenantMap = this.tenantMap;
        }

        var tenantId = currentTenantMap.getOrDefault(tenant, null);

        if (tenantId == null) {
            loadTenantMap(conn);
            synchronized (tenantLock) {
                currentTenantMap = this.tenantMap;
            }
            tenantId = currentTenantMap.getOrDefault(tenant, null);
        }

        if (tenantId == null) {
            var message = String.format("Unknown tenant [%s]", tenant);
            log.error(message);
            throw new ETenantNotFound(message);
        }

        return tenantId;
    }

    List<TenantInfo>
    listTenants(Connection conn) throws SQLException {

        var query = "select tenant_code, description from tenant";

        try (var stmt = conn.prepareStatement(query); var rs = stmt.executeQuery()) {

            var tenants = new ArrayList<TenantInfo>();

            while (rs.next()) {

                var tenantCode = rs.getString(1);
                var description = rs.getString(2);

                var tenantInfo = TenantInfo.newBuilder()
                        .setTenantCode(tenantCode);

                if (!rs.wasNull())
                    tenantInfo.setDescription(description);

                tenants.add(tenantInfo.build());
            }

            return tenants;
        }
    }

    void activateTenant(Connection conn, String tenantCode, String description) throws SQLException {

        var findNextId = "select max(tenant_id) from tenant";
        short nextId;

        try (var stmt = conn.prepareStatement(findNextId); var rs = stmt.executeQuery()) {

            if (rs.next()) {

                nextId = rs.getShort(1);

                if (rs.wasNull())
                    nextId = 1;
                else
                    nextId++;
            }
            else
                nextId = 1;
        }

        var query = "insert into tenant (tenant_id, tenant_code, description) values (?, ?, ?)";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setShort(1, nextId);
            stmt.setString(2, tenantCode);

            if (description != null)
                stmt.setString(3, description);
            else
                stmt.setNull(3, java.sql.Types.VARCHAR);

            stmt.executeUpdate();
        }
    }

    void updateTenant(Connection conn, short tenantId, String description) throws SQLException {

        var query = "update tenant set description = ? where tenant_id = ?";

        try (var stmt = conn.prepareStatement(query)) {

            stmt.setString(1, description);
            stmt.setShort(2, tenantId);

            var rows = stmt.executeUpdate();

            if (rows < 1)
                throw new JdbcException(JdbcErrorCode.NO_DATA);

            if (rows > 1)
                throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS);
        }
    }
}
