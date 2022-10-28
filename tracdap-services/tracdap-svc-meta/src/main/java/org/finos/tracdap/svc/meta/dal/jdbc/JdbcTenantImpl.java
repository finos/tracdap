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

import org.finos.tracdap.api.TenantInfo;
import org.finos.tracdap.common.exception.ETenantNotFound;
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

    List<TenantInfo>
    listTenants(Connection conn) throws SQLException {

        loadTenantMap(conn);

        Map<String, Short> currentTenantMap;

        synchronized (tenantLock) {
            currentTenantMap = this.tenantMap;
        }

        var tenantInfos = new ArrayList<TenantInfo>();

        for (var tenantCode : currentTenantMap.keySet()) {

            var tenantInfo = TenantInfo.newBuilder()
                    .setTenantName(tenantCode)
                    .build();

            tenantInfos.add(tenantInfo);
        }

        return tenantInfos;
    }

    short getTenantId(Connection conn, String tenant) throws SQLException {

        Map<String, Short> currentTenantMap;

        synchronized (tenantLock) {
            currentTenantMap = this.tenantMap;
        }

        var tenantId = currentTenantMap.getOrDefault(tenant, null);

        if (tenantId == null) {
            loadTenantMap(conn);
            tenantId = currentTenantMap.getOrDefault(tenant, null);
        }

        if (tenantId == null) {
            var message = String.format("Unknown tenant [%s]", tenant);
            log.error(message);
            throw new ETenantNotFound(message);
        }

        return tenantId;
    }
}
