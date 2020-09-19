/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.meta.dal.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

class JdbcTenantImpl {

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

    short getTenantId(String tenant) {

        Map<String, Short> currentTenantMap;

        synchronized (tenantLock) {
            currentTenantMap = this.tenantMap;
        }

        var tenantId = currentTenantMap.getOrDefault(tenant, null);

        if (tenantId == null)
            throw new RuntimeException();  // TODO: error handling, reload, expiry

        return tenantId;
    }
}
