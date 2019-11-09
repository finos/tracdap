package trac.svc.meta.dal.jdbc;

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
            throw new RuntimeException();

        return tenantId;
    }
}
