package trac.svc.meta.dal.jdbc;

import trac.common.metadata.ObjectType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;


class JdbcReadBatchImpl {

    JdbcBaseDal.KeyedItems<ObjectType>
    readObjectTypeById(Connection conn, short tenantId, UUID[] objectId) throws SQLException {

        return null;
    }
}
