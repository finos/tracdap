package trac.svc.meta.dal.jdbc;

import trac.common.metadata.ObjectType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;


class JdbcReadBatchImpl {

    private AtomicInteger mappingStage;

    JdbcReadBatchImpl() {
        mappingStage = new AtomicInteger();
    }

    JdbcBaseDal.KeyedItems<ObjectType>
    readObjectTypeById(Connection conn, short tenantId, UUID[] objectId) throws SQLException {

        var selectQuery =
                "select object_pk, object_type\n" +
                "from object_id oid\n" +
                "join key_mapping km\n" +
                "  on oid.object_pk = km.pk\n" +
                "where km.mapping_stage = ?\n" +
                "order by km.ordering";

        var mappingStage = mapPkById(conn, tenantId, objectId);

        try (var stmt = conn.prepareStatement(selectQuery)) {

            stmt.setInt(1, mappingStage);

            try (var rs = stmt.executeQuery()) {

                var keys = new long[objectId.length];
                var types = new ObjectType[objectId.length];

                for (int i = 0; i < objectId.length; i++) {

                    if (!rs.next())
                        throw new JdbcException(JdbcErrorCode.NO_DATA.name(), JdbcErrorCode.NO_DATA);

                    var pk = rs.getLong(1);
                    var objectTypeCode = rs.getString(2);
                    var objectType = ObjectType.valueOf(objectTypeCode);

                    keys[i] = pk;
                    types[i] = objectType;
                }

                if (!rs.last())
                    throw new JdbcException(JdbcErrorCode.TOO_MANY_ROWS.name(), JdbcErrorCode.TOO_MANY_ROWS);

                return new JdbcBaseDal.KeyedItems<>(keys, types);
            }
        }
    }


    private int mapPkById(Connection conn, int tenantId, UUID[] ids) throws SQLException {

        var insertQuery =
                "insert into key_mapping (id_hi, id_lo, mapping_stage, ordering)\n" +
                "values (?, ?, ?, ?)";

        var mapQuery =
                "update key_mapping set\n" +
                "key_mapping.pk = (select object_pk from object_id oid\n" +
                "  where oid.tenant_id = ?\n" +
                "  and oid.object_id_hi = key_mapping.id_hi\n" +
                "  and oid.object_id_lo = key_mapping.id_lo)\n" +
                "where mapping_stage = ?";

        var mappingStage = nextMappingStage();

        try (var stmt = conn.prepareStatement(insertQuery)) {

            for (int i = 0; i < ids.length; i++) {

                stmt.clearParameters();

                stmt.setLong(1, ids[i].getMostSignificantBits());
                stmt.setLong(2, ids[i].getLeastSignificantBits());
                stmt.setInt(3, mappingStage);
                stmt.setInt(4, i);

                stmt.addBatch();
            }

            stmt.executeBatch();
        }

        try (var stmt = conn.prepareStatement(mapQuery))  {

            stmt.setInt(1, tenantId);
            stmt.setInt(2, mappingStage);

            stmt.execute();
        }

        return mappingStage;
    }

    private int nextMappingStage() {

        return mappingStage.incrementAndGet();
    }
}
