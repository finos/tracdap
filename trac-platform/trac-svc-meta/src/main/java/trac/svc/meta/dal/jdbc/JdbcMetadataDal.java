package trac.svc.meta.dal.jdbc;

import com.google.protobuf.MessageLite;
import trac.common.metadata.*;
import trac.svc.meta.dal.IMetadataDal;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;


public class JdbcMetadataDal extends JdbcBaseDal implements IMetadataDal {

    private final JdbcTenantImpl tenants;
    private final JdbcReadImpl readSingle;
    private final JdbcReadBatchImpl readBatch;
    private final JdbcWriteBatchImpl writeBatch;


    public JdbcMetadataDal(JdbcDialect dialect, DataSource dataSource, Executor executor) {

        super(dialect, dataSource, executor);

        tenants = new JdbcTenantImpl();
        readSingle = new JdbcReadImpl();
        readBatch = new JdbcReadBatchImpl();
        writeBatch = new JdbcWriteBatchImpl();
    }

    public void startup() {

        try {
            executeDirect(tenants::loadTenantMap);
        }
        catch (SQLException e) {
            // TODO: StartupException
            throw new RuntimeException();
        }
    }

    public void shutdown() {

        // Noop
    }

    @Override
    public CompletableFuture<Void> saveNewObject(String tenant, Tag tag) {

        var parts = separateParts(tag);
        return saveNewObjects(tenant, parts);
    }

    @Override
    public CompletableFuture<Void> saveNewObjects(String tenant, List<Tag> tags) {

        var parts = separateParts(tags);
        return saveNewObjects(tenant, parts);
    }

    private CompletableFuture<Void> saveNewObjects(String tenant, ObjectParts parts) {

        return wrapTransaction(conn -> {

            var tenantId = tenants.getTenantId(tenant);

            long[] objectPk = writeBatch.writeObjectId(conn, tenantId, parts.objectType, parts.objectId);
            long[] defPk = writeBatch.writeObjectDefinition(conn, tenantId, objectPk, parts.version, parts.definition);
            long[] tagPk = writeBatch.writeTagRecord(conn, tenantId, defPk, parts.tagVersion, parts.tag);
            writeBatch.writeTagAttrs(conn, tenantId, tagPk, parts.tag);
        },
        (error, code) ->  JdbcError.handleDuplicateObjectId(error, code, parts));
    }

    @Override
    public CompletableFuture<Void> saveNewVersion(String tenant, Tag tag) {

        var parts = separateParts(tag);
        return saveNewVersions(tenant, parts);
    }

    @Override
    public CompletableFuture<Void> saveNewVersions(String tenant, List<Tag> tags) {

        var parts = separateParts(tags);
        return saveNewVersions(tenant, parts);
    }

    private CompletableFuture<Void> saveNewVersions(String tenant, ObjectParts parts) {

        return wrapTransaction(conn -> {

            prepareMappingTable(conn);

            var tenantId = tenants.getTenantId(tenant);

            var objectType = readBatch.readObjectTypeById(conn, tenantId, parts.objectId);
            long[] defPk = writeBatch.writeObjectDefinition(conn, tenantId, objectType.keys, parts.version, parts.definition);
            long[] tagPk = writeBatch.writeTagRecord(conn, tenantId, defPk, parts.tagVersion, parts.tag);
            writeBatch.writeTagAttrs(conn, tenantId, tagPk, parts.tag);
        },
        (error, code) -> JdbcError.handleMissingItem(error, code, parts),
        (error, code) ->  JdbcError.handleDuplicateObjectId(error, code, parts));
    }

    @Override
    public CompletableFuture<Void> saveNewTag(String tenant, Tag tag) {
        return null;
    }

    @Override
    public CompletableFuture<Void> saveNewTags(String tenant, List<Tag> tags) {
        return null;
    }

    @Override
    public CompletableFuture<Void> preallocateObjectId(String tenant, ObjectType objectType, UUID objectId) {
        return null;
    }

    @Override
    public CompletableFuture<Void> preallocateObjectIds(String tenant, ObjectType objectType, List<UUID> objectIds) {
        return null;
    }

    @Override
    public CompletableFuture<Void> savePreallocatedObject(String tenant, Tag tag) {
        return null;
    }

    @Override
    public CompletableFuture<Void> savePreallocatedObjects(String tenant, List<Tag> tags) {
        return null;
    }

    @Override public CompletableFuture<Tag>
    loadTag(String tenant, UUID objectId, int objectVersion, int tagVersion) {

        var parts = assembleParts(objectId, objectVersion, tagVersion);

        return wrapTransaction(conn -> {

            var tenantId = tenants.getTenantId(tenant);

            var objectType = readSingle.readObjectTypeById(conn, tenantId, objectId);
            var definition = readSingle.readDefinitionByVersion(conn, tenantId, objectType.item, objectType.key, objectVersion);
            var tagStub = readSingle.readTagRecordByVersion(conn, tenantId, definition.key, tagVersion);
            var tagAttrs = readSingle.readTagAttrs(conn, tenantId, tagStub.key);

            var header = ObjectHeader.newBuilder()
                    .setObjectType(objectType.item)
                    .setId(MetadataCodec.encode(objectId))
                    .setVersion(objectVersion)
                    .build();

            return MetadataCodec.tagForDefinition(tagStub.item, objectType.item, definition.item)
                    .setHeader(header)
                    .putAllAttr(tagAttrs)
                    .build();
        },
        (error, code) -> JdbcError.handleMissingItem(error, code, parts));
    }

    @Override public CompletableFuture<List<Tag>>
    loadTags(String tenant, List<UUID> objectIds, List<Integer> objectVersions, List<Integer> tagVersions) {
        return null;
    }

    @Override
    public CompletableFuture<Tag> loadLatestTag(String tenant, UUID objectId, int objectVersion) {
        return null;
    }

    @Override
    public CompletableFuture<Tag> loadLatestVersion(String tenant, UUID objectId) {
        return null;
    }

    @Override
    public CompletableFuture<Tag> loadLatestObject(String tenant, UUID objectId, int objectVersion) {
        return null;
    }


    // -----------------------------------------------------------------------------------------------------------------
    // OBJECT PARTS
    // -----------------------------------------------------------------------------------------------------------------

    static class ObjectParts {

        ObjectType[] objectType;
        UUID[] objectId;
        int[] version;
        int[] tagVersion;

        Tag[] tag;
        MessageLite[] definition;
    }


    private ObjectParts separateParts(Tag tag) {

        var parts = new ObjectParts();
        parts.objectType = new ObjectType[] {tag.getHeader().getObjectType()};
        parts.objectId = new UUID[] {MetadataCodec.decode(tag.getHeader().getId())};
        parts.version = new int[] {tag.getHeader().getVersion()};
        parts.tagVersion = new int[] {tag.getTagVersion()};

        parts.tag = new Tag[] {tag};
        parts.definition = new MessageLite[] {MetadataCodec.definitionForTag(tag)};

        return parts;
    }

    private ObjectParts separateParts(List<Tag> tags) {

        var objectHeaders = tags.stream().map(Tag::getHeader).toArray(ObjectHeader[]::new);

        var parts = new ObjectParts();
        parts.objectType = Arrays.stream(objectHeaders).map(ObjectHeader::getObjectType).toArray(ObjectType[]::new);
        parts.objectId = Arrays.stream(objectHeaders).map(ObjectHeader::getId).map(MetadataCodec::decode).toArray(UUID[]::new);
        parts.version = Arrays.stream(objectHeaders).mapToInt(ObjectHeader::getVersion).toArray();
        parts.tagVersion = tags.stream().mapToInt(Tag::getTagVersion).toArray();

        parts.tag = tags.toArray(Tag[]::new);
        parts.definition = tags.stream().map(MetadataCodec::definitionForTag).toArray(MessageLite[]::new);

        return parts;
    }

    private ObjectParts assembleParts(UUID id, int version, int tagVersion) {

        var parts = new ObjectParts();
        parts.objectId = new UUID[] {id};
        parts.version = new int[] {version};
        parts.tagVersion = new int[] {tagVersion};

        return parts;
    }

    private ObjectParts assembleParts(List<UUID> ids, List<Integer> versions, List<Integer> tagVersions) {

        var parts = new ObjectParts();

        return parts;
    }
}
