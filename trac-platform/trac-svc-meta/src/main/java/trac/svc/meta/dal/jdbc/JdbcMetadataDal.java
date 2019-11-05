package trac.svc.meta.dal.jdbc;

import trac.common.metadata.ObjectType;
import trac.common.metadata.Tag;
import trac.svc.meta.dal.IMetadataDal;

import javax.sql.DataSource;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;


public class JdbcMetadataDal extends JdbcBaseDal implements IMetadataDal {

    public JdbcMetadataDal(DataSource dataSource, Executor executor) {
        super(dataSource, executor);
    }

    @Override
    public CompletableFuture<Void> saveNewObject(String tenant, Tag tag) {

        return wrapTransaction(conn -> {

        });
    }

    @Override
    public CompletableFuture<Void> saveNewObjects(String tenant, List<Tag> tags) {
        return null;
    }

    @Override
    public CompletableFuture<Void> saveNewVersion(String tenant, Tag tag) {
        return null;
    }

    @Override
    public CompletableFuture<Void> saveNewVersions(String tenant, List<Tag> tags) {
        return null;
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

    @Override
    public CompletableFuture<Tag> loadTag(String tenant, UUID objectId, int objectVersion, int tagVersion) {
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
    public CompletableFuture<List<Tag>> loadTags(String tenant, List<UUID> objectIds, List<Integer> objectVersions, List<Integer> tagVersions) {
        return null;
    }

    @Override
    public CompletableFuture<Tag> loadLatestObject(String tenant, UUID objectId, int objectVersion) {
        return null;
    }
}
