package trac.svc.meta.dal;

import trac.common.metadata.*;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;


public interface IMetadataDal {

    CompletableFuture<Void> saveNewObject(String tenant, Tag tag);

    CompletableFuture<Void> saveNewObjects(String tenant, List<Tag> tags);

    CompletableFuture<Void> saveNewVersion(String tenant, Tag tag);

    CompletableFuture<Void> saveNewVersions(String tenant, List<Tag> tags);

    CompletableFuture<Void> saveNewTag(String tenant, Tag tag);

    CompletableFuture<Void> saveNewTags(String tenant, List<Tag> tags);

    CompletableFuture<Void> preallocateObjectId(String tenant, ObjectType objectType, UUID objectId);

    CompletableFuture<Void> preallocateObjectIds(String tenant, List<ObjectType> objectTypes, List<UUID> objectIds);

    CompletableFuture<Void> savePreallocatedObject(String tenant, Tag tag);

    CompletableFuture<Void> savePreallocatedObjects(String tenant, List<Tag> tags);


    CompletableFuture<Tag>
    loadTag(String tenant, ObjectType objectType, UUID objectId, int objectVersion, int tagVersion);

    CompletableFuture<List<Tag>>
    loadTags(String tenant, List<ObjectType> objectType, List<UUID> objectId, List<Integer> objectVersion, List<Integer> tagVersion);

    CompletableFuture<Tag>
    loadLatestTag(String tenant, ObjectType objectType, UUID objectId, int objectVersion);

    CompletableFuture<List<Tag>>
    loadLatestTags(String tenant, List<ObjectType> objectType, List<UUID> objectId, List<Integer> objectVersion);

    CompletableFuture<Tag>
    loadLatestVersion(String tenant, ObjectType objectType, UUID objectId);

    CompletableFuture<List<Tag>>
    loadLatestVersions(String tenant, List<ObjectType> objectType, List<UUID> objectId);

}
