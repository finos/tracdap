package trac.svc.meta.logic;

import trac.common.metadata.ObjectType;
import trac.common.metadata.Tag;
import trac.svc.meta.dal.IMetadataDal;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;


public class MetadataReadLogic {

    private final IMetadataDal dal;

    public MetadataReadLogic(IMetadataDal dal) {
        this.dal = dal;
    }

    public CompletableFuture<Tag> readTag(
            String tenant, ObjectType objectType,
            UUID objectId, int objectVersion, int tagVersion) {

        return dal.loadTag(tenant, objectType, objectId, objectVersion, tagVersion);
    }
}
