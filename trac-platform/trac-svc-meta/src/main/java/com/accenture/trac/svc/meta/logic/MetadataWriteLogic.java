package com.accenture.trac.svc.meta.logic;

import com.accenture.trac.common.metadata.ObjectHeader;
import com.accenture.trac.common.metadata.ObjectType;
import com.accenture.trac.common.metadata.Tag;
import com.accenture.trac.svc.meta.dal.IMetadataDal;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.accenture.trac.common.metadata.MetadataCodec.encode;


public class MetadataWriteLogic {

    private final IMetadataDal dal;

    public MetadataWriteLogic(IMetadataDal dal) {
        this.dal = dal;
    }

    public CompletableFuture<UUID> saveNewObject(String tenant, ObjectType objectType, Tag tag) {

        var objectId = UUID.randomUUID();
        var objectVersion = 1;
        var tagVersion = 1;

        var header = ObjectHeader.newBuilder()
                .setObjectType(objectType)
                .setId(encode(objectId))
                .setVersion(objectVersion);

        var tagToSave = tag.toBuilder()
                .setHeader(header)
                .setTagVersion(tagVersion)
                .build();

        return dal.saveNewObject(tenant, tagToSave)
                .thenApply(_ok -> objectId);
    }
}
