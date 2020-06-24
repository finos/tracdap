package com.accenture.trac.svc.meta.logic;

import com.accenture.trac.svc.meta.dal.IMetadataDal;
import com.accenture.trac.common.metadata.ObjectType;
import com.accenture.trac.common.metadata.Tag;

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
