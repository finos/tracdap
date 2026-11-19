/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.svc.meta.test;

import org.finos.tracdap.common.exception.EConsistencyValidation;
import org.finos.tracdap.common.service.IMetadataWriteHook;
import org.finos.tracdap.common.service.ObjectWrite;
import org.finos.tracdap.common.service.TagWrite;
import org.finos.tracdap.metadata.ObjectType;

import com.google.protobuf.ByteString;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;


public class TestMetadataWriteHook implements IMetadataWriteHook {

    public static final ByteString REFUSED_DATA = ByteString.copyFromUtf8("test_refused_object");
    public static final String REFUSED_MESSAGE = "Refused by the test metadata write hook";

    public static final String REFUSED_TAG_ATTR = "test_refused_tag_attr";

    public static final List<ObjectWrite> WRITES = new CopyOnWriteArrayList<>();
    public static final List<TagWrite> TAG_WRITES = new CopyOnWriteArrayList<>();

    // Runs inside validateTag, before anything in the write is saved
    public static volatile Consumer<TagWrite> tagWriteCallback = null;

    @Override
    public void validateObject(ObjectWrite write) {

        WRITES.add(write);

        if (write.objectType() == ObjectType.CUSTOM &&
            write.definition().getCustom().getCustomData().equals(REFUSED_DATA)) {

            throw new EConsistencyValidation(REFUSED_MESSAGE);
        }
    }

    @Override
    public void validateTag(TagWrite write) {

        TAG_WRITES.add(write);

        var callback = tagWriteCallback;

        if (callback != null)
            callback.accept(write);

        if (write.tagUpdates().stream().anyMatch(update -> update.getAttrName().equals(REFUSED_TAG_ATTR)))
            throw new EConsistencyValidation(REFUSED_MESSAGE);
    }
}
