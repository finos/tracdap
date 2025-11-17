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

package org.finos.tracdap.common.storage.layouts;

import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.storage.IStorageLayout;
import org.finos.tracdap.common.storage.LayoutItem;
import org.finos.tracdap.metadata.DataDefinition;
import org.finos.tracdap.metadata.DataDelta;

import java.time.OffsetDateTime;
import java.util.Random;


public class DateSnapLayout implements IStorageLayout {

    // YEAR / DATE / TIME - OBJECT ID / PART / SNAP / DELTAS & CHUNKS
    private static final String DATA_STORAGE_PATH_TEMPLATE = "%04d/%02d-%02d-%02d/%s/%s/snap-%d/delta-%d-x%06x-chunk-%d.%s";

    // YEAR / DATE / TIME - OBJECT ID / VERSION / FILENAME
    private static final String FILE_STORAGE_PATH_TEMPLATE = "%04d/%02d-%02d-%02d/%s/version-%d-x%06x/%s";

    // In case no extension is available, use .data for data files
    private static final String FALLBACK_DATA_EXTENSION = "data";

    private final Random random;

    public DateSnapLayout() {
        this.random = new Random();
    }

    @Override
    public String newFilePath(LayoutItem layoutItem) {

        var timestamp = MetadataCodec.decodeDatetime(layoutItem.header().getObjectTimestamp());
        return fileStoragePath(layoutItem, timestamp);
    }

    @Override
    public String updateFilePath(LayoutItem layoutItem, LayoutItem priorLayoutItem) {

        var timestamp = MetadataCodec.decodeDatetime(layoutItem.header().getObjectTimestamp());
        return fileStoragePath(layoutItem, timestamp);
    }

    private String fileStoragePath(LayoutItem layoutItem, OffsetDateTime timestamp) {

        var date = timestamp.toLocalDate();

        var objectId = layoutItem.header().getObjectId();
        var objectVersion = layoutItem.header().getObjectVersion();
        var versionSuffix = random.nextInt(1 << 24);
        var fileName = layoutItem.file().getName();

        return String.format(FILE_STORAGE_PATH_TEMPLATE,
                date.getYear(), date.getYear(), date.getMonthValue(), date.getDayOfMonth(),
                objectId, objectVersion, versionSuffix, fileName);
    }

    @Override
    public String newDataPath(LayoutItem layoutItem) {

        var timestamp = MetadataCodec.decodeDatetime(layoutItem.header().getObjectTimestamp());

        return dataStoragePath(layoutItem, timestamp);
    }

    @Override
    public String updateDataPath(LayoutItem layoutItem, LayoutItem priorLayoutItem) {

        // New snaps use the default logic, new parts / snaps go in the current date folder
        if (layoutItem.delta() == 0)
            return newDataPath(layoutItem);

        var delta0 = getDelta0(priorLayoutItem.data(), layoutItem);

        // This should never happen, prior metadata is used to construct updates
        if (delta0 == null) {

            var message = String.format(
                    "Prior metadata is not valid for dataset %s, %s, snap %d, delta %d",
                    layoutItem.header().getObjectId(), layoutItem.part(), layoutItem.snap(), layoutItem.delta());

            throw new ETracInternal(message);
        }

        // For TRAC 0.10 and later, data deltas are recorded with a timestamp
        // This timestamp is not affected by reincarnation and allows the original path to be recreated

        if (delta0.hasDeltaTimestamp() && !delta0.getDeltaTimestamp().getIsoDatetime().isEmpty()) {
            var timestamp = MetadataCodec.decodeDatetime(delta0.getDeltaTimestamp());
            return dataStoragePath(layoutItem, timestamp);
        }

        // For TRAC 0.9 and earlier, fall back on the storage incarnation timestamp
        // This means the path will change on reincarnation, to reflect the incarnation date

        if (!priorLayoutItem.storage().containsDataItems(delta0.getDataItem())) {

            var message = String.format(
                    "Prior data is not available for dataset %s, %s, snap %d, delta %d",
                    layoutItem.header().getObjectId(), layoutItem.part(), layoutItem.snap(), layoutItem.delta());

            throw new ETracInternal(message);
        }

        var priorStorageItem = priorLayoutItem.storage().getDataItemsOrThrow(delta0.getDataItem());
        var priorIncarnation = priorStorageItem.getIncarnations(priorStorageItem.getIncarnationsCount() - 1);
        var timestamp = MetadataCodec.decodeDatetime(priorIncarnation.getIncarnationTimestamp());

        return dataStoragePath(layoutItem, timestamp);
    }

    private String dataStoragePath(LayoutItem layoutItem, OffsetDateTime timestamp) {

        var date = timestamp.toLocalDate();

        var objectId = layoutItem.header().getObjectId();
        var part = layoutItem.part().getOpaqueKey();
        var snap = layoutItem.snap();
        var delta = layoutItem.delta();
        var deltaSuffix = random.nextInt(1 << 24);
        var chunk = 0;
        var extension = layoutItem.extension();

        if (extension == null || extension.isEmpty())
            extension = FALLBACK_DATA_EXTENSION;

        return String.format(DATA_STORAGE_PATH_TEMPLATE,
                date.getYear(), date.getYear(), date.getMonthValue(), date.getDayOfMonth(),
                objectId, part, snap, delta, deltaSuffix, chunk, extension);
    }

    private DataDelta getDelta0(DataDefinition priorData, LayoutItem layoutItem) {

        var part = layoutItem.part().getOpaqueKey();

        if (!priorData.containsParts(part))
            return null;

        var priorPart = priorData.getPartsOrThrow(part);
        var priorSnap = priorPart.getSnap();

        if (priorSnap.getSnapIndex() != layoutItem.snap())
            return null;

        if (priorSnap.getDeltasCount() == 0)
            return null;

        return priorSnap.getDeltas(0);
    }
}
