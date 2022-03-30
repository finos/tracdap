/*
 * Copyright 2022 Accenture Global Solutions Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.validation.version;

import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;
import org.slf4j.LoggerFactory;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.VERSION)
public class StorageVersionValidator {

    private static final Descriptors.Descriptor STORAGE_DEFINITION;
    private static final Descriptors.FieldDescriptor SD_DATA_ITEMS;

    private static final Descriptors.Descriptor STORAGE_ITEM;
    private static final Descriptors.FieldDescriptor SIT_INCARNATIONS;

    private static final Descriptors.Descriptor STORAGE_INCARNATION;
    private static final Descriptors.FieldDescriptor SIN_COPIES;
    private static final Descriptors.FieldDescriptor SIN_INCARNATION_INDEX;
    private static final Descriptors.FieldDescriptor SIN_INCARNATION_TIMESTAMP;
    private static final Descriptors.FieldDescriptor SIN_INCARNATION_STATUS;

    private static final Descriptors.Descriptor STORAGE_COPY;
    private static final Descriptors.FieldDescriptor SC_STORAGE_KEY;
    private static final Descriptors.FieldDescriptor SC_STORAGE_PATH;
    private static final Descriptors.FieldDescriptor SC_STORAGE_FORMAT;
    private static final Descriptors.FieldDescriptor SC_COPY_STATUS;
    private static final Descriptors.FieldDescriptor SC_COPY_TIMESTAMP;

    static {

        STORAGE_DEFINITION = StorageDefinition.getDescriptor();
        SD_DATA_ITEMS = field(STORAGE_DEFINITION, StorageDefinition.DATAITEMS_FIELD_NUMBER);

        STORAGE_ITEM = StorageItem.getDescriptor();
        SIT_INCARNATIONS = field(STORAGE_ITEM, StorageItem.INCARNATIONS_FIELD_NUMBER);

        STORAGE_INCARNATION = StorageIncarnation.getDescriptor();
        SIN_COPIES = field(STORAGE_INCARNATION, StorageIncarnation.COPIES_FIELD_NUMBER);
        SIN_INCARNATION_INDEX = field(STORAGE_INCARNATION, StorageIncarnation.INCARNATIONINDEX_FIELD_NUMBER);
        SIN_INCARNATION_TIMESTAMP = field(STORAGE_INCARNATION, StorageIncarnation.INCARNATIONTIMESTAMP_FIELD_NUMBER);
        SIN_INCARNATION_STATUS = field(STORAGE_INCARNATION, StorageIncarnation.INCARNATIONSTATUS_FIELD_NUMBER);

        STORAGE_COPY = StorageCopy.getDescriptor();
        SC_STORAGE_KEY = field(STORAGE_COPY, StorageCopy.STORAGEKEY_FIELD_NUMBER);
        SC_STORAGE_PATH = field(STORAGE_COPY, StorageCopy.STORAGEPATH_FIELD_NUMBER);
        SC_STORAGE_FORMAT = field(STORAGE_COPY, StorageCopy.STORAGEFORMAT_FIELD_NUMBER);
        SC_COPY_STATUS = field(STORAGE_COPY, StorageCopy.COPYSTATUS_FIELD_NUMBER);
        SC_COPY_TIMESTAMP = field(STORAGE_COPY, StorageCopy.COPYTIMESTAMP_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext storage(StorageDefinition current, StorageDefinition prior, ValidationContext ctx) {

        ctx = ctx.pushMap(SD_DATA_ITEMS, StorageDefinition::getDataItemsMap);

        for (var priorItem : prior.getDataItemsMap().keySet()) {
            if (!current.containsDataItems(priorItem)) {
                var err = String.format("Data item [%s] has been removed", priorItem);
                ctx = ctx.error(err);
            }
        }

        for (var key : current.getDataItemsMap().keySet()) {

            ctx = ctx.pushMapValue(key)
                    .apply(StorageVersionValidator::item, StorageItem.class)
                    .pop();
        }

        return ctx.pop();
    }

    @Validator
    public static ValidationContext item(StorageItem current, StorageItem prior, ValidationContext ctx) {

        var log = LoggerFactory.getLogger(StorageVersionValidator.class);
        log.info(prior == null ? "New storage item" : "Validating storage item version");

        // If this is a new storage item, there is no versioning validation
        if (prior == null)
            return ctx;

        ctx = ctx.pushRepeated(SIT_INCARNATIONS);

        for (var currentIncarnation : current.getIncarnationsList()) {

            var priorIncarnation = prior.getIncarnationsList()
                    .stream()
                    .filter(pi -> pi.getIncarnationIndex() == currentIncarnation.getIncarnationIndex())
                    .findFirst();

            ctx = ctx.pushRepeatedItem(currentIncarnation, priorIncarnation.orElse(null))
                    .apply(StorageVersionValidator::incarnation, StorageIncarnation.class)
                    .pop();
        }

        return ctx.pop();
    }

    @Validator
    public static ValidationContext incarnation(StorageIncarnation current, StorageIncarnation prior, ValidationContext ctx) {

        // If this is a new incarnation, we need to check the incarnation index
        if (prior == null) {
            var priorItem = (StorageItem) ctx.prior().parentMsg();
            return ctx.apply(StorageVersionValidator::newIncarnationIndex, StorageIncarnation.class, priorItem);
        }

        ctx = ctx.push(SIN_INCARNATION_INDEX)
                .apply(CommonValidators::exactMatch)
                .pop();

        ctx = ctx.push(SIN_INCARNATION_TIMESTAMP)
                .apply(CommonValidators::exactMatch)
                .pop();

        ctx = ctx.push(SIN_INCARNATION_STATUS)
                .apply(StorageVersionValidator::incarnationStatus, IncarnationStatus.class)
                .pop();

        ctx = ctx.pushRepeated(SIN_COPIES);

        for (var currentCopy : current.getCopiesList()) {

            var priorCopy = prior.getCopiesList()
                    .stream()
                    .filter(pc ->
                            pc.getStorageKey().equals(currentCopy.getStorageKey()) &&
                            pc.getStoragePath().equals(currentCopy.getStoragePath()))
                    .findFirst();

            ctx = ctx.pushRepeatedItem(currentCopy, priorCopy.orElse(null))
                    .apply(StorageVersionValidator::copy, StorageCopy.class)
                    .pop();
        }

        return ctx.pop();
    }

    @Validator
    public static ValidationContext copy(StorageCopy current, StorageCopy prior, ValidationContext ctx) {

        // If this is a new copy, there is no versioning validation
        if (prior == null)
            return ctx;

        ctx = ctx.push(SC_STORAGE_KEY)
                .apply(CommonValidators::exactMatch)
                .pop();

        ctx = ctx.push(SC_STORAGE_PATH)
                .apply(CommonValidators::exactMatch)
                .pop();

        ctx = ctx.push(SC_COPY_TIMESTAMP)
                .apply(CommonValidators::exactMatch)
                .pop();

        ctx = ctx.push(SC_STORAGE_FORMAT)
                .apply(CommonValidators::exactMatch)
                .pop();

        ctx = ctx.push(SC_COPY_STATUS)
                .apply(StorageVersionValidator::copyStatus, CopyStatus.class)
                .pop();

        return ctx;
    }

    private static ValidationContext newIncarnationIndex(StorageIncarnation current, StorageItem priorItem, ValidationContext ctx) {

        var maxPriorIncarnation = priorItem.getIncarnationsList()
                .stream()
                .mapToInt(StorageIncarnation::getIncarnationIndex)
                .max();

        // Static validation for storage should ensure every item has at least one incarnation recorded
        if (maxPriorIncarnation.isEmpty())
            return ctx.error("Version validation failed because the prior version is invalid");

        if (current.getIncarnationIndex() <= maxPriorIncarnation.getAsInt()) {

            var err = String.format(
                    "New incarnation index must be greater than any previous incarnation (incarnation index = [%d], prior max = [%d])",
                    current.getIncarnationIndex(), maxPriorIncarnation.getAsInt());

            return ctx.error(err);
        }

        return ctx;
    }

    private static ValidationContext incarnationStatus(IncarnationStatus current, IncarnationStatus prior, ValidationContext ctx) {

        if (prior == IncarnationStatus.INCARNATION_EXPUNGED && current == IncarnationStatus.INCARNATION_AVAILABLE) {

            var err = String.format(
                    "Incarnation status cannot move from [%s] to [%s]",
                    prior, current);

            return ctx.error(err);
        }

        return ctx;
    }

    private static ValidationContext copyStatus(CopyStatus current, CopyStatus prior, ValidationContext ctx) {

        if (prior == CopyStatus.COPY_EXPUNGED && current == CopyStatus.COPY_AVAILABLE) {

            var err = String.format(
                    "Copy status cannot move from [%s] to [%s]",
                    prior, current);

            return ctx.error(err);
        }

        return ctx;
    }
}
