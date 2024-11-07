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

package org.finos.tracdap.common.validation.static_;

import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.validation.ValidationConstants;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class StorageValidator {

    private static final Descriptors.Descriptor STORAGE_DEFINITION;
    private static final Descriptors.FieldDescriptor SD_DATA_ITEMS;

    private static final Descriptors.Descriptor STORAGE_ITEM;
    private static final Descriptors.FieldDescriptor SIT_INCARNATIONS;

    private static final Descriptors.Descriptor STORAGE_INCARNATION;
    private static final Descriptors.FieldDescriptor SIC_COPIES;
    private static final Descriptors.FieldDescriptor SIC_INCARNATION_INDEX;
    private static final Descriptors.FieldDescriptor SIC_INCARNATION_TIMESTAMP;
    private static final Descriptors.FieldDescriptor SIC_INCARNATION_STATUS;

    private static final Descriptors.Descriptor STORAGE_COPY;
    private static final Descriptors.FieldDescriptor SD_STORAGE_KEY;
    private static final Descriptors.FieldDescriptor SD_STORAGE_PATH;
    private static final Descriptors.FieldDescriptor SD_STORAGE_FORMAT;
    private static final Descriptors.FieldDescriptor SD_COPY_STATUS;
    private static final Descriptors.FieldDescriptor SD_COPY_TIMESTAMP;

    static {

        STORAGE_DEFINITION = StorageDefinition.getDescriptor();
        SD_DATA_ITEMS = field(STORAGE_DEFINITION, StorageDefinition.DATAITEMS_FIELD_NUMBER);

        STORAGE_ITEM = StorageItem.getDescriptor();
        SIT_INCARNATIONS = field(STORAGE_ITEM, StorageItem.INCARNATIONS_FIELD_NUMBER);

        STORAGE_INCARNATION = StorageIncarnation.getDescriptor();
        SIC_COPIES = field(STORAGE_INCARNATION, StorageIncarnation.COPIES_FIELD_NUMBER);
        SIC_INCARNATION_INDEX = field(STORAGE_INCARNATION, StorageIncarnation.INCARNATIONINDEX_FIELD_NUMBER);
        SIC_INCARNATION_TIMESTAMP = field(STORAGE_INCARNATION, StorageIncarnation.INCARNATIONTIMESTAMP_FIELD_NUMBER);
        SIC_INCARNATION_STATUS = field(STORAGE_INCARNATION, StorageIncarnation.INCARNATIONSTATUS_FIELD_NUMBER);

        STORAGE_COPY = StorageCopy.getDescriptor();
        SD_STORAGE_KEY = field(STORAGE_COPY, StorageCopy.STORAGEKEY_FIELD_NUMBER);
        SD_STORAGE_PATH = field(STORAGE_COPY, StorageCopy.STORAGEPATH_FIELD_NUMBER);
        SD_STORAGE_FORMAT = field(STORAGE_COPY, StorageCopy.STORAGEFORMAT_FIELD_NUMBER);
        SD_COPY_STATUS = field(STORAGE_COPY, StorageCopy.COPYSTATUS_FIELD_NUMBER);
        SD_COPY_TIMESTAMP = field(STORAGE_COPY, StorageCopy.COPYTIMESTAMP_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext storageDefinition(StorageDefinition msg, ValidationContext ctx) {

        ctx = ctx.pushMap(SD_DATA_ITEMS)
                .apply(CommonValidators::mapNotEmpty)
                .applyMapKeys(StorageValidator::dataItemKey)
                .applyMapValues(StorageValidator::storageItem, StorageItem.class)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext storageItem(StorageItem msg, ValidationContext ctx) {

        ctx = ctx.pushRepeated(SIT_INCARNATIONS)
                .apply(CommonValidators::listNotEmpty)
                .applyRepeated(StorageValidator::storageIncarnation, StorageIncarnation.class)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext storageIncarnation(StorageIncarnation msg, ValidationContext ctx) {

        ctx = ctx.push(SIC_INCARNATION_INDEX)
                .apply(CommonValidators::optional)  // zero-based index must be optional
                .apply(CommonValidators::notNegative, Integer.class)
                .pop();

        ctx = ctx.push(SIC_INCARNATION_TIMESTAMP)
                .apply(CommonValidators::required)
                .apply(TypeSystemValidator::datetimeValue, DatetimeValue.class)
                .apply(TypeSystemValidator::notInTheFuture, DatetimeValue.class)
                .pop();

        ctx = ctx.push(SIC_INCARNATION_STATUS)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, IncarnationStatus.class)
                .pop();

        ctx = ctx.pushRepeated(SIC_COPIES)
                .apply(CommonValidators::listNotEmpty)
                .applyRepeated(StorageValidator::storageCopy, StorageCopy.class)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext storageCopy(StorageCopy msg, ValidationContext ctx) {

        ctx = ctx.push(SD_STORAGE_KEY)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(SD_STORAGE_PATH)
                .apply(CommonValidators::required)
                .apply(CommonValidators::relativePath)
                .pop();

        ctx = ctx.push(SD_STORAGE_FORMAT)
                .apply(CommonValidators::required)
                .apply(StorageValidator::storageFormat)
                .pop();

        ctx = ctx.push(SD_COPY_STATUS)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, CopyStatus.class)
                .pop();

        ctx = ctx.push(SD_COPY_TIMESTAMP)
                .apply(CommonValidators::required)
                .apply(TypeSystemValidator::datetimeValue, DatetimeValue.class)
                .apply(TypeSystemValidator::notInTheFuture, DatetimeValue.class)
                .pop();

        return ctx;
    }

    public static ValidationContext dataItemKey(String dataItem, ValidationContext ctx) {

        var dataItemMatcher = ValidationConstants.DATA_ITEM_KEY.matcher(dataItem);

        if (!dataItemMatcher.matches()) {

            var err = String.format("Invalid data item key [%s]", dataItem);
            return ctx.error(err);
        }

        return ctx;
    }

    private static ValidationContext storageFormat(String storageFormat, ValidationContext ctx) {

        var mimeTypeMatcher = ValidationConstants.MIME_TYPE.matcher(storageFormat);
        var identifierMatcher = MetadataConstants.VALID_IDENTIFIER.matcher(storageFormat);

        if (!mimeTypeMatcher.matches() && !identifierMatcher.matches()) {

            var err = String.format("Invalid [%s], expected an identifier or a mime type, got [%s]",
                    ctx.fieldName(), storageFormat);

            return ctx.error(err);
        }

        return ctx;
    }
}
