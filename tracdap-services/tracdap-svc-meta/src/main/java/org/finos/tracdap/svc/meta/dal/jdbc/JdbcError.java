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

package org.finos.tracdap.svc.meta.dal.jdbc;

import org.finos.tracdap.common.exception.ETracInternal;
import org.finos.tracdap.metadata.TagSelector;
import org.finos.tracdap.svc.meta.dal.jdbc.dialects.IDialect;
import org.finos.tracdap.common.exception.EMetadataDuplicate;
import org.finos.tracdap.common.exception.EMetadataNotFound;
import org.finos.tracdap.common.exception.EMetadataWrongType;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;

class JdbcError {

    static final String MISSING_ITEM = "Metadata not found for {0} [{1}] version {2} tag {3}";
    static final String MISSING_ITEM_MULTIPLE = "Metadata not found for one or more items";

    static final String WRONG_OBJECT_TYPE = "Metadata does not match the expected type for {0} [{1}]";
    static final String WRONG_OBJECT_TYPE_MULTIPLE = "Metadata does not match the expected type for one or more objects";

    static final String DUPLICATE_OBJECT_ID = "Duplicate object id {0}";

    static final String OBJECT_VERSION_SUPERSEDED = "Metadata has been superseded for {0} [{1}] version {2}";
    static final String OBJECT_VERSION_SUPERSEDED_MULTIPLE = "Metadata has been superseded for one or more objects";

    static final String TAG_VERSION_SUPERSEDED = "Metadata has been superseded for {0} [{1}] version {2} tag {3}";
    static final String TAG_VERSION_SUPERSEDED_MULTIPLE = "Metadata has been superseded for one or more tags";



    static final String LOAD_ONE_MISSING_ITEM = "Metadata item does not exist {0}";
    static final String LOAD_ONE_WRONG_OBJECT_TYPE = "Metadata item has the wrong type";

    static final String LOAD_BATCH_MISSING_ITEM = "One or more metadata item does not exist {0}";
    static final String LOAD_BATCH_WRONG_OBJECT_TYPE = "One or more metadata item has the wrong type";

    static final String UNRECOGNISED_ERROR_CODE = "Unrecognised SQL Error code: {0}, sqlstate = {1}, error code = {2}";
    static final String UNHANDLED_ERROR = "Unhandled SQL Error code: {0}";




    static void missingItem(SQLException error, JdbcErrorCode code, JdbcMetadataDal.ObjectParts parts) {

        if (code == JdbcErrorCode.NO_DATA) {

            if (parts.objectId.length == 1) {

                var message = MessageFormat.format(MISSING_ITEM,
                        parts.objectType[0],
                        parts.objectId[0],
                        parts.objectVersion[0],
                        parts.tagVersion[0]);

                throw new EMetadataDuplicate(message, error);
            }
            else {

                throw new EMetadataDuplicate(MISSING_ITEM_MULTIPLE, error);
            }
        }
    }

    static void wrongObjectType(SQLException error, JdbcErrorCode code, JdbcMetadataDal.ObjectParts parts) {

        if (code == JdbcErrorCode.WRONG_OBJECT_TYPE) {

            if (parts.objectId.length == 1) {

                var message = MessageFormat.format(WRONG_OBJECT_TYPE,
                        parts.objectType[0],
                        parts.objectId[0]);

                throw new EMetadataWrongType(message, error);
            }
            else {

                throw new EMetadataWrongType(WRONG_OBJECT_TYPE_MULTIPLE, error);
            }
        }
    }


    static void duplicateObjectId(SQLException error, JdbcErrorCode code, JdbcMetadataDal.ObjectParts parts) {

        if (code == JdbcErrorCode.INSERT_DUPLICATE) {
            if (parts.objectId.length == 1) {
                var message = MessageFormat.format(DUPLICATE_OBJECT_ID, parts.objectId[0]);
                throw new EMetadataDuplicate(message, error);
            }
        }
    }

    static void objectVersionSuperseded(SQLException error, JdbcErrorCode code, JdbcMetadataDal.ObjectParts parts) {

        if (code == JdbcErrorCode.INSERT_DUPLICATE) {

            if (parts.objectId.length == 1) {

                var message = MessageFormat.format(OBJECT_VERSION_SUPERSEDED,
                        parts.objectType[0],
                        parts.objectId[0],
                        parts.objectVersion[0]);

                throw new EMetadataDuplicate(message, error);
            }
            else {

                throw new EMetadataDuplicate(OBJECT_VERSION_SUPERSEDED_MULTIPLE, error);
            }
        }
    }

    static void tagVersionSuperseded(SQLException error, JdbcErrorCode code, JdbcMetadataDal.ObjectParts parts) {

        if (code == JdbcErrorCode.INSERT_DUPLICATE) {

            if (parts.objectId.length == 1) {

                var message = MessageFormat.format(TAG_VERSION_SUPERSEDED,
                        parts.objectType[0],
                        parts.objectId[0],
                        parts.objectVersion[0],
                        parts.tagVersion[0]);

                throw new EMetadataDuplicate(message, error);
            }
            else {

                throw new EMetadataDuplicate(TAG_VERSION_SUPERSEDED_MULTIPLE, error);
            }
        }
    }

    static void loadOne_missingItem(SQLException error, JdbcErrorCode code, TagSelector selector) {

        if (code == JdbcErrorCode.NO_DATA) {
            var message = MessageFormat.format(LOAD_ONE_MISSING_ITEM, "");
            throw new EMetadataNotFound(message, error);
        }
    }

    static void loadOne_WrongObjectType(SQLException error, JdbcErrorCode code, TagSelector selector) {

        if (code == JdbcErrorCode.WRONG_OBJECT_TYPE) {
            var message = MessageFormat.format(LOAD_ONE_WRONG_OBJECT_TYPE, "");
            throw new EMetadataWrongType(message, error);
        }
    }

    static void loadBatch_missingItem(SQLException error, JdbcErrorCode code, List<TagSelector> selectors) {

        if (code == JdbcErrorCode.NO_DATA) {
            var message = MessageFormat.format(LOAD_BATCH_MISSING_ITEM, "");
            throw new EMetadataNotFound(message, error);
        }
    }

    static void loadBatch_WrongObjectType(SQLException error, JdbcErrorCode code, List<TagSelector> selectors) {

        if (code == JdbcErrorCode.WRONG_OBJECT_TYPE) {
            var message = MessageFormat.format(LOAD_BATCH_WRONG_OBJECT_TYPE, "");
            throw new EMetadataWrongType(message, error);
        }
    }

    static ETracInternal catchAll(SQLException error, IDialect dialect) {

        var code = dialect.mapErrorCode(error);

        if (code == JdbcErrorCode.UNKNOWN_ERROR_CODE) {
            var message = MessageFormat.format(UNRECOGNISED_ERROR_CODE, dialect.dialectCode(), error.getSQLState(), error.getErrorCode());
            return new ETracInternal(message, error);
        }
        else {
            var message = MessageFormat.format(UNHANDLED_ERROR, code.name());
            return new ETracInternal(message, error);
        }
    }
}
