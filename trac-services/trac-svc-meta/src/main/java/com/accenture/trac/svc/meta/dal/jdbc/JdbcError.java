/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.meta.dal.jdbc;

import com.accenture.trac.metadata.TagSelector;
import com.accenture.trac.common.exception.*;
import com.accenture.trac.svc.meta.dal.jdbc.dialects.IDialect;
import com.accenture.trac.svc.meta.exception.EDuplicateItem;
import com.accenture.trac.svc.meta.exception.EMissingItem;
import com.accenture.trac.svc.meta.exception.EWrongItemType;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;

class JdbcError {

    static final String UNHANDLED_ERROR = "Unhandled SQL Error code: {0}";
    static final String UNRECOGNISED_ERROR_CODE = "Unrecognised SQL Error code: {0}, sqlstate = {1}, error code = {2}";

    static final String DUPLICATE_OBJECT_ID = "Duplicate object id {0}";
    static final String MISSING_ITEM = "Metadata item does not exist {0}";
    static final String WRONG_OBJECT_TYPE = "Metadata item has the wrong type";

    static final String LOAD_ONE_MISSING_ITEM = "Metadata item does not exist {0}";
    static final String LOAD_ONE_WRONG_OBJECT_TYPE = "Metadata item has the wrong type";

    static final String LOAD_BATCH_MISSING_ITEM = "One or more metadata item does not exist {0}";
    static final String LOAD_BATCH_WRONG_OBJECT_TYPE = "One or more metadata item has the wrong type";


    static ETracInternal unhandledError(SQLException error, JdbcErrorCode code) {

        var message = MessageFormat.format(UNHANDLED_ERROR, code.name());
        return new ETracInternal(message, error);
    }

    static void handleUnknownError(SQLException error, JdbcErrorCode code, IDialect dialect) {

        if (code == JdbcErrorCode.UNKNOWN_ERROR_CODE) {
            var message = MessageFormat.format(UNRECOGNISED_ERROR_CODE, dialect.dialectCode(), error.getSQLState(), error.getErrorCode());
            throw new ETracInternal(message, error);
        }
    }


    static void handleDuplicateObjectId(SQLException error, JdbcErrorCode code, JdbcMetadataDal.ObjectParts parts) {

        if (code == JdbcErrorCode.INSERT_DUPLICATE) {
            var message = MessageFormat.format(DUPLICATE_OBJECT_ID, parts.objectId[0]);
            throw new EDuplicateItem(message, error);
        }
    }

    static void handleMissingItem(SQLException error, JdbcErrorCode code, JdbcMetadataDal.ObjectParts parts) {

        if (code == JdbcErrorCode.NO_DATA) {
            var message = MessageFormat.format(MISSING_ITEM, "");
            throw new EMissingItem(message, error);
        }
    }

    static void newVersion_WrongType(SQLException error, JdbcErrorCode code, JdbcMetadataDal.ObjectParts parts) {

        if (code == JdbcErrorCode.WRONG_OBJECT_TYPE) {
            var message = MessageFormat.format(WRONG_OBJECT_TYPE, "");
            throw new EWrongItemType(message, error);
        }
    }

    static void newTag_WrongType(SQLException error, JdbcErrorCode code, JdbcMetadataDal.ObjectParts parts) {

        if (code == JdbcErrorCode.WRONG_OBJECT_TYPE) {
            var message = MessageFormat.format(WRONG_OBJECT_TYPE, "");
            throw new EWrongItemType(message, error);
        }
    }

    static void savePreallocated_WrongType(SQLException error, JdbcErrorCode code, JdbcMetadataDal.ObjectParts parts) {

        if (code == JdbcErrorCode.WRONG_OBJECT_TYPE) {
            var message = MessageFormat.format(WRONG_OBJECT_TYPE, "");
            throw new EWrongItemType(message, error);
        }
    }

    static void loadOne_missingItem(SQLException error, JdbcErrorCode code, TagSelector selector) {

        if (code == JdbcErrorCode.NO_DATA) {
            var message = MessageFormat.format(LOAD_ONE_MISSING_ITEM, "");
            throw new EMissingItem(message, error);
        }
    }

    static void loadOne_WrongObjectType(SQLException error, JdbcErrorCode code, TagSelector selector) {

        if (code == JdbcErrorCode.WRONG_OBJECT_TYPE) {
            var message = MessageFormat.format(LOAD_ONE_WRONG_OBJECT_TYPE, "");
            throw new EWrongItemType(message, error);
        }
    }

    static void loadBatch_missingItem(SQLException error, JdbcErrorCode code, List<TagSelector> selectors) {

        if (code == JdbcErrorCode.NO_DATA) {
            var message = MessageFormat.format(LOAD_BATCH_MISSING_ITEM, "");
            throw new EMissingItem(message, error);
        }
    }

    static void loadBatch_WrongObjectType(SQLException error, JdbcErrorCode code, List<TagSelector> selectors) {

        if (code == JdbcErrorCode.WRONG_OBJECT_TYPE) {
            var message = MessageFormat.format(LOAD_BATCH_WRONG_OBJECT_TYPE, "");
            throw new EWrongItemType(message, error);
        }
    }
}
