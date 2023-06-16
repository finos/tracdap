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
import org.finos.tracdap.svc.meta.dal.jdbc.dialects.IDialect;
import org.finos.tracdap.common.exception.EMetadataDuplicate;
import org.finos.tracdap.common.exception.EMetadataNotFound;
import org.finos.tracdap.common.exception.EMetadataWrongType;

import java.sql.SQLException;
import java.text.MessageFormat;


class JdbcError {

    static final String MISSING_ITEM = "Metadata not found for {0} [{1}] (version = {2}, tag = {3})";
    static final String MISSING_ITEM_MULTIPLE = "Metadata not found for one or more items";

    static final String WRONG_OBJECT_TYPE = "Metadata does not match the expected type for {0} [{1}]";
    static final String WRONG_OBJECT_TYPE_MULTIPLE = "Metadata does not match the expected type for one or more objects";

    static final String DUPLICATE_OBJECT_ID = "Duplicate object ID for {0} [{1}]";
    static final String DUPLICATE_OBJECT_ID_MULTIPLE = "Duplicate object ID for one or more objects";

    static final String ID_NOT_PREALLOCATED = "Object ID was not preallocated for {0} [{1}]";
    static final String ID_NOT_PREALLOCATED_MULTIPLE = "Object ID was not preallocated for one or more objects";
    static final String ID_ALREADY_IN_USE = "Object ID already in use for {0} [{1}]";
    static final String ID_ALREADY_IN_USE_MULTIPLE = "Object ID already in use for one or more objects";

    static final String PRIOR_VERSION_MISSING = "Prior version not found for {0} [{1}] (version = {2})";
    static final String PRIOR_VERSION_MISSING_MULTIPLE = "Prior version not found for one or more version updates";
    static final String VERSION_SUPERSEDED = "Prior version has been superseded for {0} [{1}] (version = {2})";
    static final String VERSION_SUPERSEDED_MULTIPLE = "Prior version has been superseded for one or more version updates";

    static final String PRIOR_TAG_MISSING = "Prior tag not found for {0} [{1}] (version = {2}, tag = {3})";
    static final String PRIOR_TAG_MISSING_MULTIPLE = "Prior tag not found for one or more tag updates";
    static final String TAG_SUPERSEDED = "Prior tag has been superseded for {0} [{1}] (version = {2}, tag = {3})";
    static final String TAG_SUPERSEDED_MULTIPLE = "Prior tag has been superseded for one or more tag updates";

    static final String UNRECOGNISED_ERROR_CODE = "Unrecognised SQL Error code: {0}, sqlstate = {1}, error code = {2}";
    static final String UNHANDLED_ERROR = "Unhandled SQL Error code: {0}";

    static void objectNotFound(
            SQLException error, IDialect dialect, JdbcMetadataDal.ObjectParts parts,
            boolean priorVersions, boolean priorTags) {

        var multipleItemMessage =
                priorVersions ? PRIOR_VERSION_MISSING_MULTIPLE :
                priorTags ? PRIOR_TAG_MISSING_MULTIPLE :
                MISSING_ITEM_MULTIPLE;

        var singleItemMessage =
                priorVersions ? PRIOR_VERSION_MISSING :
                priorTags ? PRIOR_TAG_MISSING :
                MISSING_ITEM;

        var code = dialect.mapErrorCode(error);

        if (code != JdbcErrorCode.NO_DATA)
            return;

        if (parts.objectId.length != 1)
            throw new EMetadataNotFound(multipleItemMessage, error);

        var selector  = parts.selector[0];
        String version;
        String tag;

        if (selector.hasObjectVersion())
            version = Integer.toString(selector.getObjectVersion());
        else if (selector.hasObjectAsOf())
            version = "as-of " + selector.getObjectAsOf().getIsoDatetime();
        else
            version = "latest";

        if (selector.hasTagVersion())
            tag = Integer.toString(selector.getTagVersion());
        else if (selector.hasTagAsOf())
            tag = "as-of " + selector.getTagAsOf().getIsoDatetime();
        else
            tag = "latest";

        var message = MessageFormat.format(singleItemMessage,
                parts.objectType[0],
                parts.objectId[0],
                version,
                tag);

        throw new EMetadataNotFound(message, error);
    }

    static void wrongObjectType(SQLException error, IDialect dialect, JdbcMetadataDal.ObjectParts parts) {

        var code = dialect.mapErrorCode(error);

        if (code != JdbcErrorCode.WRONG_OBJECT_TYPE)
            return;

        if (parts.objectId.length != 1)
            throw new EMetadataWrongType(WRONG_OBJECT_TYPE_MULTIPLE, error);

        var message = MessageFormat.format(WRONG_OBJECT_TYPE,
                parts.objectType[0],
                parts.objectId[0]);

        throw new EMetadataWrongType(message, error);
    }

    static void duplicateObjectId(SQLException error, IDialect dialect, JdbcMetadataDal.ObjectParts parts) {

        var code = dialect.mapErrorCode(error);

        if (code != JdbcErrorCode.INSERT_DUPLICATE)
            return;

        if (parts.objectId.length != 1)
            throw new EMetadataDuplicate(DUPLICATE_OBJECT_ID_MULTIPLE, error);

        var message = MessageFormat.format(DUPLICATE_OBJECT_ID,
                parts.objectType[0],
                parts.objectId[0]);

        throw new EMetadataDuplicate(message, error);
    }

    static void idNotPreallocated(SQLException error, IDialect dialect, JdbcMetadataDal.ObjectParts parts) {

        var code = dialect.mapErrorCode(error);

        if (code != JdbcErrorCode.NO_DATA)
            return;

        if (parts.objectId.length != 1)
            throw new EMetadataNotFound(ID_NOT_PREALLOCATED_MULTIPLE, error);

        var message = MessageFormat.format(ID_NOT_PREALLOCATED,
                parts.objectType[0],
                parts.objectId[0]);

        throw new EMetadataNotFound(message, error);
    }

    static void idAlreadyInUse(SQLException error, IDialect dialect, JdbcMetadataDal.ObjectParts parts) {

        var code = dialect.mapErrorCode(error);

        if (code != JdbcErrorCode.INSERT_DUPLICATE)
            return;

        if (parts.objectId.length != 1)
            throw new EMetadataDuplicate(ID_ALREADY_IN_USE_MULTIPLE, error);

        var message = MessageFormat.format(ID_ALREADY_IN_USE,
                parts.objectType[0],
                parts.objectId[0]);

        throw new EMetadataDuplicate(message, error);
    }

    static void priorVersionMissing(SQLException error, IDialect dialect, JdbcMetadataDal.ObjectParts parts) {

        var code = dialect.mapErrorCode(error);

        if (code != JdbcErrorCode.NO_DATA)
            return;

        if (parts.objectId.length != 1)
            throw new EMetadataNotFound(PRIOR_VERSION_MISSING_MULTIPLE, error);

        var message = MessageFormat.format(PRIOR_VERSION_MISSING,
                parts.objectType[0],
                parts.objectId[0],
                parts.objectVersion[0]);

        throw new EMetadataNotFound(message, error);
    }

    static void versionSuperseded(SQLException error, IDialect dialect, JdbcMetadataDal.ObjectParts parts) {

        var code = dialect.mapErrorCode(error);

        if (code != JdbcErrorCode.INSERT_DUPLICATE)
            return;

        if (parts.objectId.length != 1)
            throw new EMetadataDuplicate(VERSION_SUPERSEDED_MULTIPLE, error);

        var message = MessageFormat.format(VERSION_SUPERSEDED,
                parts.objectType[0],
                parts.objectId[0],
                parts.objectVersion[0]);

        throw new EMetadataDuplicate(message, error);
    }

    static void priorTagMissing(SQLException error, IDialect dialect, JdbcMetadataDal.ObjectParts parts) {

        var code = dialect.mapErrorCode(error);

        if (code != JdbcErrorCode.NO_DATA)
            return;

        if (parts.objectId.length != 1)
            throw new EMetadataNotFound(PRIOR_TAG_MISSING_MULTIPLE, error);

        var message = MessageFormat.format(PRIOR_TAG_MISSING,
                parts.objectType[0],
                parts.objectId[0],
                parts.objectVersion[0],
                parts.tagVersion[0]);

        throw new EMetadataNotFound(message, error);
    }

    static void tagSuperseded(SQLException error, IDialect dialect, JdbcMetadataDal.ObjectParts parts) {

        var code = dialect.mapErrorCode(error);

        if (code != JdbcErrorCode.INSERT_DUPLICATE)
            return;

        if (parts.objectId.length != 1)
            throw new EMetadataDuplicate(TAG_SUPERSEDED_MULTIPLE, error);

        var message = MessageFormat.format(TAG_SUPERSEDED,
                parts.objectType[0],
                parts.objectId[0],
                parts.objectVersion[0],
                parts.tagVersion[0]);

        throw new EMetadataDuplicate(message, error);
    }

    static ETracInternal catchAll(SQLException error, IDialect dialect) {

        var code = dialect.mapErrorCode(error);

        // An unknown error code means the SQL error could not be mapped
        if (code == JdbcErrorCode.UNKNOWN_ERROR_CODE) {

            var message = MessageFormat.format(UNRECOGNISED_ERROR_CODE,
                    dialect.dialectCode(),
                    error.getSQLState(),
                    error.getErrorCode());

            return new ETracInternal(message, error);
        }

        // Last fallback - the error code was mapped but there is a missing handler
        var message = MessageFormat.format(UNHANDLED_ERROR, code.name());
        return new ETracInternal(message, error);
    }
}
