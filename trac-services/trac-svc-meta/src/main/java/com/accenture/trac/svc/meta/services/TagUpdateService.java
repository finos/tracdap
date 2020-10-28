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

package com.accenture.trac.svc.meta.services;


import com.accenture.trac.common.api.meta.TagOperation;
import com.accenture.trac.common.api.meta.TagUpdate;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.metadata.*;
import com.accenture.trac.svc.meta.exception.TagUpdateError;
import com.accenture.trac.svc.meta.exception.ValidationGapError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

import static com.accenture.trac.svc.meta.services.MetadataConstants.TRAC_RESERVED_IDENTIFIER;


public class TagUpdateService {

    // Available update operations
    private static final Map<TagOperation, BiFunction<Tag.Builder, TagUpdate, Tag.Builder>> TAG_OPERATION_MAP =
            Map.ofEntries(
            Map.entry(TagOperation.CREATE_OR_REPLACE_ATTR, TagUpdateService::createOrReplaceAttr),
            Map.entry(TagOperation.CREATE_OR_APPEND_ATTR, TagUpdateService::createOrAppendAttr),
            Map.entry(TagOperation.CREATE_ATTR, TagUpdateService::createAttr),
            Map.entry(TagOperation.REPLACE_ATTR, TagUpdateService::replaceAttr),
            Map.entry(TagOperation.APPEND_ATTR, TagUpdateService::appendAttr),
            Map.entry(TagOperation.DELETE_ATTR, TagUpdateService::deleteAttr),
            Map.entry(TagOperation.CLEAR_ALL_ATTR, TagUpdateService::clearAllAttr));

    // Error message templates
    private static final String CREATE_ALREADY_EXISTS = "{0} \"{1}\": Attribute already exists";
    private static final String REPLACE_DOES_NOT_EXIST = "{0} \"{1}\": Attribute does not exist";
    private static final String REPLACE_WRONG_TYPE = "{0} \"{1}\": Attribute type does not match (original type = {2}, new type = {3})";
    private static final String APPEND_DOES_NOT_EXIST = "{0} \"{1}\": Attribute does not exist";
    private static final String APPEND_WRONG_TYPE = "{0} \"{1}\": Attribute type does not match (original type = {2}, new type = {3})";
    private static final String DELETE_DOES_NOT_EXIST = "{0} \"{1}\": Attribute does not exist";

    private static final Logger log = LoggerFactory.getLogger(TagUpdateService.class);


    public static Tag applyTagUpdates(Tag priorTag, List<TagUpdate> updates) {

        // Combiner should never be called when reducing sequential operations
        BinaryOperator<Tag.Builder> SEQUENTIAL_COMBINATION =
                (t1, t2) -> { throw new EUnexpected(); };

        var newTag = updates.stream().reduce(
                priorTag.toBuilder(),
                TagUpdateService::applyTagUpdate,
                SEQUENTIAL_COMBINATION);

        return newTag.build();
    }

    private static Tag.Builder applyTagUpdate(Tag.Builder prior, TagUpdate update) {

        if (update.getOperation() == TagOperation.UNRECOGNIZED)
            throw new ValidationGapError("Validation gap for TagUpdate (unrecognized operation)");  // TODO

        var func = TAG_OPERATION_MAP.getOrDefault(update.getOperation(), null);

        if (func == null)
            throw new ValidationGapError(""); // TODO

        return func.apply(prior, update);
    }

    private static Tag.Builder createOrReplaceAttr(Tag.Builder prior, TagUpdate update) {

        if (prior.containsAttr(update.getAttrName()))
            return replaceAttr(prior, update);
        else
            return createAttr(prior, update);
    }

    private static Tag.Builder createOrAppendAttr(Tag.Builder prior, TagUpdate update) {

        if (prior.containsAttr(update.getAttrName()))
            return appendAttr(prior, update);
        else
            return createAttr(prior, update);
    }

    private static Tag.Builder createAttr(Tag.Builder prior, TagUpdate update) {

        requireAttrDoesNotExist(prior, update, CREATE_ALREADY_EXISTS, TagOperation.CREATE_ATTR);

        return prior.putAttr(update.getAttrName(), update.getValue());
    }

    private static Tag.Builder replaceAttr(Tag.Builder prior, TagUpdate update) {

        requireAttrExists(prior, update, REPLACE_DOES_NOT_EXIST, TagOperation.REPLACE_ATTR);
        requireTypeMatches(prior, update, REPLACE_WRONG_TYPE, TagOperation.REPLACE_ATTR);

        return prior.putAttr(update.getAttrName(), update.getValue());
    }

    private static Tag.Builder appendAttr(Tag.Builder prior, TagUpdate update) {

        requireAttrExists(prior, update, APPEND_DOES_NOT_EXIST, TagOperation.APPEND_ATTR);
        requireTypeMatches(prior, update, APPEND_WRONG_TYPE, TagOperation.APPEND_ATTR);

        var priorValue = prior.getAttrOrThrow(update.getAttrName());

        var priorArrayValue = TypeSystem.isPrimitive(priorValue)
                ? ArrayValue.newBuilder().addItem(priorValue)
                : priorValue.getArrayValue().toBuilder();

        var valuesToAppend = TypeSystem.isPrimitive(update.getValue())
                ? List.of(update.getValue())
                : update.getValue().getArrayValue().getItemList();

        var attrType = TypeDescriptor.newBuilder()
                .setBasicType(BasicType.ARRAY)
                .setArrayType(TypeDescriptor.newBuilder()
                .setBasicType(attrBasicType(priorValue)))
                .build();

        var newValue = Value.newBuilder()
                .setType(attrType)
                .setArrayValue(priorArrayValue
                .addAllItem(valuesToAppend))
                .build();

        return prior.putAttr(update.getAttrName(), newValue);
    }

    private static Tag.Builder deleteAttr(Tag.Builder prior, TagUpdate update) {

        requireAttrExists(prior, update, DELETE_DOES_NOT_EXIST, TagOperation.DELETE_ATTR);

        return prior.removeAttr(update.getAttrName());
    }

    private static Tag.Builder clearAllAttr(Tag.Builder prior, TagUpdate update) {

        // Don't try to remove keys from the set we are iterating
        var allAttrs = new HashSet<>(prior.getAttrMap().keySet());

        var working = prior;

        for (var attrName : allAttrs) {

            // Only clear regular attributes, not TRAC controlled ones
            if (!TRAC_RESERVED_IDENTIFIER.matcher(attrName).matches())
                working = working.removeAttr(attrName);
        }

        return working;
    }

    private static BasicType attrBasicType(Value attrValue) {

        if (TypeSystem.isPrimitive(attrValue))
            return attrValue.getType().getBasicType();

        // Should never happen
        if (!attrValue.hasType())
            throw new ValidationGapError("");

        if (TypeSystem.basicType(attrValue) == BasicType.ARRAY) {

            var arrayType = attrValue.getType().getArrayType();

            if (TypeSystem.isPrimitive(arrayType))
                return arrayType.getBasicType();
        }

        // Should never happen
        throw new ValidationGapError("");  // TODO
    }

    private static void requireAttrDoesNotExist(
            Tag.Builder tag, TagUpdate update,
            String errorTemplate, TagOperation operation) {

        if (tag.containsAttr(update.getAttrName())) {

            var message = MessageFormat.format(errorTemplate,
                    operation.name(), update.getAttrName());

            log.error("{} ({})", message, logTagHeader(tag));
            throw new TagUpdateError(message);
        }
    }

    private static void requireAttrExists(
            Tag.Builder tag, TagUpdate update,
            String errorTemplate, TagOperation operation) {

        if (!tag.containsAttr(update.getAttrName())) {

            var message = MessageFormat.format(errorTemplate,
                    operation.name(), update.getAttrName());

            log.error("{} ({})", message, logTagHeader(tag));
            throw new TagUpdateError(message);
        }
    }

    private static void requireTypeMatches(
            Tag.Builder tag, TagUpdate update,
            String errorTemplate, TagOperation operation) {

        var originalType = attrBasicType(tag.getAttrOrThrow(update.getAttrName()));
        var updateType = attrBasicType(update.getValue());

        if (originalType != updateType) {

            var message = MessageFormat.format(errorTemplate,
                    operation.name(), update.getAttrName(),
                    originalType.name(), updateType.name());

            log.error("{} ({})", message, logTagHeader(tag));
            throw new TagUpdateError(message);
        }
    }

    private static String logTagHeader(TagOrBuilder tag) {

        if (!tag.hasHeader())
            return "tag header not available";

        var headerTemplate = "%s %s, version = %d, tag version = %d";
        var header = tag.getHeader();

        return String.format(headerTemplate,
                header.getObjectType(),
                header.getObjectId(),
                header.getObjectVersion(),
                header.getTagVersion());
    }
}
