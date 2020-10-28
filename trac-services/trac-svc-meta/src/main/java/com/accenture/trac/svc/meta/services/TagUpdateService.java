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
import com.accenture.trac.common.exception.ETrac;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.metadata.Tag;
import com.accenture.trac.common.metadata.TypeDescriptor;
import com.accenture.trac.common.metadata.TypeSystem;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;


public class TagUpdateService {

    private static Map<TagOperation, BiFunction<Tag.Builder, TagUpdate, Tag.Builder>> TAG_OPERATION_MAP =
            Map.ofEntries(
            Map.entry(TagOperation.CREATE_OR_REPLACE_ATTR, TagUpdateService::createOrReplaceAttr),
            Map.entry(TagOperation.CREATE_OR_APPEND_ATTR, TagUpdateService::createOrAppendAttr),
            Map.entry(TagOperation.CREATE_ATTR, TagUpdateService::createAttr),
            Map.entry(TagOperation.REPLACE_ATTR, TagUpdateService::replaceAttr),
            Map.entry(TagOperation.APPEND_ATTR, TagUpdateService::appendAttr),
            Map.entry(TagOperation.DELETE_ATTR, TagUpdateService::deleteAttr),
            Map.entry(TagOperation.CLEAR_ALL_ATTR, TagUpdateService::clearAllAttr));


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

        var func = TAG_OPERATION_MAP.getOrDefault(update.getOperation(), null);

        if (func == null)
            throw new RuntimeException("");  // TODO: Error

        return func.apply(prior, update);
    }

    private static Tag.Builder createOrReplaceAttr(Tag.Builder prior, TagUpdate update) {

        return prior;
    }

    private static Tag.Builder createOrAppendAttr(Tag.Builder prior, TagUpdate update) {

        return prior;
    }

    private static Tag.Builder createAttr(Tag.Builder prior, TagUpdate update) {

        return prior;
    }

    private static Tag.Builder replaceAttr(Tag.Builder prior, TagUpdate update) {

        return prior;
    }

    private static Tag.Builder appendAttr(Tag.Builder prior, TagUpdate update) {

        return prior;
    }

    private static Tag.Builder deleteAttr(Tag.Builder prior, TagUpdate update) {

        return prior;
    }

    private static Tag.Builder clearAllAttr(Tag.Builder prior, TagUpdate update) {

        return prior;
    }

//
//    private static Tag.Builder applyTagUpdate(Tag.Builder tag, TagUpdate update) {
//
//        switch (update.getOperation()) {
//
//            case CREATE_OR_REPLACE_ATTR:
//
//                return tag.putAttr(update.getAttrName(), update.getValue());
//
//            case CREATE_ATTR:
//
//                if (tag.containsAttr(update.getAttrName()))
//                    throw new ETrac("");  // attr already exists
//
//                return tag.putAttr(update.getAttrName(), update.getValue());
//
//            case REPLACE_ATTR:
//
//                if (!tag.containsAttr(update.getAttrName()))
//                    throw new ETrac("");
//
//                var priorType = tag.getAttrOrDefault(update.getAttrName(), update.getValue()).getType();
//                var newType = update.getValue().getType();
//
//                if (!attrTypesMatch(priorType, newType))
//                    throw new ETrac("");
//
//                return tag.putAttr(update.getAttrName(), update.getValue());
//
//            case APPEND_ATTR:
//
//                throw new ETrac("");  // TODO
//
//            case DELETE_ATTR:
//
//                if (!tag.containsAttr(update.getAttrName()))
//                    throw new ETrac("");
//
//                return tag.removeAttr(update.getAttrName());
//
//            default:
//                // Should be picked up by validation
//                throw new EUnexpected();
//        }
//    }
//
//    private boolean attrTypesMatch(TypeDescriptor attr1, TypeDescriptor attr2) {
//
//        // TODO: Array types
//        return TypeSystem.isPrimitive(attr1) && attr1.getBasicType() == attr2.getBasicType();
//
//    }
}
