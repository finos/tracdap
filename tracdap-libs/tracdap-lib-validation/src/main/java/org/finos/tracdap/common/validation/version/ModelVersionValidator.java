/*
 * Copyright 2024 finTRAC Limited
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

import org.finos.tracdap.metadata.ModelDefinition;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;

import com.google.protobuf.Descriptors;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.VERSION)
public class ModelVersionValidator {

    private static final Descriptors.Descriptor MODEL_DEFINITION;
    private static final Descriptors.FieldDescriptor MD_MODEL_TYPE;
    private static final Descriptors.FieldDescriptor MD_LANGUAGE;
    private static final Descriptors.FieldDescriptor MD_REPOSITORY;
    private static final Descriptors.FieldDescriptor MD_PACKAGE_GROUP;
    private static final Descriptors.FieldDescriptor MD_PACKAGE;
    private static final Descriptors.FieldDescriptor MD_VERSION;
    private static final Descriptors.FieldDescriptor MD_PATH;
    private static final Descriptors.FieldDescriptor MD_ENTRY_POINT;

    static {

        MODEL_DEFINITION = ModelDefinition.getDescriptor();
        MD_MODEL_TYPE = field(MODEL_DEFINITION, ModelDefinition.MODELTYPE_FIELD_NUMBER);
        MD_LANGUAGE = field(MODEL_DEFINITION, ModelDefinition.LANGUAGE_FIELD_NUMBER);
        MD_REPOSITORY = field(MODEL_DEFINITION, ModelDefinition.REPOSITORY_FIELD_NUMBER);
        MD_PACKAGE_GROUP = field(MODEL_DEFINITION, ModelDefinition.PACKAGEGROUP_FIELD_NUMBER);
        MD_PACKAGE = field(MODEL_DEFINITION, ModelDefinition.PACKAGE_FIELD_NUMBER);
        MD_VERSION = field(MODEL_DEFINITION, ModelDefinition.VERSION_FIELD_NUMBER);
        MD_PATH = field(MODEL_DEFINITION, ModelDefinition.PATH_FIELD_NUMBER);
        MD_ENTRY_POINT = field(MODEL_DEFINITION, ModelDefinition.ENTRYPOINT_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext modelVersion(ModelDefinition current, ModelDefinition prior, ValidationContext ctx) {

        // Two models are considered versions of the same model if all coordinates match between versions
        // I.e. model compatibility is not based on functional compatibility
        // If a model is moved to a different repo or namespace, it is considered a new model
        // So, the evolution of one model in code can be tracked by a single model object in TRAC

        ctx = ctx.push(MD_MODEL_TYPE)
                .apply(CommonValidators::exactMatch)
                .pop();

        ctx = ctx.push(MD_LANGUAGE)
                .apply(CommonValidators::exactMatch)
                .pop();

        ctx = ctx.push(MD_REPOSITORY)
                .apply(CommonValidators::exactMatch)
                .pop();

        ctx = ctx.push(MD_PACKAGE_GROUP)
                .apply(CommonValidators::exactMatch)
                .pop();

        ctx = ctx.push(MD_PACKAGE)
                .apply(CommonValidators::exactMatch)
                .pop();

        ctx = ctx.push(MD_VERSION)
                .apply(CommonValidators::exactMatch)
                .pop();

        ctx = ctx.push(MD_PATH)
                .apply(CommonValidators::exactMatch)
                .pop();

        ctx = ctx.push(MD_ENTRY_POINT)
                .apply(CommonValidators::exactMatch)
                .pop();

        return ctx;
    }
}
