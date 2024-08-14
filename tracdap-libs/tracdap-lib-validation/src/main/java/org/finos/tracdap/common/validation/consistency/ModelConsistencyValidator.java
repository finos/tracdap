/*
 * Copyright 2024 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.validation.consistency;

import com.google.protobuf.Descriptors;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.ModelDefinition;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.CONSISTENCY)
public class ModelConsistencyValidator {

    private static final Descriptors.Descriptor MODEL_DEFINITION;
    private static final Descriptors.FieldDescriptor MD_LANGUAGE;
    private static final Descriptors.FieldDescriptor MD_REPOSITORY;

    static {

        MODEL_DEFINITION = ModelDefinition.getDescriptor();
        MD_LANGUAGE = field(MODEL_DEFINITION, ModelDefinition.LANGUAGE_FIELD_NUMBER);
        MD_REPOSITORY = field(MODEL_DEFINITION, ModelDefinition.REPOSITORY_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext modelDefinition(ModelDefinition model, ValidationContext ctx) {

        ctx.push(MD_LANGUAGE)
                .apply(ModelConsistencyValidator::isSupportedLanguage)
                .pop();

        ctx.push(MD_REPOSITORY)
                .apply(ModelConsistencyValidator::isKnownModelRepo)
                .pop();

        return ctx;
    }

    public static ValidationContext isSupportedLanguage(String language, ValidationContext ctx) {

        // TODO: The list of supported languages needs to be made available as a platform resource

        return ctx;
    }

    public static ValidationContext isKnownModelRepo(String repoName, ValidationContext ctx) {

        var resources = ctx.getResources();
        var repos = resources.getRepositoriesMap();

        if (!repos.containsKey(repoName))
            return ctx.error("Model repository [" + repoName + "] is not available in the TRAC platform");

        return ctx;
    }
}
