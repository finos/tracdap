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

package org.finos.tracdap.common.validation.static_;

import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationFunction;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;

import java.util.HashMap;
import java.util.Map;

import static org.finos.tracdap.common.validation.ValidationConstants.MODEL_ENTRY_POINT;
import static org.finos.tracdap.common.validation.ValidationConstants.MODEL_VERSION;
import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class ModelValidator {

    private static final Descriptors.Descriptor MODEL_DEFINITION;
    private static final Descriptors.FieldDescriptor MD_LANGUAGE;
    private static final Descriptors.FieldDescriptor MD_REPOSITORY;
    private static final Descriptors.FieldDescriptor MD_PATH;
    private static final Descriptors.FieldDescriptor MD_ENTRY_POINT;
    private static final Descriptors.FieldDescriptor MD_VERSION;
    private static final Descriptors.FieldDescriptor MD_PARAMETERS;
    private static final Descriptors.FieldDescriptor MD_INPUTS;
    private static final Descriptors.FieldDescriptor MD_OUTPUTS;

    private static final Descriptors.Descriptor MODEL_PARAMETER;
    private static final Descriptors.FieldDescriptor MP_PARAM_TYPE;
    private static final Descriptors.FieldDescriptor MP_LABEL;
    private static final Descriptors.FieldDescriptor MP_DEFAULT_VALUE;

    private static final Descriptors.Descriptor MODEL_INPUT_SCHEMA;
    private static final Descriptors.FieldDescriptor MIS_SCHEMA;

    private static final Descriptors.Descriptor MODEL_OUTPUT_SCHEMA;
    private static final Descriptors.FieldDescriptor MOS_SCHEMA;

    static {

        MODEL_DEFINITION = ModelDefinition.getDescriptor();
        MD_LANGUAGE = field(MODEL_DEFINITION, ModelDefinition.LANGUAGE_FIELD_NUMBER);
        MD_REPOSITORY = field(MODEL_DEFINITION, ModelDefinition.REPOSITORY_FIELD_NUMBER);
        MD_PATH = field(MODEL_DEFINITION, ModelDefinition.PATH_FIELD_NUMBER);
        MD_ENTRY_POINT = field(MODEL_DEFINITION, ModelDefinition.ENTRYPOINT_FIELD_NUMBER);
        MD_VERSION = field(MODEL_DEFINITION, ModelDefinition.VERSION_FIELD_NUMBER);
        MD_PARAMETERS = field(MODEL_DEFINITION, ModelDefinition.PARAMETERS_FIELD_NUMBER);
        MD_INPUTS = field(MODEL_DEFINITION, ModelDefinition.INPUTS_FIELD_NUMBER);
        MD_OUTPUTS = field(MODEL_DEFINITION, ModelDefinition.OUTPUTS_FIELD_NUMBER);

        MODEL_PARAMETER = ModelParameter.getDescriptor();
        MP_PARAM_TYPE = field(MODEL_PARAMETER, ModelParameter.PARAMTYPE_FIELD_NUMBER);
        MP_LABEL = field(MODEL_PARAMETER, ModelParameter.LABEL_FIELD_NUMBER);
        MP_DEFAULT_VALUE = field(MODEL_PARAMETER, ModelParameter.DEFAULTVALUE_FIELD_NUMBER);

        MODEL_INPUT_SCHEMA = ModelInputSchema.getDescriptor();
        MIS_SCHEMA = field(MODEL_INPUT_SCHEMA, ModelInputSchema.SCHEMA_FIELD_NUMBER);

        MODEL_OUTPUT_SCHEMA = ModelOutputSchema.getDescriptor();
        MOS_SCHEMA = field(MODEL_OUTPUT_SCHEMA, ModelOutputSchema.SCHEMA_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext model(ModelDefinition msg, ValidationContext ctx) {

        ctx = modelDetails(MD_LANGUAGE, MD_REPOSITORY, MD_PATH, MD_ENTRY_POINT, MD_VERSION, ctx);

        ctx = modelSchema(MD_PARAMETERS, MD_INPUTS, MD_OUTPUTS, ctx);

        return ctx;
    }

    public static ValidationContext modelDetails(
            Descriptors.FieldDescriptor languageField,
            Descriptors.FieldDescriptor repositoryField,
            Descriptors.FieldDescriptor pathField,
            Descriptors.FieldDescriptor entryPointField,
            Descriptors.FieldDescriptor versionField,
            ValidationContext ctx) {

        ctx = ctx.push(languageField)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(repositoryField)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(pathField)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::relativePath)
                .pop();

        ctx = ctx.push(entryPointField)
                .apply(CommonValidators::required)
                .apply(ModelValidator::modelEntryPoint)
                .pop();

        ctx = ctx.push(versionField)
                .apply(CommonValidators::required)
                .apply(ModelValidator::modelVersion)
                .pop();

        return ctx;
    }

    public static ValidationContext modelSchema(
            Descriptors.FieldDescriptor paramsField,
            Descriptors.FieldDescriptor inputsField,
            Descriptors.FieldDescriptor outputsField,
            ValidationContext ctx) {

        var knownIdentifiers = new HashMap<String, String>();

        ctx = ctx.pushMap(paramsField)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyMapKeys(uniqueContextCheck(knownIdentifiers, paramsField.getName()))
                .applyMapValues(ModelValidator::modelParameter, ModelParameter.class)
                .pop();

        ctx = ctx.pushMap(inputsField)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyMapKeys(uniqueContextCheck(knownIdentifiers, inputsField.getName()))
                .applyMapValues(ModelValidator::modelInputSchema, ModelInputSchema.class)
                .pop();

        ctx = ctx.pushMap(outputsField)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyMapKeys(uniqueContextCheck(knownIdentifiers, outputsField.getName()))
                .applyMapValues(ModelValidator::modelOutputSchema, ModelOutputSchema.class)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext modelParameter(ModelParameter msg, ValidationContext ctx) {

        ctx = ctx.push(MP_PARAM_TYPE)
                .apply(CommonValidators::required)
                .apply(TypeSystemValidator::typeDescriptor, TypeDescriptor.class)
                .pop();

        ctx = ctx.push(MP_LABEL)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::labelLengthLimit)
                .pop();

        ctx = ctx.push(MP_DEFAULT_VALUE)
                .apply(CommonValidators::optional)
                .apply(TypeSystemValidator::valueWithType, Value.class, msg.getParamType())
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext modelInputSchema(ModelInputSchema msg, ValidationContext ctx) {

        return ctx.push(MIS_SCHEMA)
                .apply(CommonValidators::required)
                .applyRegistered()
                .pop();
    }

    @Validator
    public static ValidationContext modelOutputSchema(ModelOutputSchema msg, ValidationContext ctx) {

        return ctx.push(MOS_SCHEMA)
                .apply(CommonValidators::required)
                .applyRegistered()
                .pop();
    }

    public static ValidationContext modelEntryPoint(String modelEntryPoint, ValidationContext ctx) {

        var matcher = MODEL_ENTRY_POINT.matcher(modelEntryPoint);

        if (!matcher.matches()) {

            var err = String.format(
                    "Invalid model entry point [%s] (expected format: pkg.sub_pkg.ModelClass)",
                    modelEntryPoint);

            return ctx.error(err);
        }

        return ctx;
    }

    public static ValidationContext modelVersion(String modelVersion, ValidationContext ctx) {

        var matcher = MODEL_VERSION.matcher(modelVersion);

        if (!matcher.matches()) {

            var err = String.format(
                    "Invalid model version [%s] (version can contain letters, numbers, hyphen, underscore and period, starting with a letter or number)",
                    modelVersion);

            return ctx.error(err);
        }

        return ctx;
    }

    private static ValidationFunction.Typed<String> uniqueContextCheck(Map<String, String> knownIdentifiers, String fieldName) {

        return (key, ctx) -> {

            var lowerKey = key.toLowerCase();

            if (knownIdentifiers.containsKey(lowerKey)) {

                var err = String.format(
                        "[%s] is already defined in [%s]",
                        key,  knownIdentifiers.get(lowerKey));

                return ctx.error(err);
            }

            knownIdentifiers.put(lowerKey, fieldName);

            return ctx;
        };
    }
}
