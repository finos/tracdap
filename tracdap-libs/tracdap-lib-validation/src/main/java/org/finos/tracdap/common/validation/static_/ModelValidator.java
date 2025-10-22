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

import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;

import java.util.HashMap;
import java.util.List;

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
    private static final Descriptors.FieldDescriptor MD_RESOURCES;

    private static final Descriptors.Descriptor MODEL_PARAMETER;
    private static final Descriptors.FieldDescriptor MP_PARAM_TYPE;
    private static final Descriptors.FieldDescriptor MP_LABEL;
    private static final Descriptors.FieldDescriptor MP_DEFAULT_VALUE;
    private static final Descriptors.FieldDescriptor MP_PARAM_PROPS;

    private static final Descriptors.Descriptor MODEL_INPUT_SCHEMA;
    private static final Descriptors.FieldDescriptor MIS_OBJECT_TYPE;
    private static final Descriptors.FieldDescriptor MIS_SCHEMA;
    private static final Descriptors.FieldDescriptor MIS_FILE_TYPE;
    private static final Descriptors.FieldDescriptor MIS_LABEL;
    private static final Descriptors.FieldDescriptor MIS_OPTIONAL;
    private static final Descriptors.FieldDescriptor MIS_DYNAMIC;
    private static final Descriptors.FieldDescriptor MIS_INPUT_PROPS;

    private static final Descriptors.Descriptor MODEL_OUTPUT_SCHEMA;
    private static final Descriptors.FieldDescriptor MOS_OBJECT_TYPE;
    private static final Descriptors.FieldDescriptor MOS_SCHEMA;
    private static final Descriptors.FieldDescriptor MOS_FILE_TYPE;
    private static final Descriptors.FieldDescriptor MOS_LABEL;
    private static final Descriptors.FieldDescriptor MOS_OPTIONAL;
    private static final Descriptors.FieldDescriptor MOS_DYNAMIC;
    private static final Descriptors.FieldDescriptor MOS_OUTPUT_PROPS;

    private static final Descriptors.Descriptor MODEL_RESOURCE;
    private static final Descriptors.FieldDescriptor MR_RESOURCE_TYPE;
    private static final Descriptors.FieldDescriptor MR_PROTOCOL;
    private static final Descriptors.FieldDescriptor MR_SUB_PROTOCOL;
    private static final Descriptors.FieldDescriptor MR_SYSTEM;
    private static final Descriptors.FieldDescriptor MR_LABEL;
    private static final Descriptors.FieldDescriptor MR_RESOURCE_PROPS;

    private static final Descriptors.Descriptor MODEL_SYSTEM_DETAILS;
    private static final Descriptors.FieldDescriptor MSD_CLIENT_TYPE;

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
        MD_RESOURCES = field(MODEL_DEFINITION, ModelDefinition.RESOURCES_FIELD_NUMBER);

        MODEL_PARAMETER = ModelParameter.getDescriptor();
        MP_PARAM_TYPE = field(MODEL_PARAMETER, ModelParameter.PARAMTYPE_FIELD_NUMBER);
        MP_LABEL = field(MODEL_PARAMETER, ModelParameter.LABEL_FIELD_NUMBER);
        MP_DEFAULT_VALUE = field(MODEL_PARAMETER, ModelParameter.DEFAULTVALUE_FIELD_NUMBER);
        MP_PARAM_PROPS = field(MODEL_PARAMETER, ModelParameter.PARAMPROPS_FIELD_NUMBER);

        MODEL_INPUT_SCHEMA = ModelInputSchema.getDescriptor();
        MIS_OBJECT_TYPE = field(MODEL_INPUT_SCHEMA, ModelOutputSchema.OBJECTTYPE_FIELD_NUMBER);
        MIS_SCHEMA = field(MODEL_INPUT_SCHEMA, ModelInputSchema.SCHEMA_FIELD_NUMBER);
        MIS_FILE_TYPE = field(MODEL_INPUT_SCHEMA, ModelInputSchema.FILETYPE_FIELD_NUMBER);
        MIS_LABEL = field(MODEL_INPUT_SCHEMA, ModelInputSchema.LABEL_FIELD_NUMBER);
        MIS_OPTIONAL = field(MODEL_INPUT_SCHEMA, ModelInputSchema.OPTIONAL_FIELD_NUMBER);
        MIS_DYNAMIC = field(MODEL_INPUT_SCHEMA, ModelInputSchema.DYNAMIC_FIELD_NUMBER);
        MIS_INPUT_PROPS = field(MODEL_INPUT_SCHEMA, ModelInputSchema.INPUTPROPS_FIELD_NUMBER);

        MODEL_OUTPUT_SCHEMA = ModelOutputSchema.getDescriptor();
        MOS_OBJECT_TYPE = field(MODEL_OUTPUT_SCHEMA, ModelOutputSchema.OBJECTTYPE_FIELD_NUMBER);
        MOS_SCHEMA = field(MODEL_OUTPUT_SCHEMA, ModelOutputSchema.SCHEMA_FIELD_NUMBER);
        MOS_FILE_TYPE = field(MODEL_OUTPUT_SCHEMA, ModelOutputSchema.FILETYPE_FIELD_NUMBER);
        MOS_LABEL = field(MODEL_OUTPUT_SCHEMA, ModelOutputSchema.LABEL_FIELD_NUMBER);
        MOS_OPTIONAL = field(MODEL_OUTPUT_SCHEMA, ModelOutputSchema.OPTIONAL_FIELD_NUMBER);
        MOS_DYNAMIC = field(MODEL_OUTPUT_SCHEMA, ModelOutputSchema.DYNAMIC_FIELD_NUMBER);
        MOS_OUTPUT_PROPS = field(MODEL_OUTPUT_SCHEMA, ModelOutputSchema.OUTPUTPROPS_FIELD_NUMBER);

        MODEL_RESOURCE = ModelResource.getDescriptor();
        MR_RESOURCE_TYPE = field(MODEL_RESOURCE, ModelResource.RESOURCETYPE_FIELD_NUMBER);
        MR_PROTOCOL = field(MODEL_RESOURCE, ModelResource.PROTOCOL_FIELD_NUMBER);
        MR_SUB_PROTOCOL = field(MODEL_RESOURCE, ModelResource.SUBPROTOCOL_FIELD_NUMBER);
        MR_SYSTEM = field(MODEL_RESOURCE, ModelResource.SYSTEM_FIELD_NUMBER);
        MR_LABEL = field(MODEL_RESOURCE, ModelResource.LABEL_FIELD_NUMBER);
        MR_RESOURCE_PROPS = field(MODEL_RESOURCE, ModelResource.RESOURCEPROPS_FIELD_NUMBER);

        MODEL_SYSTEM_DETAILS = ModelSystemDetails.getDescriptor();
        MSD_CLIENT_TYPE = field(MODEL_SYSTEM_DETAILS, ModelSystemDetails.CLIENTTYPE_FIELD_NUMBER);
    }

    @Validator
    public static ValidationContext model(ModelDefinition msg, ValidationContext ctx) {

        ctx = modelDetails(MD_LANGUAGE, MD_REPOSITORY, MD_PATH, MD_ENTRY_POINT, MD_VERSION, ctx);

        ctx = modelSchema(MD_PARAMETERS, MD_INPUTS, MD_OUTPUTS, MD_RESOURCES, ctx);

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
            Descriptors.FieldDescriptor resourcesField,
            ValidationContext ctx) {

        var knownIdentifiers = new HashMap<String, String>();

        ctx = ctx.pushMap(paramsField)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyMapKeys(CommonValidators.uniqueContextCheck(knownIdentifiers, paramsField.getName()))
                .applyMapValues(ModelValidator::modelParameter, ModelParameter.class)
                .pop();

        ctx = ctx.pushMap(inputsField)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyMapKeys(CommonValidators.uniqueContextCheck(knownIdentifiers, inputsField.getName()))
                .applyMapValues(ModelValidator::modelInputSchema, ModelInputSchema.class)
                .pop();

        ctx = ctx.pushMap(outputsField)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyMapKeys(CommonValidators.uniqueContextCheck(knownIdentifiers, outputsField.getName()))
                .applyMapValues(ModelValidator::modelOutputSchema, ModelOutputSchema.class)
                .pop();

        ctx = ctx.pushMap(resourcesField)
                .applyMapKeys(CommonValidators::identifier)
                .applyMapKeys(CommonValidators::notTracReserved)
                .apply(CommonValidators::caseInsensitiveDuplicates)
                .applyMapKeys(CommonValidators.uniqueContextCheck(knownIdentifiers, resourcesField.getName()))
                .applyMapValues(ModelValidator::modelResource, ModelResource.class)
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

        ctx = ctx.pushMap(MP_PARAM_PROPS)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::standardProps)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext modelInputSchema(ModelInputSchema msg, ValidationContext ctx) {

        ctx = ctx.push(MIS_OBJECT_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, ObjectType.class)
                .pop();

        if (msg.getObjectType() == ObjectType.DATA) {

            // Dynamic schemas require different validation logic

            ctx = ctx.push(MIS_SCHEMA)
                    .apply(CommonValidators::required)
                    .applyIf(!msg.getDynamic(), SchemaValidator::schema, SchemaDefinition.class)
                    .applyIf(msg.getDynamic(), SchemaValidator::dynamicSchema, SchemaDefinition.class)
                    .pop();
        }
        else if (msg.getObjectType() == ObjectType.FILE) {

            ctx = ctx.push(MIS_FILE_TYPE)
                    .apply(CommonValidators::required)
                    .apply(FileValidator::fileType, FileType.class)
                    .pop();
        }
        else {

            ctx = ctx.push(MIS_OBJECT_TYPE)
                    .error(String.format("Object type [%s] is not supported", msg.getObjectType()))
                    .pop();
        }

        ctx = ctx.push(MIS_LABEL)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::labelLengthLimit)
                .pop();

        ctx = ctx.pushMap(MIS_INPUT_PROPS)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::standardProps)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext modelOutputSchema(ModelOutputSchema msg, ValidationContext ctx) {

        ctx = ctx.push(MOS_OBJECT_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, ObjectType.class)
                .pop();

        if (msg.getObjectType() == ObjectType.DATA) {

            // Dynamic schemas require different validation logic

            ctx = ctx.push(MOS_SCHEMA)
                    .apply(CommonValidators::required)
                    .applyIf(!msg.getDynamic(), SchemaValidator::schema, SchemaDefinition.class)
                    .applyIf(msg.getDynamic(), SchemaValidator::dynamicSchema, SchemaDefinition.class)
                    .pop();
        }
        else if (msg.getObjectType() == ObjectType.FILE) {

            ctx = ctx.push(MOS_FILE_TYPE)
                    .apply(CommonValidators::required)
                    .apply(FileValidator::fileType, FileType.class)
                    .pop();
        }
        else {

            ctx = ctx.push(MOS_OBJECT_TYPE)
                    .error(String.format("Object type [%s] is not supported", msg.getObjectType()))
                    .pop();
        }

        ctx = ctx.push(MOS_LABEL)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::labelLengthLimit)
                .pop();

        ctx = ctx.pushMap(MOS_OUTPUT_PROPS)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::standardProps)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext modelResource(ModelResource msg, ValidationContext ctx) {

        var MODEL_RESOURCE_TYPES = List.of(
                ResourceType.EXTERNAL_STORAGE, ResourceType.EXTERNAL_SYSTEM);

        ctx = ctx.push(MR_RESOURCE_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, ResourceType.class)
                .apply(CommonValidators::allowedEnums, ResourceType.class, MODEL_RESOURCE_TYPES)
                .pop();

        ctx = ctx.push(MR_PROTOCOL)
                .applyIfElse(msg.getResourceType() == ResourceType.EXTERNAL_SYSTEM,
                        CommonValidators::required,
                        CommonValidators::optional)
                .apply(CommonValidators::identifier)
                .apply(CommonValidators::notTracReserved)
                .pop();

        ctx = ctx.push(MR_SUB_PROTOCOL)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::identifier)
                .apply(CommonValidators::notTracReserved)
                .pop();

        ctx = ctx.push(MR_SYSTEM)
                .applyIfElse(msg.getResourceType() == ResourceType.EXTERNAL_SYSTEM,
                        CommonValidators::required,
                        CommonValidators::omitted)
                .apply(ModelValidator::modelSystemDetails, ModelSystemDetails.class)
                .pop();

        ctx = ctx.push(MR_LABEL)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::labelLengthLimit)
                .pop();

        ctx = ctx.pushMap(MR_RESOURCE_PROPS)
                .apply(CommonValidators::optional)
                .apply(CommonValidators::standardProps)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext modelSystemDetails(ModelSystemDetails msg, ValidationContext ctx) {

        // Client type is a qualified class name, required when MSD is present

        return ctx.push(MSD_CLIENT_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::qualifiedIdentifier)
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
}
