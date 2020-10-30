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

package com.accenture.trac.svc.meta.test;

import com.accenture.trac.common.api.meta.TagUpdate;
import com.accenture.trac.common.metadata.*;
import com.google.protobuf.ByteString;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


public class TestData {

    public static final boolean INCLUDE_HEADER = true;
    public static final boolean NO_HEADER = false;

    public static final boolean UPDATE_TAG_VERSION = true;
    public static final boolean KEEP_ORIGINAL_TAG_VERSION = false;

    public static final String TEST_TENANT = "ACME_CORP";


    public static ObjectDefinition dummyDefinitionForType(ObjectType objectType) {

        switch (objectType) {

            case DATA: return dummyDataDef();
            case MODEL: return dummyModelDef();
            case FLOW: return dummyFlowDef();
            case JOB: return dummyJobDef();
            case FILE: return dummyFileDef();
            case CUSTOM: return dummyCustomDef();

            default:
                throw new RuntimeException("No dummy data available for object type " + objectType.name());
        }
    }

    public static Tag dummyTagForObjectType(ObjectType objectType) {

        return dummyTag(dummyDefinitionForType(objectType), INCLUDE_HEADER);
    }

    public static ObjectDefinition dummyVersionForType(ObjectDefinition definition) {

        // Not all object types have semantics defined for versioning
        // It is sometimes helpful to create versions anyway for testing
        // E.g. to test that version increments are rejected for objects that don't support versioning!

        var objectType = definition.getObjectType();

        switch (objectType) {

            case DATA: return nextDataDef(definition);
            case MODEL: return nextModelDef(definition);
            case CUSTOM: return nextCustomDef(definition);

            case FLOW:
            case JOB:
            case FILE:
                return definition;

            default:
                throw new RuntimeException("No second version available in dummy data for object type " + objectType.name());
        }
    }

    public static TagHeader newHeader(ObjectType objectType) {

        return TagHeader.newBuilder()
                .setObjectType(objectType)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1)
                .setTagVersion(1)
                .build();
    }

    public static TagHeader nextTagHeader(TagHeader priorTagHeader) {

        return priorTagHeader.toBuilder()
                .setTagVersion(priorTagHeader.getTagVersion() + 1)
                .build();
    }

    public static ObjectDefinition dummyDataDef() {

        return ObjectDefinition.newBuilder()
            .setObjectType(ObjectType.DATA)
            .setData(DataDefinition.newBuilder()
            .addStorage("test-storage")
            .setPath("path/to/test/dataset")
            .setFormat(DataFormat.CSV)
            .setSchema(TableDefinition.newBuilder()
                .addField(FieldDefinition.newBuilder()
                        .setFieldName("transaction_id")
                        .setFieldType(BasicType.STRING)
                        .setFieldOrder(1)
                        .setBusinessKey(true))
                .addField(FieldDefinition.newBuilder()
                        .setFieldName("customer_id")
                        .setFieldType(BasicType.STRING)
                        .setFieldOrder(2)
                        .setBusinessKey(true))
                .addField(FieldDefinition.newBuilder()
                        .setFieldName("order_date")
                        .setFieldType(BasicType.DATE)
                        .setFieldOrder(3)
                        .setBusinessKey(true))
                .addField(FieldDefinition.newBuilder()
                        .setFieldName("widgets_ordered")
                        .setFieldType(BasicType.INTEGER)
                        .setFieldOrder(4)
                        .setBusinessKey(true))))
            .build();
    }

    public static ObjectDefinition nextDataDef(ObjectDefinition origDef) {

        var fieldName = "extra_field_" + (origDef.getData().getSchema().getFieldCount() + 1);

        return origDef.toBuilder()
                .setData(origDef.getData()
                .toBuilder()
                .setSchema(origDef.getData().getSchema().toBuilder()
                    .addField(FieldDefinition.newBuilder()
                    .setFieldName(fieldName)
                    .setFieldOrder(origDef.getData().getSchema().getFieldCount())
                    .setFieldType(BasicType.FLOAT)
                    .setFieldLabel("We got an extra field!")
                    .setFormatCode("PERCENT"))))
                .build();
    }

    public static ObjectDefinition dummyModelDef() {

        return ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setModel(ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("trac-test-repo")
                .setRepositoryVersion("trac-test-repo-1.2.3-RC4")
                .setPath("src/main/python")
                .setEntryPoint("trac_test.test1.SampleModel1")
                .putParam("param1", ModelParameter.newBuilder().setParamType(TypeSystem.descriptor(BasicType.STRING)).build())
                .putParam("param2", ModelParameter.newBuilder().setParamType(TypeSystem.descriptor(BasicType.INTEGER)).build())
                .putInput("input1", TableDefinition.newBuilder()
                        .addField(FieldDefinition.newBuilder()
                                .setFieldName("field1")
                                .setFieldType(BasicType.DATE))
                        .addField(FieldDefinition.newBuilder()
                                .setFieldName("field2")
                                .setBusinessKey(true)
                                .setFieldType(BasicType.DECIMAL)
                                .setFieldLabel("A display name")
                                .setCategorical(true)
                                .setFormatCode("GBP"))
                        .build())
                .putOutput("output1", TableDefinition.newBuilder()
                        .addField(FieldDefinition.newBuilder()
                                .setFieldName("checksum_field")
                                .setFieldType(BasicType.DECIMAL))
                        .build()))
                .build();
    }

    public static ObjectDefinition nextModelDef(ObjectDefinition origDef) {

        return origDef.toBuilder()
                .setModel(origDef.getModel()
                .toBuilder()
                .putParam("param3", ModelParameter.newBuilder().setParamType(TypeSystem.descriptor(BasicType.DATE)).build()))
                .build();
    }

    public static ObjectDefinition dummyFlowDef() {

        return ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.FLOW)
                .setFlow(FlowDefinition.newBuilder()
                .putNode("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                .putNode("main_model", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE).build())
                .putNode("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())
                .addEdge(FlowEdge.newBuilder()
                        .setHead(FlowSocket.newBuilder().setNode("main_model").setSocket("input_1"))
                        .setTail(FlowSocket.newBuilder().setNode("input_1")))
                .addEdge(FlowEdge.newBuilder()
                        .setHead(FlowSocket.newBuilder().setNode("output_1"))
                        .setTail(FlowSocket.newBuilder().setNode("main_model").setSocket("output_1"))))
                .build();
    }

    public static ObjectDefinition dummyJobDef() {

        // Job will be invalid because the model ID it points to does not exist!
        // Ok for e.g. DAL testing, but will fail metadata validation

        var targetSelector = TagSelector.newBuilder()
                .setObjectType(ObjectType.FLOW)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(1);

        return ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.JOB)
                .setJob(JobDefinition.newBuilder()
                .setJobType(JobType.RUN_MODEL)
                .setTarget(targetSelector))
                .build();
    }

    public static ObjectDefinition dummyFileDef() {

        return ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.FILE)
                .setFile(FileDefinition.newBuilder()
                .addStorage("test-storage")
                .setStoragePath("<preallocated_id>/contents/magic_template.xlsx")
                .setName("magic_template")
                .setExtension("docx")
                .setSize(45285)
                .setMimeType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
                .build();
    }

    public static ObjectDefinition dummyCustomDef() {

        var jsonReportDef = "{ reportType: 'magic', mainGraph: { content: 'more_magic' } }";

        return ObjectDefinition.newBuilder()
                .setObjectType(ObjectType.CUSTOM)
                .setCustom(CustomDefinition.newBuilder()
                .setCustomType("REPORT")
                .setCustomSchemaVersion(2)
                .setCustomData(ByteString.copyFromUtf8(jsonReportDef)))
                .build();
    }

    public static ObjectDefinition nextCustomDef(ObjectDefinition origDef) {

        var ver = UUID.randomUUID();
        var jsonReportDef = "{ reportType: 'magic', mainGraph: { content: 'more_magic_" + ver + " ' } }";

        return origDef.toBuilder()
                .setCustom(origDef.getCustom()
                .toBuilder()
                .setCustomData(ByteString.copyFromUtf8(jsonReportDef)))
                .build();
    }

    public static Tag dummyTag(ObjectDefinition definition, boolean includeHeader) {

        var tag = Tag.newBuilder()
                .setDefinition(definition)
                .putAttr("dataset_key", MetadataCodec.encodeValue("widget_orders"))
                .putAttr("widget_type", MetadataCodec.encodeValue("non_standard_widget"));

        if (includeHeader) {
            var header = newHeader(definition.getObjectType());
            return tag.setHeader(header).build();
        }
        else
            return tag.build();
    }

    public static Map<String, Value> dummyAttrs() {

        var attrs = new HashMap<String, Value>();
        attrs.put("dataset_key", MetadataCodec.encodeValue("widget_orders"));
        attrs.put("widget_type", MetadataCodec.encodeValue("non_standard_widget"));

        return attrs;
    }

    public static List<TagUpdate> tagUpdatesForAttrs(Map<String, Value> attrs) {

        return attrs.entrySet().stream()
                .map(entry -> TagUpdate.newBuilder()
                .setAttrName(entry.getKey())
                .setValue(entry.getValue()).build())
                .collect(Collectors.toList());
    }

    public static Tag tagForNextObject(Tag previous, ObjectDefinition obj, boolean includeHeader) {

        var newTag = previous.toBuilder()
                .setDefinition(obj)
                .putAttr("extra_attr", Value.newBuilder()
                        .setType(TypeSystem.descriptor(BasicType.STRING))
                        .setStringValue("A new descriptive value")
                        .build());

        if (includeHeader) {

            var header = previous.getHeader().toBuilder()
                    .setObjectVersion(previous.getHeader().getObjectVersion() + 1)
                    .setTagVersion(1)
                    .build();

            return newTag.setHeader(header).build();
        }
        else
            return newTag.clearHeader().build();
    }

    public static Tag nextTag(Tag previous, boolean updateTagVersion) {

        var updatedTag = previous.toBuilder()
                .putAttr("extra_attr", Value.newBuilder()
                .setType(TypeSystem.descriptor(BasicType.STRING))
                .setStringValue("A new descriptive value")
                .build());

        if (updateTagVersion == KEEP_ORIGINAL_TAG_VERSION)
            return updatedTag.build();

        var nextHeader = nextTagHeader(previous.getHeader());
        return updatedTag.setHeader(nextHeader).build();
    }

    public static Tag addMultiValuedAttr(Tag tag) {

        var dataClassification = MetadataCodec.encodeArrayValue(
                List.of("pii", "bcbs239", "confidential"),
                TypeSystem.descriptor(BasicType.STRING));

        return tag.toBuilder()
                .putAttr("data_classification", dataClassification)
                .build();
    }

    public static <T> T unwrap(CompletableFuture<T> future) throws Exception {

        try {
            return future.get();
        }
        catch (ExecutionException e) {
            var cause = e.getCause();
            if (cause instanceof Exception)
                throw (Exception) cause;
            throw e;
        }
    }


    // Create Java objects according to the TRAC type system

    public static Object objectOfType(BasicType basicType) {

        switch (basicType) {

            case BOOLEAN: return true;
            case INTEGER: return (long) 42;
            case FLOAT: return Math.PI;
            case DECIMAL: return new BigDecimal("1234.567");
            case STRING: return "the_droids_you_are_looking_for";
            case DATE: return LocalDate.now();

            // Metadata datetime attrs have microsecond precision
            case DATETIME:
                var dateTime = OffsetDateTime.now(ZoneOffset.UTC);
                return truncateMicrosecondPrecision(dateTime);

            default:
                throw new RuntimeException("Test object not available for basic type " + basicType.toString());
        }
    }

    public static Object objectOfDifferentType(BasicType basicType) {

        if (basicType == BasicType.STRING)
            return objectOfType(BasicType.INTEGER);
        else
            return objectOfType(BasicType.STRING);
    }

    public static Object differentObjectOfSameType(BasicType basicType, Object originalObject) {

        switch (basicType) {

            case BOOLEAN: return ! ((Boolean) originalObject);
            case INTEGER: return ((Long) originalObject) + 1L;
            case FLOAT: return ((Double) originalObject) * 2.0D;
            case DECIMAL: return ((BigDecimal) originalObject).multiply(new BigDecimal(2));
            case STRING: return originalObject.toString() + " and friends";
            case DATE: return ((LocalDate) originalObject).plusDays(1);
            case DATETIME: return ((OffsetDateTime) originalObject).plusHours(1);

            default:
                throw new RuntimeException("Test object not available for basic type " + basicType.toString());
        }
    }

    public static OffsetDateTime truncateMicrosecondPrecision(OffsetDateTime dateTime) {

        int precision = 6;

        var nanos = dateTime.getNano();
        var nanoPrecision = (int) Math.pow(10, 9 - precision);
        var truncatedNanos = (nanos / nanoPrecision) * nanoPrecision;
        return dateTime.withNano(truncatedNanos);
    }

    public static TagSelector selectorForTag(TagHeader tagHeader) {

        return TagSelector.newBuilder()
                .setObjectType(tagHeader.getObjectType())
                .setObjectId(tagHeader.getObjectId())
                .setObjectVersion(tagHeader.getObjectVersion())
                .setTagVersion(tagHeader.getTagVersion())
                .build();
    }

    public static TagSelector selectorForTag(Tag tag) {

        return selectorForTag(tag.getHeader());
    }
}
