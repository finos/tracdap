package com.accenture.trac.svc.meta.test;

import com.accenture.trac.common.metadata.*;
import com.google.protobuf.ByteString;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


public class TestData {

    public static final boolean INCLUDE_HEADER = true;
    public static final boolean NO_HEADER = false;

    public static final boolean UPDATE_HEADER = true;
    public static final boolean KEEP_ORIGINAL_HEADER = false;

    public static final String TEST_TENANT = "ACME_CORP";

    public static ObjectDefinition dummyDefinitionForType(ObjectType objectType, boolean includeHeader) {

        switch (objectType) {

            case DATA: return dummyDataDef(includeHeader);
            case MODEL: return dummyModelDef(includeHeader);
            case FLOW: return dummyFlowDef(includeHeader);
            case JOB: return dummyJobDef(includeHeader);
            case FILE: return dummyFileDef(includeHeader);
            case CUSTOM: return dummyCustomDef(includeHeader);

            default:
                throw new RuntimeException("No dummy data available for object type " + objectType.name());
        }
    }

    public static ObjectDefinition dummyVersionForType(ObjectDefinition definition, boolean updateHeader) {

        var objectType = definition.getHeader().getObjectType();

        switch (objectType) {

            case DATA: return nextDataDef(definition, updateHeader);
            case MODEL: return nextModelDef(definition, updateHeader);
            case CUSTOM: return nextCustomDef(definition, updateHeader);

            default:
                throw new RuntimeException("No second version available in dummy data for object type " + objectType.name());
        }
    }

    public static ObjectDefinition newObjectHeader(ObjectType objectType, ObjectDefinition.Builder def, boolean includeHeader) {

        if (includeHeader == NO_HEADER)
            return def.build();

        else
            return def
                .setHeader(ObjectHeader.newBuilder()
                .setObjectType(objectType)
                .setObjectId(MetadataCodec.encode(UUID.randomUUID()))
                .setObjectVersion(1))
                .build();
    }

    public static ObjectDefinition newVersionHeader(ObjectDefinition.Builder defUpdate, boolean updateHeader) {

        if (updateHeader == KEEP_ORIGINAL_HEADER)
            return defUpdate.build();

        else
            return defUpdate.mergeHeader(defUpdate.getHeader()
                .toBuilder()
                .setObjectVersion(defUpdate.getHeader().getObjectVersion() + 1)
                .build())
                .build();
    }

    public static ObjectDefinition dummyDataDef(boolean includeHeader) {

        var def = ObjectDefinition.newBuilder()
            .setData(DataDefinition.newBuilder()
            .addStorage("test-storage")
            .setPath("path/to/test/dataset")
            .setFormat(DataFormat.CSV)
            .setSchema(TableDefinition.newBuilder()
                .addField(FieldDefinition.newBuilder()
                        .setFieldName("transaction_id")
                        .setFieldType(PrimitiveType.STRING)
                        .setFieldOrder(1)
                        .setBusinessKey(true))
                .addField(FieldDefinition.newBuilder()
                        .setFieldName("customer_id")
                        .setFieldType(PrimitiveType.STRING)
                        .setFieldOrder(2)
                        .setBusinessKey(true))
                .addField(FieldDefinition.newBuilder()
                        .setFieldName("order_date")
                        .setFieldType(PrimitiveType.DATE)
                        .setFieldOrder(3)
                        .setBusinessKey(true))
                .addField(FieldDefinition.newBuilder()
                        .setFieldName("widgets_ordered")
                        .setFieldType(PrimitiveType.INTEGER)
                        .setFieldOrder(4)
                        .setBusinessKey(true))));

        return newObjectHeader(ObjectType.DATA, def, includeHeader);
    }

    public static ObjectDefinition nextDataDef(ObjectDefinition origDef, boolean updateHeader) {

        if (origDef.getHeader().getObjectType() != ObjectType.DATA || !origDef.hasData())
            throw new RuntimeException("Original object is not a valid data definition");

        var defUpdate = origDef.toBuilder()
                .setData(origDef.getData()
                .toBuilder()
                .mergeSchema(origDef.getData().getSchema().toBuilder()
                    .addField(FieldDefinition.newBuilder()
                    .setFieldName("extra_field")
                    .setFieldOrder(origDef.getData().getSchema().getFieldCount())
                    .setFieldType(PrimitiveType.FLOAT)
                    .setFieldLabel("We got an extra field!")
                    .setFormatCode("PERCENT")
                    .build()).build()));

        return newVersionHeader(defUpdate, updateHeader);
    }

    public static ObjectDefinition dummyModelDef(boolean includeHeader) {

        var def = ObjectDefinition.newBuilder()
                .setModel(ModelDefinition.newBuilder()
                .setLanguage("python")
                .setRepository("trac-test-repo")
                .setRepositoryVersion("trac-test-repo-1.2.3-RC4")
                .setPath("src/main/python")
                .setEntryPoint("trac_test.test1.SampleModel1")
                .putParam("param1", ModelParameter.newBuilder().setParamType(PrimitiveType.STRING).build())
                .putParam("param2", ModelParameter.newBuilder().setParamType(PrimitiveType.INTEGER).build())
                .putInput("input1", TableDefinition.newBuilder()
                        .addField(FieldDefinition.newBuilder()
                                .setFieldName("field1")
                                .setFieldType(PrimitiveType.DATE))
                        .addField(FieldDefinition.newBuilder()
                                .setFieldName("field2")
                                .setBusinessKey(true)
                                .setFieldType(PrimitiveType.DECIMAL)
                                .setFieldLabel("A display name")
                                .setCategorical(true)
                                .setFormatCode("GBP"))
                        .build())
                .putOutput("output1", TableDefinition.newBuilder()
                        .addField(FieldDefinition.newBuilder()
                                .setFieldName("checksum_field")
                                .setFieldType(PrimitiveType.DECIMAL))
                        .build()));

        return newObjectHeader(ObjectType.MODEL, def, includeHeader);
    }

    public static ObjectDefinition nextModelDef(ObjectDefinition origDef, boolean updateHeader) {

        if (origDef.getHeader().getObjectType() != ObjectType.MODEL || !origDef.hasModel())
            throw new RuntimeException("Original object is not a valid model definition");

        var defUpdate = origDef.toBuilder()
                .setModel(origDef.getModel()
                .toBuilder()
                .putParam("param3", ModelParameter.newBuilder().setParamType(PrimitiveType.DATE).build()));

        return newVersionHeader(defUpdate, updateHeader);
    }

    public static ObjectDefinition dummyFlowDef(boolean includeHeader) {

        var def = ObjectDefinition.newBuilder()
                .setFlow(FlowDefinition.newBuilder()
                    .putNode("input_1", FlowNode.newBuilder().setNodeType(FlowNodeType.INPUT_NODE).build())
                    .putNode("main_model", FlowNode.newBuilder().setNodeType(FlowNodeType.MODEL_NODE).build())
                    .putNode("output_1", FlowNode.newBuilder().setNodeType(FlowNodeType.OUTPUT_NODE).build())
                    .addEdge(FlowEdge.newBuilder()
                            .setStart(FlowSocket.newBuilder().setNode("input_1"))
                            .setEnd(FlowSocket.newBuilder().setNode("main_model").setSocket("input_1")))
                    .addEdge(FlowEdge.newBuilder()
                            .setStart(FlowSocket.newBuilder().setNode("main_model").setSocket("output_1"))
                            .setEnd(FlowSocket.newBuilder().setNode("output_1"))));

        return newObjectHeader(ObjectType.FLOW, def, includeHeader);
    }

    public static ObjectDefinition dummyJobDef(boolean includeHeader) {

        // Job will be invalid because the model ID it points to does not exist!
        // Ok for e.g. DAL testing, but will fail metadata validation

        var def = ObjectDefinition.newBuilder()
                .setHeader(ObjectHeader.newBuilder()
                    .setObjectType(ObjectType.JOB)
                    .setObjectId(MetadataCodec.encode(UUID.randomUUID()))
                    .setObjectVersion(1))
                .setJob(JobDefinition.newBuilder()
                        .setJobType(JobType.RUN_MODEL)
                        .setTargetId(MetadataCodec.encode(UUID.randomUUID())));

        return newObjectHeader(ObjectType.JOB, def, includeHeader);
    }

    public static ObjectDefinition dummyFileDef(boolean includeHeader) {

        var def = ObjectDefinition.newBuilder()
                .setFile(FileDefinition.newBuilder()
                        .addStorage("test-storage")
                        .setStoragePath("<preallocated_id>/contents/magic_template.xlsx")
                        .setName("magic_template")
                        .setExtension("docx")
                        .setSize(45285)
                        .setMimeType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                        .build());

        return newObjectHeader(ObjectType.FILE, def, includeHeader);
    }

    public static ObjectDefinition dummyCustomDef(boolean includeHeader) {

        var jsonReportDef = "{ reportType: 'magic', mainGraph: { content: 'more_magic' } }";

        var def = ObjectDefinition.newBuilder()
                .setHeader(ObjectHeader.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setObjectId(MetadataCodec.encode(UUID.randomUUID()))
                        .setObjectVersion(1))
                .setCustom(CustomDefinition.newBuilder()
                        .setCustomType("REPORT")
                        .setCustomSchemaVersion(2)
                        .setCustomData(ByteString.copyFromUtf8(jsonReportDef))
                        .build());

        return newObjectHeader(ObjectType.CUSTOM, def, includeHeader);
    }

    public static ObjectDefinition nextCustomDef(ObjectDefinition origDef, boolean updateHeader) {

        if (origDef.getHeader().getObjectType() != ObjectType.CUSTOM || !origDef.hasCustom())
            throw new RuntimeException("Original object is not a valid custom definition");

        var ver = origDef.getHeader().getObjectVersion();
        var jsonReportDef = "{ reportType: 'magic', mainGraph: { content: 'more_magic_" + ver + " ' } }";

        var defUpdate = origDef.toBuilder()
                .setCustom(origDef.getCustom()
                .toBuilder()
                .setCustomData(ByteString.copyFromUtf8(jsonReportDef)));

        return newVersionHeader(defUpdate, updateHeader);
    }

    public static Tag dummyTag(ObjectDefinition definition) {

        return Tag.newBuilder()
                .setDefinition(definition)
                .setTagVersion(1)
                .putAttr("dataset_key", PrimitiveValue.newBuilder()
                        .setType(PrimitiveType.STRING)
                        .setStringValue("widget_orders")
                        .build())
                .putAttr("widget_type", PrimitiveValue.newBuilder()
                        .setType(PrimitiveType.STRING)
                        .setStringValue("non_standard_widget")
                        .build())
                .build();
    }

    public static Tag nextTag(Tag previous) {

        return previous.toBuilder()
                .setTagVersion(previous.getTagVersion() + 1)
                .putAttr("extra_attr", PrimitiveValue.newBuilder()
                        .setType(PrimitiveType.STRING)
                        .setStringValue("A new descriptive value")
                        .build())
                .build();
    }

    public static Tag dummyTagForObjectType(ObjectType objectType) {

        if (objectType == ObjectType.DATA)
            return dummyTag(dummyDataDef(INCLUDE_HEADER));

        if (objectType == ObjectType.MODEL)
            return dummyTag(dummyModelDef(INCLUDE_HEADER));

        throw new RuntimeException("Object type not supported for test data: " + objectType.name());
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

}
