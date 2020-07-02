package com.accenture.trac.svc.meta.test;

import com.accenture.trac.common.metadata.*;
import com.google.protobuf.ByteString;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


public class TestData {

    public static final String TEST_TENANT = "ACME_CORP";

    public static ObjectDefinition dummyDefinitionForType(ObjectType objectType) {

        switch (objectType) {

            case DATA: return dummyDataDef();
            case MODEL: return dummyModelDef();
            case FILE: return dummyFileDef();
            case CUSTOM: return dummyCustomDef();

            default:
                throw new RuntimeException("No dummy data available for object type " + objectType.name());
        }
    }

    public static ObjectDefinition dummyDataDef() {

        return ObjectDefinition.newBuilder()
            .setHeader(ObjectHeader.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(MetadataCodec.encode(UUID.randomUUID()))
                .setObjectVersion(1))
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
                        .setBusinessKey(true))))
            .build();
    }

    public static ObjectDefinition nextDataDef(ObjectDefinition origDef) {

        if (origDef.getHeader().getObjectType() != ObjectType.DATA || !origDef.hasData())
            throw new RuntimeException("Original object is not a valid data definition");

        return origDef.toBuilder()
                .mergeHeader(origDef.getHeader()
                        .toBuilder()
                        .setObjectVersion(origDef.getHeader().getObjectVersion() + 1)
                        .build())
                .setData(origDef.getData()
                        .toBuilder()
                        .mergeSchema(origDef.getData().getSchema().toBuilder()
                                .addField(FieldDefinition.newBuilder()
                                .setFieldName("extra_field")
                                .setFieldOrder(origDef.getData().getSchema().getFieldCount())
                                .setFieldType(PrimitiveType.FLOAT)
                                .setFieldLabel("We got an extra field!")
                                .setFormatCode("PERCENT")
                                .build()).build()))
                .build();
    }

    public static ObjectDefinition dummyModelDef() {

        return ObjectDefinition.newBuilder()
                .setHeader(ObjectHeader.newBuilder()
                        .setObjectType(ObjectType.MODEL)
                        .setObjectId(MetadataCodec.encode(UUID.randomUUID()))
                        .setObjectVersion(1))
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
                        .build()))
                .build();
    }

    public static ObjectDefinition nextModelDef(ObjectDefinition origDef) {

        if (origDef.getHeader().getObjectType() != ObjectType.MODEL || !origDef.hasModel())
            throw new RuntimeException("Original object is not a valid model definition");

        return origDef.toBuilder()
                .mergeHeader(origDef.getHeader()
                        .toBuilder()
                        .setObjectVersion(origDef.getHeader().getObjectVersion() + 1)
                        .build())
                .setModel(origDef.getModel()
                        .toBuilder()
                        .putParam("param3", ModelParameter.newBuilder().setParamType(PrimitiveType.DATE).build()))
                .build();
    }

    public static ObjectDefinition dummyFileDef() {

        return ObjectDefinition.newBuilder()
                .setHeader(ObjectHeader.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setObjectId(MetadataCodec.encode(UUID.randomUUID()))
                        .setObjectVersion(1))
                .setFile(FileDefinition.newBuilder()
                        .addStorage("test-storage")
                        .setStoragePath("<preallocated_id>/contents/magic_template.xlsx")
                        .setName("magic_template")
                        .setExtension("docx")
                        .setSize(45285)
                        .setMimeType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                        .build())
                .build();
    }

    public static ObjectDefinition dummyCustomDef() {

        String jsonReportDef = "{ reportType: 'magic', mainGraph: { content: 'more_magic' } }";

        return ObjectDefinition.newBuilder()
                .setHeader(ObjectHeader.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setObjectId(MetadataCodec.encode(UUID.randomUUID()))
                        .setObjectVersion(1))
                .setCustom(CustomDefinition.newBuilder()
                        .setCustomType("REPORT")
                        .setCustomSchemaVersion(2)
                        .setCustomData(ByteString.copyFromUtf8(jsonReportDef))
                        .build())
                .build();
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
            return dummyTag(dummyDataDef());

        if (objectType == ObjectType.MODEL)
            return dummyTag(dummyModelDef());

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
