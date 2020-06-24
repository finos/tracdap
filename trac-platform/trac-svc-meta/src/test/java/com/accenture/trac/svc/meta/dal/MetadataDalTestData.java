package com.accenture.trac.svc.meta.dal;

import com.accenture.trac.common.metadata.*;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


public class MetadataDalTestData {

    public static final String TEST_TENANT = "ACME_CORP";

    static DataDefinition dummyDataDef() {

        return DataDefinition.newBuilder()
                .setHeader(ObjectHeader.newBuilder()
                        .setObjectType(ObjectType.DATA)
                        .setId(MetadataCodec.encode(UUID.randomUUID()))
                        .setVersion(1))
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
                                .setBusinessKey(true)))
                .build();
    }

    static DataDefinition nextDataDef(DataDefinition origDef) {

        return origDef.toBuilder()
                .mergeHeader(origDef.getHeader()
                        .toBuilder()
                        .setVersion(origDef.getHeader().getVersion() + 1)
                        .build())
                .mergeSchema(origDef.getSchema().toBuilder()
                        .addField(FieldDefinition.newBuilder()
                        .setFieldName("extra_field")
                        .setFieldOrder(origDef.getSchema().getFieldCount())
                        .setFieldType(PrimitiveType.FLOAT)
                        .setFieldLabel("We got an extra field!")
                        .setFormatCode("PERCENT")
                        .build()).build())
                .build();
    }

    static ModelDefinition dummyModelDef() {

        return ModelDefinition.newBuilder()
                .setHeader(ObjectHeader.newBuilder()
                        .setObjectType(ObjectType.MODEL)
                        .setId(MetadataCodec.encode(UUID.randomUUID()))
                        .setVersion(1))
                .setLanguage("python")
                .setRepository("trac-test-repo")
                .setRepositoryVersion("trac-test-repo-1.2.3-RC4")
                .setPath("src/main/python")
                .setEntryPoint("trac_test.test1.SampleModel1")
                .putParam("param1", Parameter.newBuilder().setParamType(PrimitiveType.STRING).build())
                .putParam("param2", Parameter.newBuilder().setParamType(PrimitiveType.INTEGER).build())
                .putInput("input1", TableDefinition.newBuilder()
                        .addField(FieldDefinition.newBuilder()
                                .setFieldName("field1")
                                .setFieldType(PrimitiveType.DATE)
                                .build())
                        .addField(FieldDefinition.newBuilder()
                                .setFieldName("field2")
                                .setBusinessKey(true)
                                .setFieldType(PrimitiveType.DECIMAL)
                                .setFieldLabel("A display name")
                                .setCategorical(true)
                                .setFormatCode("GBP")
                                .build())
                        .build())
                .putOutput("output1", TableDefinition.newBuilder()
                        .addField(FieldDefinition.newBuilder()
                                .setFieldName("checksum_field")
                                .setFieldType(PrimitiveType.DECIMAL)
                                .build())
                        .build())
                .build();
    }

    static ModelDefinition nextModelDef(ModelDefinition origDef) {

        return origDef.toBuilder()
                .mergeHeader(origDef.getHeader()
                        .toBuilder()
                        .setVersion(origDef.getHeader().getVersion() + 1)
                        .build())
                .putParam("param3", Parameter.newBuilder().setParamType(PrimitiveType.DATE).build())
                .build();
    }

    static Tag dummyTag(DataDefinition dataDef) {

        return Tag.newBuilder()
                .setHeader(dataDef.getHeader())
                .setTagVersion(1)
                .setDataDefinition(dataDef)
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

    static Tag dummyTag(ModelDefinition modelDef) {

        return Tag.newBuilder()
                .setHeader(modelDef.getHeader())
                .setTagVersion(1)
                .setModelDefinition(modelDef)
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

    static Tag nextTag(Tag previous) {

        return previous.toBuilder()
                .setTagVersion(previous.getTagVersion() + 1)
                .putAttr("extra_attr", PrimitiveValue.newBuilder()
                        .setType(PrimitiveType.STRING)
                        .setStringValue("A new descriptive value")
                        .build())
                .build();
    }

    static Tag dummyTagForObjectType(ObjectType objectType) {

        if (objectType == ObjectType.DATA)
            return dummyTag(dummyDataDef());

        if (objectType == ObjectType.MODEL)
            return dummyTag(dummyModelDef());

        throw new RuntimeException("Object type not supported for test data: " + objectType.name());
    }

    static <T> T unwrap(CompletableFuture<T> future) throws Exception {

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
