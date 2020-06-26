package com.accenture.trac.svc.meta.dal;

import com.accenture.trac.common.metadata.*;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


public class MetadataDalTestData {

    public static final String TEST_TENANT = "ACME_CORP";

    static ObjectDefinition dummyDataDef() {

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

    static ObjectDefinition nextDataDef(ObjectDefinition origDef) {

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

    static ObjectDefinition dummyModelDef() {

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
                .putParam("param1", Parameter.newBuilder().setParamType(PrimitiveType.STRING).build())
                .putParam("param2", Parameter.newBuilder().setParamType(PrimitiveType.INTEGER).build())
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

    static ObjectDefinition nextModelDef(ObjectDefinition origDef) {

        if (origDef.getHeader().getObjectType() != ObjectType.MODEL || !origDef.hasModel())
            throw new RuntimeException("Original object is not a valid model definition");

        return origDef.toBuilder()
                .mergeHeader(origDef.getHeader()
                        .toBuilder()
                        .setObjectVersion(origDef.getHeader().getObjectVersion() + 1)
                        .build())
                .setModel(origDef.getModel()
                        .toBuilder()
                        .putParam("param3", Parameter.newBuilder().setParamType(PrimitiveType.DATE).build()))
                .build();
    }

    static Tag dummyTag(ObjectDefinition definition) {

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
