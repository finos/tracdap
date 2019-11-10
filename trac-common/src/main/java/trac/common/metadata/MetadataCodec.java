package trac.common.metadata;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

import java.util.UUID;


public class MetadataCodec {

    public static trac.common.metadata.UUID encode(java.util.UUID uuid) {

        return trac.common.metadata.UUID.newBuilder()
                .setHi(uuid.getMostSignificantBits())
                .setLo(uuid.getLeastSignificantBits())
                .build();
    }

    public static java.util.UUID decode(trac.common.metadata.UUID uuid) {

        return new UUID(uuid.getHi(), uuid.getLo());
    }

    public static MessageLite decode(ObjectType objectType, byte[] bytes) throws InvalidProtocolBufferException {

        switch (objectType) {
            case NONE_OBJECT: return null;
            case DATA: return DataDefinition.parseFrom(bytes);
            case MODEL: return ModelDefinition.parseFrom(bytes);
            case FLOW: return null;
            case JOB: return null;
            case JOB_OUTPUT: return null;
            case CUSTOM: return null;
            case FILE: return null;
            case UNRECOGNIZED: return null;
            default: return null;
            // TODO: Not implemented exceptions
        }

        // TODO: Handler errors here? How to do?
    }

    public static Tag.Builder tagForDefinition(ObjectType objectType, MessageLite objectDef) {

        return tagForDefinition(Tag.newBuilder(), objectType, objectDef);
    }

    public static Tag.Builder tagForDefinition(Tag.Builder tag, ObjectType objectType, MessageLite objectDef) {

        switch (objectType) {

            case DATA: return tag.setDataDefinition((DataDefinition) objectDef);
            case MODEL: return tag.setModelDefinition((ModelDefinition) objectDef);
//
//            case NONE_OBJECT: return null;
//            case FLOW: return null;
//            case JOB: return null;
//            case JOB_OUTPUT: return null;
//            case CUSTOM: return null;
//            case FILE: return null;
//            case UNRECOGNIZED: return null;
//            default: return null;
            // TODO: Not implemented exceptions
        }

        // TODO: Handler errors here? How to do?
        throw new RuntimeException("");
    }

    public static MessageLite definitionForTag(TagOrBuilder tag) {

        var objectType = tag.getHeader().getObjectType();
        return definitionForTag(tag, objectType);
    }

    public static MessageLite definitionForTag(TagOrBuilder tag, ObjectType objectType) {

        switch (objectType) {

            case DATA: return tag.getDataDefinition();
            case MODEL: return tag.getModelDefinition();
//            case FLOW: return null;
//            case JOB: return null;
//            case JOB_OUTPUT: return null;
//            case CUSTOM: return null;
//            case FILE: return null;
//            case UNRECOGNIZED: return null;
//            case NONE_OBJECT: return null;
//            default: return null;
            // TODO: Not implemented exceptions
        }

        // TODO: Handler errors here? How to do?
        throw new RuntimeException("");
    }

}
