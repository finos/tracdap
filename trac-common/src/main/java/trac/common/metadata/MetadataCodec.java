package trac.common.metadata;

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

}
