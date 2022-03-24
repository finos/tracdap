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

import com.google.protobuf.Message;
import org.finos.tracdap.common.exception.EInputValidation;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.common.metadata.MetadataConstants;
import org.finos.tracdap.common.validation.Validator;
import org.finos.tracdap.metadata.DatetimeValue;
import org.finos.tracdap.metadata.ObjectType;
import org.finos.tracdap.metadata.TagHeader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;


class ObjectIdTest {

    static Validator validator;

    @BeforeAll
    static void setupValidator() {

        validator = new Validator();
    }

    static <TMsg extends Message> void expectValid(TMsg msg) {

        Assertions.assertDoesNotThrow(
                () -> validator.validateFixedObject(msg),
                "Validation failed for a valid message");
    }

    static <TMsg extends Message> void expectInvalid(TMsg msg) {

        Assertions.assertThrows(EInputValidation.class,
                () -> validator.validateFixedObject(msg),
                "Validation passed for an invalid message");
    }

    @Test
    void tagHeader_ok1() {

        var timestamp = Instant.now();

        var header = TagHeader.newBuilder()
                .setObjectType(ObjectType.SCHEMA)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        expectValid(header);
    }

    @Test
    void tagHeader_ok2() {

        var timestamp = Instant.now();

        var header = TagHeader.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION + 3)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp.plusSeconds(600)))
                .build();

        expectValid(header);
    }

    @Test
    void tagHeader_ok3() {

        var timestamp = Instant.now();

        var header = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION + 2)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION + 3)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp.plusSeconds(600)))
                .build();

        expectValid(header);
    }

    @Test
    void tagHeader_typeMissing() {

        var timestamp = Instant.now();

        var header = TagHeader.newBuilder()
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        expectInvalid(header);
    }

    @Test
    void tagHeader_idMissing() {

        var timestamp = Instant.now();

        var header = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        expectInvalid(header);
    }

    @Test
    void tagHeader_idEmpty() {

        var timestamp = Instant.now();

        var header = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId("")
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        expectInvalid(header);
    }

    @Test
    void tagHeader_idBlank() {

        var timestamp = Instant.now();

        var header = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId("   ")
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        expectInvalid(header);
    }

    @Test
    void tagHeader_idInvalid() {

        var timestamp = Instant.now();

        var header = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId("not_a_valid_id")
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        expectInvalid(header);
    }

    @Test
    void tagHeader_objectVersionMissing() {

        var timestamp = Instant.now();

        var header = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        expectInvalid(header);
    }

    @Test
    void tagHeader_objectVersionNegative() {

        var timestamp = Instant.now();

        var header = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(-1)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        expectInvalid(header);
    }

    @Test
    void tagHeader_objectTimeMissing() {

        var timestamp = Instant.now();

        var header = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        expectInvalid(header);
    }

    @Test
    void tagHeader_objectTimeInvalid() {

        var timestamp = Instant.now();

        var header = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(DatetimeValue.newBuilder().setIsoDatetime("not_a_valid_datetime"))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        expectInvalid(header);
    }

    @Test
    void tagHeader_tagVersionMissing() {

        var timestamp = Instant.now();

        var header = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        expectInvalid(header);
    }

    @Test
    void tagHeader_tagVersionNegative() {

        var timestamp = Instant.now();

        var header = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(-1)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        expectInvalid(header);
    }

    @Test
    void tagHeader_tagTimeMissing() {

        var timestamp = Instant.now();

        var header = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .build();

        expectInvalid(header);
    }

    @Test
    void tagHeader_tagTimeInvalid() {

        var timestamp = Instant.now();

        var header = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .setTagTimestamp(DatetimeValue.newBuilder().setIsoDatetime("not_a_valid_datetime"))
                .build();

        expectInvalid(header);
    }

    @Test
    void tagHeader_timestampV1T1() {

        // V1, T1 -> tag timestamp = obj timestamp

        var timestamp = Instant.now();

        var header1 = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp.plusSeconds(1)))
                .build();

        expectInvalid(header1);

        var header2 = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        expectValid(header2);

        var header3 = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp.minusSeconds(1)))
                .build();

        expectInvalid(header3);
    }

    @Test
    void tagHeader_timestampV1T2() {

        // V1, T2 -> tag timestamp > obj timestamp

        var timestamp = Instant.now();

        var header1 = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION + 1)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp.plusSeconds(1)))
                .build();

        expectValid(header1);

        var header2 = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION + 1)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        expectInvalid(header2);

        var header3 = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION + 1)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp.minusSeconds(1)))
                .build();

        expectInvalid(header3);
    }

    @Test
    void tagHeader_timestampV2T1() {

        // V2, T1 -> tag timestamp = obj timestamp

        var timestamp = Instant.now();

        var header1 = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION + 1)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp.plusSeconds(1)))
                .build();

        expectInvalid(header1);

        var header2 = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION + 1)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        expectValid(header2);

        var header3 = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION + 1)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION )
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp.minusSeconds(1)))
                .build();

        expectInvalid(header3);
    }

    @Test
    void tagHeader_timestampV2T2() {

        // V2, T2 -> tag timestamp > obj timestamp

        var timestamp = Instant.now();

        var header1 = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION + 1)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION + 1)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp.plusSeconds(1)))
                .build();

        expectValid(header1);

        var header2 = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION + 1)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION + 1)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .build();

        expectInvalid(header2);

        var header3 = TagHeader.newBuilder()
                .setObjectType(ObjectType.MODEL)
                .setObjectId(UUID.randomUUID().toString())
                .setObjectVersion(MetadataConstants.OBJECT_FIRST_VERSION + 1)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(timestamp))
                .setTagVersion(MetadataConstants.TAG_FIRST_VERSION + 1)
                .setTagTimestamp(MetadataCodec.encodeDatetime(timestamp.minusSeconds(1)))
                .build();

        expectInvalid(header3);
    }
}
