/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.data.api;

import com.accenture.trac.api.FileReadRequest;
import com.accenture.trac.api.FileReadResponse;
import com.accenture.trac.api.FileWriteRequest;
import com.accenture.trac.api.MetadataReadRequest;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.common.metadata.MetadataUtil;
import com.accenture.trac.common.util.Concurrent;
import com.accenture.trac.common.util.Futures;
import com.accenture.trac.metadata.*;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static com.accenture.trac.svc.data.api.Helpers.readRequest;
import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.resultOf;
import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.waitFor;
import static org.junit.jupiter.api.Assertions.*;


public class DataApiTest_File extends DataApiTest_Base {

    // Functional test cases for file operations in the data API
    // (createFile, updateFile, readFile)

    static final List<TagUpdate> BASIC_TAG_UPDATES = List.of(
            TagUpdate.newBuilder()
                    .setAttrName("app_template")
                    .setOperation(TagOperation.CREATE_ATTR)
                    .setValue(MetadataCodec.encodeValue("template_name"))
                    .build(),
            TagUpdate.newBuilder()
                    .setAttrName("description")
                    .setOperation(TagOperation.CREATE_ATTR)
                    .setValue(MetadataCodec.encodeValue("Describes what this template does in the app"))
                    .build());

    static final ByteString BASIC_FILE_CONTENT = ByteString
            .copyFrom("Sample content\n", StandardCharsets.UTF_8);

    static final FileWriteRequest BASIC_CREATE_FILE_REQUEST = FileWriteRequest.newBuilder()
            .setTenant(TEST_TENANT)
            .addAllTagUpdates(BASIC_TAG_UPDATES)
            .setName("some_file.txt")
            .setMimeType("text/plain")
            .setSize(BASIC_FILE_CONTENT.size())
            .setContent(BASIC_FILE_CONTENT)
            .build();

    static final List<TagUpdate> BASIC_TAG_UPDATES_V2 = List.of(
            TagUpdate.newBuilder()
                    .setAttrName("description")
                    .setOperation(TagOperation.REPLACE_ATTR)
                    .setValue(MetadataCodec.encodeValue("Describes what is in the V2 template"))
                    .build());

    static final ByteString BASIC_FILE_CONTENT_V2 = ByteString
            .copyFrom("Sample content v2\nSome new updated content\n", StandardCharsets.UTF_8);

    static final FileWriteRequest BASIC_UPDATE_FILE_REQUEST = FileWriteRequest.newBuilder()
            .setTenant(TEST_TENANT)
            .addAllTagUpdates(BASIC_TAG_UPDATES_V2)
            .setName("some_file.txt")
            .setMimeType("text/plain")
            .setSize(BASIC_FILE_CONTENT_V2.size())
            .setContent(BASIC_FILE_CONTENT_V2)
            .build();


    // -----------------------------------------------------------------------------------------------------------------
    // CREATE FILE
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void testCreateFile_dataOk() throws Exception {

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var fileId = resultOf(createFile);

        // Read back content and check

        var fileReadRequest = FileReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(fileId))
                .build();

        var responseStream = Concurrent.<FileReadResponse>hub(execContext);
        var byteStream = Concurrent.map(responseStream, FileReadResponse::getContent);
        var content = Concurrent.fold(byteStream,
                ByteString::concat,
                ByteString.EMPTY);

        Helpers.serverStreaming(dataClient::readFile, fileReadRequest, responseStream);

        waitFor(TEST_TIMEOUT, content);
        Assertions.assertEquals(BASIC_FILE_CONTENT, resultOf(content));
    }

    @Test
    void testCreateFile_metadataOk() throws Exception {

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var fileId = resultOf(createFile);

        var metaReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(fileId))
                .build();

        var metaResponse = Futures.javaFuture(metaClient.readObject(metaReadRequest));
        waitFor(TEST_TIMEOUT, metaResponse);
        var tag = resultOf(metaResponse);

        // Explicitly set attrs

        Assertions.assertTrue(tag.containsAttrs("app_template"));
        Assertions.assertTrue(tag.containsAttrs("description"));
        var appTemplateAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("app_template"));
        var descriptionAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("description"));
        Assertions.assertEquals("template_name", appTemplateAttr);
        Assertions.assertEquals("Describes what this template does in the app", descriptionAttr);

        // Controlled attrs should always be set for files

        Assertions.assertTrue(tag.containsAttrs("trac_file_name"));
        Assertions.assertTrue(tag.containsAttrs("trac_file_extension"));
        Assertions.assertTrue(tag.containsAttrs("trac_file_mime_type"));
        Assertions.assertTrue(tag.containsAttrs("trac_file_size"));
        var nameAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("trac_file_name"));
        var extensionAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("trac_file_extension"));
        var mimeTypeAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("trac_file_mime_type"));
        var sizeAttr = MetadataCodec.decodeIntegerValue(tag.getAttrsOrThrow("trac_file_size"));
        Assertions.assertEquals("some_file.txt", nameAttr);
        Assertions.assertEquals("txt", extensionAttr);
        Assertions.assertEquals("text/plain", mimeTypeAttr);
        Assertions.assertEquals(BASIC_FILE_CONTENT.size(), sizeAttr);

        // Check core attributes of the file definition

        var def = tag.getDefinition();
        Assertions.assertEquals(ObjectType.FILE, def.getObjectType());

        var fileDef = def.getFile();
        Assertions.assertEquals("some_file.txt", fileDef.getName());
        Assertions.assertEquals("txt", fileDef.getExtension());
        Assertions.assertEquals("text/plain", fileDef.getMimeType());
        Assertions.assertEquals(BASIC_FILE_CONTENT.size(), fileDef.getSize());

        // Storage ID should always point to a storage object and refer to the latest object/tag version
        // This is because storage can evolve independently of logical files/data (e.g. due to retention policy)
        // Data item should always be set

        Assertions.assertEquals(ObjectType.STORAGE, fileDef.getStorageId().getObjectType());
        Assertions.assertTrue(fileDef.getStorageId().getLatestObject());
        Assertions.assertTrue(fileDef.getStorageId().getLatestTag());
        Assertions.assertFalse(fileDef.getDataItem().isBlank());
    }

    @Test
    void testCreateFile_priorVersionNotNull() throws Exception {

        // Prior version should not be set when creating a new file object

        // Create an object to use as the prior, so errors will not come because the prior does not exist
        var priorResult = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, priorResult);
        var priorId = resultOf(priorResult);

        var priorNotNull = BASIC_CREATE_FILE_REQUEST.toBuilder()
                .setPriorVersion(MetadataUtil.selectorFor(priorId))
                .build();

        var priorNotNullResult = Helpers.clientStreaming(dataClient::createFile, priorNotNull);
        waitFor(TEST_TIMEOUT, priorNotNullResult);
        var priorNotNullError = assertThrows(StatusRuntimeException.class, () -> resultOf(priorNotNullResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, priorNotNullError.getStatus().getCode());
    }

    @Test
    void testCreateFile_tagUpdateInvalid() {

        var invalidTagUpdate = TagUpdate.newBuilder()
                .setAttrName("£!£$%£$%")
                .setOperation(TagOperation.CREATE_ATTR)
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        var invalidTagRequest = BASIC_CREATE_FILE_REQUEST.toBuilder().addTagUpdates(invalidTagUpdate).build();
        var invalidTagResult = Helpers.clientStreaming(dataClient::createFile, invalidTagRequest);
        waitFor(TEST_TIMEOUT, invalidTagResult);
        var invalidTagError = assertThrows(StatusRuntimeException.class, () -> resultOf(invalidTagResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, invalidTagError.getStatus().getCode());
    }

    @Test
    void testCreateFile_tagUpdateReserved() {

        var reservedAttrs = List.of("trac_file_attr", "_file_attr", "__file_attr");

        for (var attrName: reservedAttrs) {

            var invalidTagUpdate = TagUpdate.newBuilder()
                    .setAttrName(attrName)
                    .setOperation(TagOperation.CREATE_ATTR)
                    .setValue(MetadataCodec.encodeValue("some_value"))
                    .build();

            var invalidTagRequest = BASIC_CREATE_FILE_REQUEST.toBuilder().addTagUpdates(invalidTagUpdate).build();
            var invalidTagResult = Helpers.clientStreaming(dataClient::createFile, invalidTagRequest);
            waitFor(TEST_TIMEOUT, invalidTagResult);
            var invalidTagError = assertThrows(StatusRuntimeException.class, () -> resultOf(invalidTagResult));
            assertEquals(Status.Code.INVALID_ARGUMENT, invalidTagError.getStatus().getCode());
        }
    }

    @Test
    void testCreateFile_nameMissing() {

        var noName = BASIC_CREATE_FILE_REQUEST.toBuilder().clearName().build();
        var noNameResult = Helpers.clientStreaming(dataClient::createFile, noName);
        waitFor(TEST_TIMEOUT, noNameResult);
        var noNameError = assertThrows(StatusRuntimeException.class, () -> resultOf(noNameResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, noNameError.getStatus().getCode());
    }

    @Test
    void testCreateFile_nameInvalid() {

        var invalidNames = List.of(
                ".", "/", "\\",
                "./a", ".\\a", "a/b", "a\\b", "a/", "a\\",
                "a\rb", "a\nb", "a\tb", "a\0b",
                "$£\"£$%^\0<:$%^&D'¬#FSG)",
                "asci\u0003_ctrl.dat", "nul\0char.dat", "form\ffeed.txt",
                " leading_space.txt", "trailing_space.txt ", "trailing_dot.",
                "COM1", "lpt2.txt", "trac_file.dat", "_special.txt");

        for (var name: invalidNames) {

            var badName = BASIC_CREATE_FILE_REQUEST.toBuilder().setName(name).build();
            var badNameResult = Helpers.clientStreaming(dataClient::createFile, badName);
            waitFor(TEST_TIMEOUT, badNameResult);

            var badNameError = assertThrows(
                    StatusRuntimeException.class,
                    () -> resultOf(badNameResult),
                    "Invalid file name did not fail: " + name);

            assertEquals(Status.Code.INVALID_ARGUMENT, badNameError.getStatus().getCode());
        }
    }

    @Test
    void testCreateFile_nameValid() {

        // Make sure file name validation is not rejecting valid punctuation, unicode etc.

        var validNames = List.of(
                "A name with spaces.doc",
                "Some punctuation - this is allowed (for non reserved chars).pptx",
                "Unicode - 你好.txt");

        for (var name: validNames) {

            var badName = BASIC_CREATE_FILE_REQUEST.toBuilder().setName(name).build();
            var badNameResult = Helpers.clientStreaming(dataClient::createFile, badName);
            waitFor(TEST_TIMEOUT, badNameResult);

            assertDoesNotThrow(
                    () -> resultOf(badNameResult),
                    "Valid file name rejected: " + name);
        }
    }

    @Test
    void testCreateFile_mimeTypeMissing() {

        var noMimeType = BASIC_CREATE_FILE_REQUEST.toBuilder().clearMimeType().build();
        var noMimeTypeResult = Helpers.clientStreaming(dataClient::createFile, noMimeType);
        waitFor(TEST_TIMEOUT, noMimeTypeResult);
        var noMimeTypeResultError = assertThrows(StatusRuntimeException.class, () -> resultOf(noMimeTypeResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, noMimeTypeResultError.getStatus().getCode());
    }

    @Test
    void testCreateFile_mimeTypeInvalid() {

        var invalidMimeTypes = List.of(
                "test/plain/extra_part",
                "single_part",
                "forward\\slash",
                ".", "/", "\\",
                "unregistered/primary.type");

        for (var mimeType: invalidMimeTypes) {

            var badMimeType = BASIC_CREATE_FILE_REQUEST.toBuilder().setMimeType(mimeType).build();
            var badMimeTypeResult = Helpers.clientStreaming(dataClient::createFile, badMimeType);
            waitFor(TEST_TIMEOUT, badMimeTypeResult);

            var badMimeTypeError = assertThrows(
                    StatusRuntimeException.class,
                    () -> resultOf(badMimeTypeResult),
                    "Invalid mime type did not fail: " + mimeType);

            assertEquals(Status.Code.INVALID_ARGUMENT, badMimeTypeError.getStatus().getCode());
        }
    }

    @Test
    void testCreateFile_sizeOptional() throws Exception {

        // If size is not specified, TRAC should still accept the file and set the received size in the metadata
        // In this case the usual verification of file size is not performed

        var sizeOmitted = BASIC_CREATE_FILE_REQUEST.toBuilder().clearSize().build();
        var createFile = Helpers.clientStreaming(dataClient::createFile, sizeOmitted);
        waitFor(TEST_TIMEOUT, createFile);
        var fileId = resultOf(createFile);

        // Check size is set in metadata

        var metaReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(fileId))
                .build();

        var metaResponse = Futures.javaFuture(metaClient.readObject(metaReadRequest));
        waitFor(TEST_TIMEOUT, metaResponse);
        var tag = resultOf(metaResponse);
        var def = tag.getDefinition();
        var fileDef = def.getFile();
        Assertions.assertEquals(BASIC_FILE_CONTENT.size(), fileDef.getSize());

        // Read back content and check

        var fileReadRequest = FileReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(fileId))
                .build();

        var responseStream = Concurrent.<FileReadResponse>hub(execContext);
        var byteStream = Concurrent.map(responseStream, FileReadResponse::getContent);
        var content = Concurrent.fold(byteStream,
                ByteString::concat,
                ByteString.EMPTY);

        Helpers.serverStreaming(dataClient::readFile, fileReadRequest, responseStream);

        waitFor(TEST_TIMEOUT, content);
        Assertions.assertEquals(BASIC_FILE_CONTENT, resultOf(content));
    }

    @Test
    void testCreateFile_sizeNegative() {

        var sizeNegative = BASIC_CREATE_FILE_REQUEST.toBuilder().setSize(-1).build();
        var sizeNegativeResult = Helpers.clientStreaming(dataClient::createFile, sizeNegative);
        waitFor(TEST_TIMEOUT, sizeNegativeResult);
        var sizeNegativeError = assertThrows(StatusRuntimeException.class, () -> resultOf(sizeNegativeResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, sizeNegativeError.getStatus().getCode());
    }

    @Test
    void testCreateFile_sizeMismatch() {

        // Size is specified but does not match the number of bytes received on the stream
        // This is reported as DATA_LOSS

        var sizeNegative = BASIC_CREATE_FILE_REQUEST.toBuilder().setSize(1).build();
        var sizeNegativeResult = Helpers.clientStreaming(dataClient::createFile, sizeNegative);
        waitFor(TEST_TIMEOUT, sizeNegativeResult);
        var sizeNegativeError = assertThrows(StatusRuntimeException.class, () -> resultOf(sizeNegativeResult));
        assertEquals(Status.Code.DATA_LOSS, sizeNegativeError.getStatus().getCode());
    }


    // -----------------------------------------------------------------------------------------------------------------
    // UPDATE FILE
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void testUpdateFile_dataOk() throws Exception {

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var v1Selector = MetadataUtil.selectorFor(v1Id);
        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder().setPriorVersion(v1Selector).build();
        var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
        waitFor(TEST_TIMEOUT, updateFile);
        var v2Id = resultOf(updateFile);

        // Read back updated content and check

        var v2ReadRequest = FileReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(v2Id))
                .build();

        var v2Response = Concurrent.<FileReadResponse>hub(execContext);
        var v2ByteStream = Concurrent.map(v2Response, FileReadResponse::getContent);
        var v2Content = Concurrent.fold(v2ByteStream,
                ByteString::concat,
                ByteString.EMPTY);

        Helpers.serverStreaming(dataClient::readFile, v2ReadRequest, v2Response);

        waitFor(TEST_TIMEOUT, v2Content);
        Assertions.assertEquals(BASIC_FILE_CONTENT_V2, resultOf(v2Content));

        // Make sure original content is still available as v1

        var v1ReadRequest = FileReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(v1Id))
                .build();

        var v1Response = Concurrent.<FileReadResponse>hub(execContext);
        var v1ByteStream = Concurrent.map(v1Response, FileReadResponse::getContent);
        var v1Content = Concurrent.fold(v1ByteStream,
                ByteString::concat,
                ByteString.EMPTY);

        Helpers.serverStreaming(dataClient::readFile, v1ReadRequest, v1Response);

        waitFor(TEST_TIMEOUT, v1Content);
        Assertions.assertEquals(BASIC_FILE_CONTENT, resultOf(v1Content));
    }

    @Test
    void testUpdateFile_metadataOk() throws Exception {

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var v1Selector = MetadataUtil.selectorFor(v1Id);
        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder().setPriorVersion(v1Selector).build();
        var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
        waitFor(TEST_TIMEOUT, updateFile);
        var v2Id = resultOf(updateFile);

        var metaReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(v2Id))
                .build();

        var metaResponse = Futures.javaFuture(metaClient.readObject(metaReadRequest));
        waitFor(TEST_TIMEOUT, metaResponse);
        var tag = resultOf(metaResponse);

        // Explicitly set attrs

        Assertions.assertTrue(tag.containsAttrs("app_template"));
        Assertions.assertTrue(tag.containsAttrs("description"));
        var appTemplateAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("app_template"));
        var descriptionAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("description"));
        Assertions.assertEquals("template_name", appTemplateAttr);
        Assertions.assertEquals("Describes what is in the V2 template", descriptionAttr);

        // Controlled attrs should always be set for files

        Assertions.assertTrue(tag.containsAttrs("trac_file_name"));
        Assertions.assertTrue(tag.containsAttrs("trac_file_extension"));
        Assertions.assertTrue(tag.containsAttrs("trac_file_mime_type"));
        Assertions.assertTrue(tag.containsAttrs("trac_file_size"));
        var nameAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("trac_file_name"));
        var extensionAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("trac_file_extension"));
        var mimeTypeAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("trac_file_mime_type"));
        var sizeAttr = MetadataCodec.decodeIntegerValue(tag.getAttrsOrThrow("trac_file_size"));
        Assertions.assertEquals("some_file.txt", nameAttr);
        Assertions.assertEquals("txt", extensionAttr);
        Assertions.assertEquals("text/plain", mimeTypeAttr);
        Assertions.assertEquals(BASIC_FILE_CONTENT_V2.size(), sizeAttr);

        // Check core attributes of the file definition

        var def = tag.getDefinition();
        Assertions.assertEquals(ObjectType.FILE, def.getObjectType());

        var fileDef = def.getFile();
        Assertions.assertEquals("some_file.txt", fileDef.getName());
        Assertions.assertEquals("txt", fileDef.getExtension());
        Assertions.assertEquals("text/plain", fileDef.getMimeType());
        Assertions.assertEquals(BASIC_FILE_CONTENT_V2.size(), fileDef.getSize());

        // Storage checks

        Assertions.assertEquals(ObjectType.STORAGE, fileDef.getStorageId().getObjectType());
        Assertions.assertTrue(fileDef.getStorageId().getLatestObject());
        Assertions.assertTrue(fileDef.getStorageId().getLatestTag());
        Assertions.assertFalse(fileDef.getDataItem().isBlank());

        // Make sure V2 file is referring to the same storage object
        // Also, V1 and V2 file blobs should have different data item IDs

        var metaV1Request = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(v1Id))
                .build();

        var metaV1Response = Futures.javaFuture(metaClient.readObject(metaV1Request));
        waitFor(TEST_TIMEOUT, metaV1Response);
        var tagV1 = resultOf(metaV1Response);
        var fileDefV1 = tagV1.getDefinition().getFile();

        Assertions.assertEquals(fileDefV1.getStorageId(), fileDef.getStorageId());
        Assertions.assertNotEquals(fileDefV1.getDataItem(), fileDef.getDataItem());
    }

    @Test
    void testUpdateFile_versionDuplicated() throws Exception {

        // Create valid v1

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        // Create valid v2

        var v1Selector = MetadataUtil.selectorFor(v1Id);
        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder().setPriorVersion(v1Selector).build();
        var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
        waitFor(TEST_TIMEOUT, updateFile);
        Assertions.assertDoesNotThrow(() -> resultOf(updateFile));

        // Attempt to create v2 again

        var updateRequest2 = BASIC_UPDATE_FILE_REQUEST.toBuilder().setPriorVersion(v1Selector).build();
        var updateFile2 = Helpers.clientStreaming(dataClient::updateFile, updateRequest2);
        waitFor(TEST_TIMEOUT, updateFile2);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(updateFile2));
        assertEquals(Status.Code.ALREADY_EXISTS, updateError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_versionMissing() throws Exception {

        // Create valid v1

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        // Attempt to create v3 without create v2 first

        var v1Selector = MetadataUtil.selectorFor(v1Id);
        var v2Selector = v1Selector.toBuilder()
                .setObjectVersion(2)
                .build();

        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder().setPriorVersion(v2Selector).build();
        var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
        waitFor(TEST_TIMEOUT, updateFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(updateFile));
        assertEquals(Status.Code.ALREADY_EXISTS, updateError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_priorVersionNull() {

        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder().clearPriorVersion().build();
        var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
        waitFor(TEST_TIMEOUT, updateFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(updateFile));
        assertEquals(Status.Code.INVALID_ARGUMENT, updateError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_priorVersionInvalid() {

        var priorSelector = TagSelector.newBuilder()
            .setObjectType(ObjectType.FILE)
            .setObjectId("not_a_valid_id")
            .setLatestObject(true)
            .setLatestTag(true)
            .build();

        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder().setPriorVersion(priorSelector).build();
        var update = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
        waitFor(TEST_TIMEOUT, update);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(update));
        assertEquals(Status.Code.INVALID_ARGUMENT, updateError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_priorVersionWrongType() throws Exception {

        // Create a valid v1

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        // Set up an update request using the valid v1 prior version

        var v1Selector = MetadataUtil.selectorFor(v1Id).toBuilder()
                .setObjectType(ObjectType.DATA)
                .build();

        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder().setPriorVersion(v1Selector).build();
        var update = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
        waitFor(TEST_TIMEOUT, update);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(update));
        assertEquals(Status.Code.INVALID_ARGUMENT, updateError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_priorVersionIdNotFound() {

        var priorSelector = TagSelector.newBuilder()
                .setObjectType(ObjectType.FILE)
                .setObjectId(UUID.randomUUID().toString())
                .setLatestObject(true)
                .setLatestTag(true)
                .build();

        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder().setPriorVersion(priorSelector).build();
        var update = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
        waitFor(TEST_TIMEOUT, update);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(update));
        assertEquals(Status.Code.NOT_FOUND, updateError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_tagUpdateInvalid() throws Exception {

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var v1Selector = MetadataUtil.selectorFor(v1Id);

        var invalidTagUpdate = TagUpdate.newBuilder()
                .setAttrName("£!£$%£$%")
                .setOperation(TagOperation.CREATE_ATTR)
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder()
                .setPriorVersion(v1Selector)
                .addTagUpdates(invalidTagUpdate)
                .build();

        var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
        waitFor(TEST_TIMEOUT, updateFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(updateFile));
        assertEquals(Status.Code.INVALID_ARGUMENT, updateError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_tagUpdateReserved() throws Exception {

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var v1Selector = MetadataUtil.selectorFor(v1Id);

        var reservedAttrs = List.of("trac_file_attr", "_file_attr", "__file_attr");

        for (var attrName: reservedAttrs) {

            var invalidTagUpdate = TagUpdate.newBuilder()
                    .setAttrName(attrName)
                    .setOperation(TagOperation.CREATE_ATTR)
                    .setValue(MetadataCodec.encodeValue("some_value"))
                    .build();

            var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder()
                    .setPriorVersion(v1Selector)
                    .addTagUpdates(invalidTagUpdate)
                    .build();

            var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
            waitFor(TEST_TIMEOUT, updateFile);
            var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(updateFile));
            assertEquals(Status.Code.INVALID_ARGUMENT, updateError.getStatus().getCode());
        }
    }

    @Test
    void testUpdateFile_tagUpdateAttrNotPresent() throws Exception {

        // Attempt to replace an attr that doesn't already exist
        // Report this as failed_precondition, rather than not_found
        // not_found would imply the file object/version itself was missing

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var v1Selector = MetadataUtil.selectorFor(v1Id);

        var invalidTagUpdate = TagUpdate.newBuilder()
                .setAttrName("an_attr_that_is_not_set")
                .setOperation(TagOperation.REPLACE_ATTR)
                .setValue(MetadataCodec.encodeValue("some_value"))
                .build();

        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder()
                .setPriorVersion(v1Selector)
                .addTagUpdates(invalidTagUpdate)
                .build();

        var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
        waitFor(TEST_TIMEOUT, updateFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(updateFile));
        assertEquals(Status.Code.FAILED_PRECONDITION, updateError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_nameOmitted() throws Exception {

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var v1Selector = MetadataUtil.selectorFor(v1Id);

        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder()
                .setPriorVersion(v1Selector)
                .clearName()
                .build();

        var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
        waitFor(TEST_TIMEOUT, updateFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(updateFile));
        assertEquals(Status.Code.INVALID_ARGUMENT, updateError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_nameInvalid() throws Exception {

        var invalidNames = List.of(
                ".", "/", "\\",
                "./a", ".\\a", "a/b", "a\\b", "a/", "a\\",
                "a\rb", "a\nb", "a\tb", "a\0b",
                "$£\"£$%^\0<:$%^&D'¬#FSG)");

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var v1Selector = MetadataUtil.selectorFor(v1Id);

        for (var name: invalidNames) {

            var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder()
                    .setPriorVersion(v1Selector)
                    .setName(name)
                    .build();

            var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
            waitFor(TEST_TIMEOUT, updateFile);
            var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(updateFile));
            assertEquals(Status.Code.INVALID_ARGUMENT, updateError.getStatus().getCode());
        }
    }

    @Test
    void testUpdateFile_nameValid() throws Exception {

        // Make sure file name validation is not rejecting valid punctuation, unicode etc.

        var validNames = List.of(
                "A name with spaces.txt",
                "Some punctuation - this is allowed (for non reserved chars).txt",
                "Unicode - 你好.txt");

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var latestSelector = MetadataUtil.selectorFor(v1Id);

        for (var name: validNames) {

            var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder()
                    .setPriorVersion(latestSelector)
                    .setName(name)
                    .build();

            var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
            waitFor(TEST_TIMEOUT, updateFile);

            var latestId = assertDoesNotThrow(
                    () -> resultOf(updateFile),
                    "Valid file name rejected: " + name);

            latestSelector = MetadataUtil.selectorFor(latestId);
        }
    }

    @Test
    void testUpdateFile_nameChange() throws Exception {

        // Name change is allowed so long as extension remains the same
        // metadataOk test also checks new file name is set

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var v1Selector = MetadataUtil.selectorFor(v1Id);

        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder()
                .setPriorVersion(v1Selector)
                .setName("alternate_file_name_v2.txt")
                .build();

        var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
        waitFor(TEST_TIMEOUT, updateFile);
        var v2Id =  resultOf(updateFile);

        var metaReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(v2Id))
                .build();

        var metaResponse = Futures.javaFuture(metaClient.readObject(metaReadRequest));
        waitFor(TEST_TIMEOUT, metaResponse);
        var tag = resultOf(metaResponse);

        // Check controlled tag attrs

        Assertions.assertTrue(tag.containsAttrs("trac_file_name"));
        Assertions.assertTrue(tag.containsAttrs("trac_file_extension"));
        Assertions.assertTrue(tag.containsAttrs("trac_file_mime_type"));
        Assertions.assertTrue(tag.containsAttrs("trac_file_size"));
        var nameAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("trac_file_name"));
        var extensionAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("trac_file_extension"));
        var mimeTypeAttr = MetadataCodec.decodeStringValue(tag.getAttrsOrThrow("trac_file_mime_type"));
        var sizeAttr = MetadataCodec.decodeIntegerValue(tag.getAttrsOrThrow("trac_file_size"));
        Assertions.assertEquals("alternate_file_name_v2.txt", nameAttr);
        Assertions.assertEquals("txt", extensionAttr);
        Assertions.assertEquals("text/plain", mimeTypeAttr);
        Assertions.assertEquals(BASIC_FILE_CONTENT_V2.size(), sizeAttr);

        // Check file definition

        var def = tag.getDefinition();
        Assertions.assertEquals(ObjectType.FILE, def.getObjectType());

        var fileDef = def.getFile();
        Assertions.assertEquals("alternate_file_name_v2.txt", fileDef.getName());
        Assertions.assertEquals("txt", fileDef.getExtension());
        Assertions.assertEquals("text/plain", fileDef.getMimeType());
        Assertions.assertEquals(BASIC_FILE_CONTENT_V2.size(), fileDef.getSize());
    }

    @Test
    void testUpdateFile_nameExtensionChange() throws Exception {

        // Differing extension is a failed pre-condition

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var v1Selector = MetadataUtil.selectorFor(v1Id);

        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder()
                .setPriorVersion(v1Selector)
                .setName("some_file.docx")
                .build();

        var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
        waitFor(TEST_TIMEOUT, updateFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(updateFile));
        assertEquals(Status.Code.FAILED_PRECONDITION, updateError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_mimeTypeOmitted() throws Exception {

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var v1Selector = MetadataUtil.selectorFor(v1Id);

        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder()
                .setPriorVersion(v1Selector)
                .clearMimeType()
                .build();

        var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
        waitFor(TEST_TIMEOUT, updateFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(updateFile));
        assertEquals(Status.Code.INVALID_ARGUMENT, updateError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_mimeTypeInvalid() throws Exception {

        var invalidMimeTypes = List.of(
                "test/plain/extra_part",
                "single_part",
                "forward\\slash",
                ".", "/", "\\",
                "unregistered/primary.type");

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var v1Selector = MetadataUtil.selectorFor(v1Id);

        for (var mimeType: invalidMimeTypes) {

            var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder()
                    .setPriorVersion(v1Selector)
                    .setMimeType(mimeType)
                    .build();

            var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
            waitFor(TEST_TIMEOUT, updateFile);
            var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(updateFile));
            assertEquals(Status.Code.INVALID_ARGUMENT, updateError.getStatus().getCode());
        }
    }

    @Test
    void testUpdateFile_mimeTypeChange() throws Exception {

        // Changing mime type is a failed pre-condition

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var v1Selector = MetadataUtil.selectorFor(v1Id);

        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder()
                .setPriorVersion(v1Selector)
                .setMimeType("text/html")
                .build();

        var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
        waitFor(TEST_TIMEOUT, updateFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(updateFile));
        assertEquals(Status.Code.FAILED_PRECONDITION, updateError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_sizeOptional() throws Exception {

        // If size is not specified, TRAC should still accept the file and set the received size in the metadata
        // In this case the usual verification of file size is not performed

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var v1Selector = MetadataUtil.selectorFor(v1Id);

        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder()
                .setPriorVersion(v1Selector)
                .clearSize()
                .build();

        var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
        waitFor(TEST_TIMEOUT, updateFile);
        var v2Id = resultOf(updateFile);

        // Check size is set in metadata

        var metaReadRequest = MetadataReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(v2Id))
                .build();

        var metaResponse = Futures.javaFuture(metaClient.readObject(metaReadRequest));
        waitFor(TEST_TIMEOUT, metaResponse);
        var tag = resultOf(metaResponse);
        var def = tag.getDefinition();
        var fileDef = def.getFile();
        Assertions.assertEquals(BASIC_FILE_CONTENT_V2.size(), fileDef.getSize());

        // Read back content and check

        var fileReadRequest = FileReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(v2Id))
                .build();

        var responseStream = Concurrent.<FileReadResponse>hub(execContext);
        var byteStream = Concurrent.map(responseStream, FileReadResponse::getContent);
        var content = Concurrent.fold(byteStream,
                ByteString::concat,
                ByteString.EMPTY);

        Helpers.serverStreaming(dataClient::readFile, fileReadRequest, responseStream);

        waitFor(TEST_TIMEOUT, content);
        Assertions.assertEquals(BASIC_FILE_CONTENT_V2, resultOf(content));
    }

    @Test
    void testUpdateFile_sizeNegative() throws Exception {

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var v1Selector = MetadataUtil.selectorFor(v1Id);

        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder()
                .setPriorVersion(v1Selector)
                .setSize(-1)
                .build();

        var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
        waitFor(TEST_TIMEOUT, updateFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(updateFile));
        assertEquals(Status.Code.INVALID_ARGUMENT, updateError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_sizeMismatch() throws Exception {

        // Size mis-match is reported as error status DATA_LOSS

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);
        var v1Selector = MetadataUtil.selectorFor(v1Id);

        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder()
                .setPriorVersion(v1Selector)
                .setSize(1)
                .build();

        var updateFile = Helpers.clientStreaming(dataClient::updateFile, updateRequest);
        waitFor(TEST_TIMEOUT, updateFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(updateFile));
        assertEquals(Status.Code.DATA_LOSS, updateError.getStatus().getCode());
    }


    // -----------------------------------------------------------------------------------------------------------------
    // READ FILE
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void testReadFile_ok() {
        Assertions.fail();
    }

    @Test
    void testReadFile_objectVersionLatest() {
        Assertions.fail();
    }

    @Test
    void testReadFile_objectVersionExplicit() {
        Assertions.fail();
    }

    @Test
    void testReadFile_objectVersionAsOf() {
        Assertions.fail();
    }

    @Test
    void testReadFile_tagVersionLatest() {
        Assertions.fail();
    }

    @Test
    void testReadFile_tagVersionExplicit() {
        Assertions.fail();
    }

    @Test
    void testReadFile_tagVersionAsOf() {
        Assertions.fail();
    }

    @Test
    void testReadFile_selectorTypeOmitted() throws Exception {

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .clearObjectType())
                .build();

        var readFile = Helpers.serverStreamingDiscard(dataClient::readFile, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(readFile));
        assertEquals(Status.Code.INVALID_ARGUMENT, updateError.getStatus().getCode());
    }

    @Test
    void testReadFile_selectorTypeNotFile() throws Exception {

        // Selector object type must be ObjectType.FILE for file read requests

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setObjectType(ObjectType.DATA))
                .build();

        var readFile = Helpers.serverStreamingDiscard(dataClient::readFile, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(readFile));
        assertEquals(Status.Code.INVALID_ARGUMENT, updateError.getStatus().getCode());
    }

    @Test
    void testReadFile_selectorIdOmitted() throws Exception {

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .clearObjectId())
                .build();

        var readFile = Helpers.serverStreamingDiscard(dataClient::readFile, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(readFile));
        assertEquals(Status.Code.INVALID_ARGUMENT, updateError.getStatus().getCode());
    }

    @Test
    void testReadFile_selectorIdInvalid() throws Exception {

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setObjectId("not_a_valid_id"))
                .build();

        var readFile = Helpers.serverStreamingDiscard(dataClient::readFile, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(readFile));
        assertEquals(Status.Code.INVALID_ARGUMENT, updateError.getStatus().getCode());
    }

    @Test
    void testReadFile_selectorIdNotFound() throws Exception {

        var objId = UUID.randomUUID().toString();  // non-existent object ID to look for

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setObjectId(objId))
                .build();

        var readFile = Helpers.serverStreamingDiscard(dataClient::readFile, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(readFile));
        assertEquals(Status.Code.NOT_FOUND, updateError.getStatus().getCode());
    }

    @Test
    void testReadFile_objectVersionMissing() throws Exception {

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .clearObjectVersionCriteria())
                .build();

        var readFile = Helpers.serverStreamingDiscard(dataClient::readFile, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(readFile));
        assertEquals(Status.Code.INVALID_ARGUMENT, updateError.getStatus().getCode());
    }

    @Test
    void testReadFile_objectVersionInvalid() throws Exception {

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var basicRequest = readRequest(v1Id);

        // Try to read obj version 0 - versions should start at 1

        var readRequest1 = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setObjectVersion(0))
                .build();

        var readFile1 = Helpers.serverStreamingDiscard(dataClient::readFile, readRequest1, execContext);

        waitFor(TEST_TIMEOUT, readFile1);
        var updateError1 = assertThrows(StatusRuntimeException.class, () -> resultOf(readFile1));
        assertEquals(Status.Code.INVALID_ARGUMENT, updateError1.getStatus().getCode());

        // Try to read as-of an invalid datetime

        var readRequest2 = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setObjectAsOf(DatetimeValue.newBuilder()
                .setIsoDatetime("invalid_iso_datetime")))
                .build();

        var readFile2 = Helpers.serverStreamingDiscard(dataClient::readFile, readRequest2, execContext);

        waitFor(TEST_TIMEOUT, readFile2);
        var updateError2 = assertThrows(StatusRuntimeException.class, () -> resultOf(readFile1));
        assertEquals(Status.Code.INVALID_ARGUMENT, updateError2.getStatus().getCode());
    }

    @Test
    void testReadFile_objectVersionNotFound() throws Exception {

        var objVer = 2;  // non-existent object version to look for

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setObjectVersion(objVer))
                .build();

        var readFile = Helpers.serverStreamingDiscard(dataClient::readFile, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(readFile));
        assertEquals(Status.Code.NOT_FOUND, updateError.getStatus().getCode());
    }

    @Test
    void testReadFile_objectVersionNotFoundAsOf() throws Exception {

        var timeBeforeTest = Instant.now();
        Thread.sleep(10);

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setObjectAsOf(MetadataCodec.encodeDatetime(timeBeforeTest)))
                .build();

        var readFile = Helpers.serverStreamingDiscard(dataClient::readFile, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(readFile));
        assertEquals(Status.Code.NOT_FOUND, updateError.getStatus().getCode());
    }

    @Test
    void testReadFile_tagVersionMissing() throws Exception {

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .clearTagVersionCriteria())
                .build();

        var readFile = Helpers.serverStreamingDiscard(dataClient::readFile, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(readFile));
        assertEquals(Status.Code.INVALID_ARGUMENT, updateError.getStatus().getCode());
    }

    @Test
    void testReadFile_tagVersionInvalid() throws Exception {

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var basicRequest = readRequest(v1Id);

        // Try to read tag version 0 - versions should start at 1

        var readRequest1 = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setTagVersion(0))
                .build();

        var readFile1 = Helpers.serverStreamingDiscard(dataClient::readFile, readRequest1, execContext);

        waitFor(TEST_TIMEOUT, readFile1);
        var updateError1 = assertThrows(StatusRuntimeException.class, () -> resultOf(readFile1));
        assertEquals(Status.Code.INVALID_ARGUMENT, updateError1.getStatus().getCode());

        // Try to read as-of an invalid datetime

        var readRequest2 = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setTagAsOf(DatetimeValue.newBuilder()
                .setIsoDatetime("invalid_iso_datetime")))
                .build();

        var readFile2 = Helpers.serverStreamingDiscard(dataClient::readFile, readRequest2, execContext);

        waitFor(TEST_TIMEOUT, readFile2);
        var updateError2 = assertThrows(StatusRuntimeException.class, () -> resultOf(readFile1));
        assertEquals(Status.Code.INVALID_ARGUMENT, updateError2.getStatus().getCode());
    }

    @Test
    void testReadFile_tagVersionNotFound() throws Exception {

        var tagVer = 2;  // non-existent object version to look for

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setTagVersion(tagVer))
                .build();

        var readFile = Helpers.serverStreamingDiscard(dataClient::readFile, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(readFile));
        assertEquals(Status.Code.NOT_FOUND, updateError.getStatus().getCode());
    }

    @Test
    void testReadFile_tagVersionNotFoundAsOf() throws Exception {

        var timeBeforeTest = Instant.now();
        Thread.sleep(10);

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        var basicRequest = readRequest(v1Id);

        var readRequest = basicRequest.toBuilder()
                .setSelector(basicRequest.getSelector().toBuilder()
                .setTagAsOf(MetadataCodec.encodeDatetime(timeBeforeTest)))
                .build();

        var readFile = Helpers.serverStreamingDiscard(dataClient::readFile, readRequest, execContext);

        waitFor(TEST_TIMEOUT, readFile);
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(readFile));
        assertEquals(Status.Code.NOT_FOUND, updateError.getStatus().getCode());
    }
}
