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
import com.accenture.trac.metadata.ObjectType;
import com.accenture.trac.metadata.TagOperation;
import com.accenture.trac.metadata.TagSelector;
import com.accenture.trac.metadata.TagUpdate;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.resultOf;
import static com.accenture.trac.test.concurrent.ConcurrentTestHelpers.waitFor;
import static org.junit.jupiter.api.Assertions.*;


public class DataApiTest_File extends DataApiTest_Base {

    // Functional test cases for file operations in the data API
    // (createFile, updateFile, readFile)

    private static final List<TagUpdate> BASIC_TAG_UPDATES = List.of(
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

    private static final ByteString BASIC_FILE_CONTENT = ByteString
            .copyFrom("Sample content\n", StandardCharsets.UTF_8);

    private static final FileWriteRequest BASIC_CREATE_FILE_REQUEST = FileWriteRequest.newBuilder()
            .setTenant(TEST_TENANT)
            .addAllTagUpdates(BASIC_TAG_UPDATES)
            .setName("some_file.txt")
            .setMimeType("text/plain")
            .setSize(BASIC_FILE_CONTENT.size())
            .setContent(BASIC_FILE_CONTENT)
            .build();

    private static final List<TagUpdate> BASIC_TAG_UPDATES_V2 = List.of(
            TagUpdate.newBuilder()
                    .setAttrName("description")
                    .setOperation(TagOperation.REPLACE_ATTR)
                    .setValue(MetadataCodec.encodeValue("Describes what is in the V2 template"))
                    .build());

    private static final ByteString BASIC_FILE_CONTENT_V2 = ByteString
            .copyFrom("Sample content v2\nSome new updated content\n", StandardCharsets.UTF_8);

    private static final FileWriteRequest BASIC_UPDATE_FILE_REQUEST = FileWriteRequest.newBuilder()
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

        var responseStream = Helpers.serverStreaming(dataClient::readFile, fileReadRequest, execContext);
        var byteStream = Concurrent.map(responseStream, FileReadResponse::getContent);
        var content = Concurrent.fold(byteStream,
                ByteString::concat,
                ByteString.EMPTY);

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

        // Storage def is intended for internal use by the platform
        // This is just a cursory check to make sure it has been set

        Assertions.assertEquals(ObjectType.STORAGE, fileDef.getStorageId().getObjectType());
        Assertions.assertFalse(fileDef.getDataItem().isBlank());
    }

    @Test
    void testCreateFile_tenancy() {

        var noTenant = BASIC_CREATE_FILE_REQUEST.toBuilder().clearTenant().build();
        var noTenantResult = Helpers.clientStreaming(dataClient::createFile, noTenant);
        waitFor(TEST_TIMEOUT, noTenantResult);
        var noTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(noTenantResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, noTenantError.getStatus().getCode());

        var invalidTenant = BASIC_CREATE_FILE_REQUEST.toBuilder().setTenant("£$%^**!\0\n/`¬").build();
        var invalidTenantResult = Helpers.clientStreaming(dataClient::createFile, invalidTenant);
        waitFor(TEST_TIMEOUT, invalidTenantResult);
        var invalidTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(invalidTenantResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, invalidTenantError.getStatus().getCode());

        var unknownTenant = BASIC_CREATE_FILE_REQUEST.toBuilder().setTenant("UNKNOWN").build();
        var unknownTenantResult = Helpers.clientStreaming(dataClient::createFile, unknownTenant);
        waitFor(TEST_TIMEOUT, unknownTenantResult);
        var unknownTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(unknownTenantResult));
        assertEquals(Status.Code.NOT_FOUND, unknownTenantError.getStatus().getCode());
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
                "$£\"£$%^\0<:$%^&D'¬#FSG)");

        for (var name: invalidNames) {

            var badName = BASIC_CREATE_FILE_REQUEST.toBuilder().setName(name).build();
            var badNameResult = Helpers.clientStreaming(dataClient::createFile, badName);
            waitFor(TEST_TIMEOUT, badNameResult);
            var badNameError = assertThrows(StatusRuntimeException.class, () -> resultOf(badNameResult));
            assertEquals(Status.Code.INVALID_ARGUMENT, badNameError.getStatus().getCode());
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
            var badMimeTypeError = assertThrows(StatusRuntimeException.class, () -> resultOf(badMimeTypeResult));
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

        var responseStream = Helpers.serverStreaming(dataClient::readFile, fileReadRequest, execContext);
        var byteStream = Concurrent.map(responseStream, FileReadResponse::getContent);
        var content = Concurrent.fold(byteStream,
                ByteString::concat,
                ByteString.EMPTY);

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
        var v2Id = resultOf(updateFile);

        // Read back updated content and check

        var v2ReadRequest = FileReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(v2Id))
                .build();

        var v2Response = Helpers.serverStreaming(dataClient::readFile, v2ReadRequest, execContext);
        var v2ByteStream = Concurrent.map(v2Response, FileReadResponse::getContent);
        var v2Content = Concurrent.fold(v2ByteStream,
                ByteString::concat,
                ByteString.EMPTY);

        waitFor(TEST_TIMEOUT, v2Content);
        Assertions.assertEquals(BASIC_FILE_CONTENT, resultOf(v2Content));

        // Make sure original content is still available as v1

        var v1ReadRequest = FileReadRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSelector(MetadataUtil.selectorFor(v1Id))
                .build();

        var v1Response = Helpers.serverStreaming(dataClient::readFile, v1ReadRequest, execContext);
        var v1ByteStream = Concurrent.map(v1Response, FileReadResponse::getContent);
        var v1Content = Concurrent.fold(v1ByteStream,
                ByteString::concat,
                ByteString.EMPTY);

        waitFor(TEST_TIMEOUT, v1Content);
        Assertions.assertEquals(BASIC_FILE_CONTENT, resultOf(v1Content));
    }

    @Test
    void testUpdateFile_metadataOk() {
        Assertions.fail();
    }

    @Test
    void testUpdateFile_tenancy() throws Exception {

        // Create a valid v1

        var createFile = Helpers.clientStreaming(dataClient::createFile, BASIC_CREATE_FILE_REQUEST);
        waitFor(TEST_TIMEOUT, createFile);
        var v1Id = resultOf(createFile);

        // Set up an update request using the valid v1 prior version

        var v1Selector = MetadataUtil.selectorFor(v1Id);
        var updateRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder().setPriorVersion(v1Selector).build();

        // No do the tenancy checks on updateFile

        var noTenant = updateRequest.toBuilder().clearTenant().build();
        var noTenantResult = Helpers.clientStreaming(dataClient::updateFile, noTenant);
        waitFor(TEST_TIMEOUT, noTenantResult);
        var noTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(noTenantResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, noTenantError.getStatus().getCode());

        var invalidTenant = updateRequest.toBuilder().setTenant("£$%^**!\0\n/`¬").build();
        var invalidTenantResult = Helpers.clientStreaming(dataClient::updateFile, invalidTenant);
        waitFor(TEST_TIMEOUT, invalidTenantResult);
        var invalidTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(invalidTenantResult));
        assertEquals(Status.Code.INVALID_ARGUMENT, invalidTenantError.getStatus().getCode());

        var unknownTenant = updateRequest.toBuilder().setTenant("UNKNOWN").build();
        var unknownTenantResult = Helpers.clientStreaming(dataClient::updateFile, unknownTenant);
        waitFor(TEST_TIMEOUT, unknownTenantResult);
        var unknownTenantError = assertThrows(StatusRuntimeException.class, () -> resultOf(unknownTenantResult));
        assertEquals(Status.Code.NOT_FOUND, unknownTenantError.getStatus().getCode());
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
        Assertions.assertDoesNotThrow(() -> resultOf(updateFile));

        // Attempt to create v2 again

        var updateRequest2 = BASIC_UPDATE_FILE_REQUEST.toBuilder().setPriorVersion(v1Selector).build();
        var updateFile2 = Helpers.clientStreaming(dataClient::updateFile, updateRequest2);
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
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(updateFile));
        assertEquals(Status.Code.ALREADY_EXISTS, updateError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_priorVersionNull() {

        var nullPriorRequest = BASIC_UPDATE_FILE_REQUEST.toBuilder().clearPriorVersion().build();
        var nullPrior = Helpers.clientStreaming(dataClient::updateFile, nullPriorRequest);
        waitFor(TEST_TIMEOUT, nullPrior);
        var nullPriorError = assertThrows(StatusRuntimeException.class, () -> resultOf(nullPrior));
        assertEquals(Status.Code.INVALID_ARGUMENT, nullPriorError.getStatus().getCode());
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
    void testUpdateFile_tagUpdateInvalid() {
        Assertions.fail();
    }

    @Test
    void testUpdateFile_tagUpdateReserved() {
        Assertions.fail();
    }

    @Test
    void testUpdateFile_tagUpdateAttrNotPresent() {
        Assertions.fail();
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

            var updateFile = Helpers.clientStreaming(dataClient::createFile, updateRequest);
            waitFor(TEST_TIMEOUT, updateFile);
            var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(updateFile));
            assertEquals(Status.Code.INVALID_ARGUMENT, updateError.getStatus().getCode());
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
        assertDoesNotThrow(() -> resultOf(updateFile));
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

            var updateFile = Helpers.clientStreaming(dataClient::createFile, updateRequest);
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
        var updateError = assertThrows(StatusRuntimeException.class, () -> resultOf(updateFile));
        assertEquals(Status.Code.FAILED_PRECONDITION, updateError.getStatus().getCode());
    }

    @Test
    void testUpdateFile_sizeOptional() {
        Assertions.fail();
    }

    @Test
    void testUpdateFile_sizeNegative() {
        Assertions.fail();
    }

    @Test
    void testUpdateFile_sizeMismatch() {
        Assertions.fail();
    }


    // -----------------------------------------------------------------------------------------------------------------
    // READ FILE
    // -----------------------------------------------------------------------------------------------------------------

    void testReadFile_ok() {}

    void testReadFile_objectVersionLatest() {}

    void testReadFile_objectVersionExplicit() {}

    void testReadFile_objectVersionAsOf() {}

    void testReadFile_tagVersionLatest() {}

    void testReadFile_tagVersionExplicit() {}

    void testReadFile_tagVersionAsOf() {}

    void testReadFile_tenantOmitted() {}

    void testReadFile_tenantInvalid() {}

    void testReadFile_tenantNotFound() {}

    void testReadFile_selectorTypeOmitted() {}

    void testReadFile_selectorTypeNotFile() {}

    void testReadFile_selectorIdOmitted() {}

    void testReadFile_selectorIdInvalid() {}

    void testReadFile_selectorIdNotFound() {}

    void testReadFile_objectVersionMissing() {}

    void testReadFile_objectVersionInvalid() {}

    void testReadFile_objectVersionNotFound() {}

    void testReadFile_objectVersionNotFoundAsOf() {}

    void testReadFile_tagVersionMissing() {}

    void testReadFile_tagVersionInvalid() {}

    void testReadFile_tagVersionNotFound() {}

    void testReadFile_tagVersionNotFoundAsOf() {}
}
