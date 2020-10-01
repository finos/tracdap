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

package com.accenture.trac.svc.meta.api;

import com.accenture.trac.common.api.meta.MetadataSearchApiGrpc;
import com.accenture.trac.common.api.meta.MetadataSearchRequest;
import com.accenture.trac.common.api.meta.MetadataTrustedWriteApiGrpc;
import com.accenture.trac.common.api.meta.MetadataWriteRequest;
import com.accenture.trac.common.metadata.BasicType;
import com.accenture.trac.common.metadata.ObjectHeader;
import com.accenture.trac.common.metadata.ObjectType;
import com.accenture.trac.common.metadata.Tag;
import com.accenture.trac.common.metadata.search.*;
import com.accenture.trac.svc.meta.dal.IMetadataDal;
import com.accenture.trac.svc.meta.logic.MetadataSearchLogic;
import com.accenture.trac.svc.meta.logic.MetadataWriteLogic;
import com.accenture.trac.svc.meta.test.IDalTestable;

import com.accenture.trac.svc.meta.test.JdbcH2Impl;
import com.accenture.trac.svc.meta.test.TestData;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;

import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static com.accenture.trac.common.metadata.MetadataCodec.encodeNativeObject;
import static com.accenture.trac.common.metadata.MetadataCodec.encodeValue;
import static com.accenture.trac.svc.meta.test.TestData.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


@ExtendWith(JdbcH2Impl.class)
class MetadataSearchApiTest implements IDalTestable {

    // This test case is meant to test the search API, not the core search functionality
    // I.e. it is to make sure the API covers the common, corner and error cases
    // and passes the parameters down to the DAL search implementation correctly
    // Tests are basic round trip, request validation, page limits, empty search results etc.

    // This class does not try to test every combination of search types and logical operators
    // The DAL search test has that exhaustive set of tests around the search operations

    private IMetadataDal dal;

    public void setDal(IMetadataDal dal) {
        this.dal = dal;
    }

    @Rule
    final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private MetadataSearchApiGrpc.MetadataSearchApiBlockingStub searchApi;
    private MetadataTrustedWriteApiGrpc.MetadataTrustedWriteApiBlockingStub writeApi;

    @BeforeEach
    void setup() throws Exception {

        var serverName = InProcessServerBuilder.generateName();

        var searchLogic = new MetadataSearchLogic(dal);
        var searchApiImpl = new MetadataSearchApi(searchLogic);

        var writeLogic = new MetadataWriteLogic(dal);
        var writeApiImpl = new MetadataTrustedWriteApi(writeLogic);

        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(InProcessServerBuilder
                .forName(serverName)
                .directExecutor()
                .addService(searchApiImpl)
                .addService(writeApiImpl)
                .build()
                .start());

        searchApi = MetadataSearchApiGrpc.newBlockingStub(
                // Create a client channel and register for automatic graceful shutdown.
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));

        writeApi = MetadataTrustedWriteApiGrpc.newBlockingStub(
                // Create a client channel and register for automatic graceful shutdown.
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
    }

    @Test
    void basicSearch() {

        var searchAttr = "basicSearch_WHICH_DROIDS";

        var obj1 = TestData.dummyDataDef(false);
        var obj2 = TestData.dummyDataDef(false);

        var tag1 = Tag.newBuilder()
                .setDefinition(obj1)
                .putAttr(searchAttr, encodeValue("the_droids_you_are_looking_for"))
                .build();

        var tag2 = Tag.newBuilder()
                .setDefinition(obj2)
                .putAttr(searchAttr, encodeValue("not_the_droids_you_are_looking_for"))
                .build();

        var save1 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(tag1)
                .build();

        var save2 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(tag2)
                .build();

        var id1 = writeApi.saveNewObject(save1);

        // noinspection ResultOfMethodCallIgnored
        writeApi.saveNewObject(save2);

        var searchRequest = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(searchAttr)
                        .setAttrType(BasicType.STRING)
                        .setOperator(SearchOperator.EQ)
                        .setSearchValue(encodeValue("the_droids_you_are_looking_for")))))
                .build();

        var searchResult = searchApi.search(searchRequest);

        // Search results do not include the definition body
        var t1 = tag1.toBuilder()
                .setTagVersion(id1.getTagVersion())
                .setDefinition(tag1.getDefinition().toBuilder()
                .setHeader(ObjectHeader.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(id1.getObjectId())
                .setObjectVersion(id1.getObjectVersion()))
                .clearDefinition())
                .build();

        assertEquals(1, searchResult.getSearchResultCount());
        assertEquals(t1, searchResult.getSearchResult(0));
    }

    @Test
    void compoundSearch() {

        var searchAttr = "compoundSearch_WHICH_DROIDS";
        var searchAttr2 = "compoundSearch_WHERE_ARE_THEY";

        var obj1 = TestData.dummyDataDef(false);
        var obj2 = TestData.dummyDataDef(false);

        var tag1 = Tag.newBuilder()
                .setDefinition(obj1)
                .putAttr(searchAttr, encodeValue("the_droids_you_are_looking_for"))
                .putAttr(searchAttr2, encodeValue("a_galaxy_far_far_away"))
                .build();

        var tag2 = Tag.newBuilder()
                .setDefinition(obj2)
                .putAttr(searchAttr, encodeValue("not_the_droids_you_are_looking_for"))
                .putAttr(searchAttr2, encodeValue("under_your_nose"))
                .build();

        var save1 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(tag1)
                .build();

        var save2 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(tag2)
                .build();

        var id1 = writeApi.saveNewObject(save1);

        // noinspection ResultOfMethodCallIgnored
        writeApi.saveNewObject(save2);

        var searchRequest = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                    .setLogical(LogicalExpression.newBuilder()
                    .setOperator(LogicalOperator.AND)
                    .addExpr(SearchExpression.newBuilder()
                    .setTerm(SearchTerm.newBuilder()
                            .setAttrName(searchAttr)
                            .setAttrType(BasicType.STRING)
                            .setOperator(SearchOperator.EQ)
                            .setSearchValue(encodeValue("the_droids_you_are_looking_for"))))
                    .addExpr(SearchExpression.newBuilder()
                    .setTerm(SearchTerm.newBuilder()
                            .setAttrName(searchAttr2)
                            .setAttrType(BasicType.STRING)
                            .setOperator(SearchOperator.EQ)
                            .setSearchValue(encodeValue("a_galaxy_far_far_away")))))))
                .build();

        var searchResult = searchApi.search(searchRequest);

        // Search results do not include the definition body
        var t1 = tag1.toBuilder()
                .setTagVersion(id1.getTagVersion())
                .setDefinition(tag1.getDefinition().toBuilder()
                .setHeader(ObjectHeader.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(id1.getObjectId())
                .setObjectVersion(id1.getObjectVersion()))
                .clearDefinition())
                .build();

        assertEquals(1, searchResult.getSearchResultCount());
        assertEquals(t1, searchResult.getSearchResult(0));
    }

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED"})
    void allObjectTypes(ObjectType objectType) {

        var searchAttr = "allObjectTypes_" + objectType.name();

        var obj1 = TestData.dummyDefinitionForType(objectType, false);
        var obj2 = TestData.dummyDefinitionForType(objectType, false);

        var tag1 = Tag.newBuilder()
                .setDefinition(obj1)
                .putAttr(searchAttr, encodeValue("the_droids_you_are_looking_for"))
                .build();

        var tag2 = Tag.newBuilder()
                .setDefinition(obj2)
                .putAttr(searchAttr, encodeValue("not_the_droids_you_are_looking_for"))
                .build();

        var save1 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setTag(tag1)
                .build();

        var save2 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setTag(tag2)
                .build();

        var id1 = writeApi.saveNewObject(save1);

        // noinspection ResultOfMethodCallIgnored
        writeApi.saveNewObject(save2);

        var searchRequest = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                .setObjectType(objectType)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(searchAttr)
                        .setAttrType(BasicType.STRING)
                        .setOperator(SearchOperator.EQ)
                        .setSearchValue(encodeValue("the_droids_you_are_looking_for")))))
                .build();

        var searchResult = searchApi.search(searchRequest);

        // Search results do not include the definition body
        var t1 = tag1.toBuilder()
                .setTagVersion(id1.getTagVersion())
                .setDefinition(tag1.getDefinition().toBuilder()
                .setHeader(ObjectHeader.newBuilder()
                        .setObjectType(objectType)
                        .setObjectId(id1.getObjectId())
                        .setObjectVersion(id1.getObjectVersion()))
                .clearDefinition())
                .build();

        assertEquals(1, searchResult.getSearchResultCount());
        assertEquals(t1, searchResult.getSearchResult(0));
    }

    @ParameterizedTest
    @EnumSource(value = BasicType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"BASIC_TYPE_NOT_SET", "UNRECOGNIZED", "ARRAY"})
    void allAttrTypes(BasicType attrType) {

        var attrToLookFor = "allAttrTypes_" + attrType.name();
        var valueToLookFor = objectOfType(attrType);
        var valueNotToLookFor = differentObjectOfSameType(attrType, valueToLookFor);

        var obj1 = TestData.dummyDataDef(false);
        var obj2 = TestData.dummyDataDef(false);

        var tag1 = Tag.newBuilder()
                .setDefinition(obj1)
                .putAttr(attrToLookFor, encodeNativeObject(valueToLookFor))
                .build();

        var tag2 = Tag.newBuilder()
                .setDefinition(obj2)
                .putAttr(attrToLookFor, encodeNativeObject(valueNotToLookFor))
                .build();

        var save1 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(tag1)
                .build();

        var save2 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setTag(tag2)
                .build();

        var id1 = writeApi.saveNewObject(save1);

        // noinspection ResultOfMethodCallIgnored
        writeApi.saveNewObject(save2);

        var searchRequest = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(attrToLookFor)
                        .setAttrType(attrType)
                        .setOperator(SearchOperator.EQ)
                        .setSearchValue(encodeNativeObject(valueToLookFor)))))
                .build();

        var searchResult = searchApi.search(searchRequest);

        // Search results do not include the definition body
        var t1 = tag1.toBuilder()
                .setTagVersion(id1.getTagVersion())
                .setDefinition(tag1.getDefinition().toBuilder()
                .setHeader(ObjectHeader.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setObjectId(id1.getObjectId())
                .setObjectVersion(id1.getObjectVersion()))
                .clearDefinition())
                .build();

        assertEquals(1, searchResult.getSearchResultCount());
        assertEquals(t1, searchResult.getSearchResult(0));
    }

    @Test
    @Disabled("Search result limit not implemented yet")
    void maxResultsLimit() {
        Assertions.fail();
    }

    @Test
    void emptySearchResult() {

        var searchAttr = "emptySearchResult_WHICH_DROIDS";

        var searchRequest = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(searchAttr)
                        .setAttrType(BasicType.STRING)
                        .setOperator(SearchOperator.EQ)
                        .setSearchValue(encodeValue("the_droids_you_are_looking_for")))))
                .build();

        var searchResult = searchApi.search(searchRequest);

        assertEquals(0, searchResult.getSearchResultCount());
    }

    @Test
    @Disabled("Metadata validation not implemented yet")
    void invalidSearch_nullParams() {

        var searchRequest = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .clearSearchParams()
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> searchApi.search(searchRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());

        var searchRequest2 = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .clearSearch())
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error2 = assertThrows(StatusRuntimeException.class, () -> searchApi.search(searchRequest2));
        assertEquals(Status.Code.INVALID_ARGUMENT, error2.getStatus().getCode());
    }

    @Test
    @Disabled("Metadata validation not implemented yet")
    void invalidSearch_badParams() {

        // An invalid search request
        // In this case, a logical NOT operator with two sub-expressions
        // Logical NOT expressions are required to have exactly one sub-expression

        var searchRequest = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                    .setLogical(LogicalExpression.newBuilder()
                    .setOperator(LogicalOperator.NOT)
                    .addExpr(SearchExpression.newBuilder()
                    .setTerm(SearchTerm.newBuilder()
                            .setAttrName("meaning_of_life")
                            .setAttrType(BasicType.INTEGER)
                            .setOperator(SearchOperator.EQ)
                            .setSearchValue(encodeValue(42))))
                    .addExpr(SearchExpression.newBuilder()
                    .setTerm(SearchTerm.newBuilder()
                            .setAttrName("what_is_the_question")
                            .setAttrType(BasicType.STRING)
                            .setOperator(SearchOperator.EQ)
                            .setSearchValue(encodeValue("What do you get if you multiply six by nine?")))))))
                .build();

        // noinspection ResultOfMethodCallIgnored
        var error = assertThrows(StatusRuntimeException.class, () -> searchApi.search(searchRequest));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }
}
