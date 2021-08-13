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

import com.accenture.trac.api.*;
import com.accenture.trac.metadata.*;
import com.accenture.trac.common.metadata.MetadataCodec;
import com.accenture.trac.svc.meta.dal.IMetadataDal;
import com.accenture.trac.svc.meta.services.MetadataReadService;
import com.accenture.trac.svc.meta.services.MetadataSearchService;
import com.accenture.trac.svc.meta.services.MetadataWriteService;
import com.accenture.trac.svc.meta.test.IDalTestable;

import com.accenture.trac.svc.meta.test.JdbcIntegration;
import com.accenture.trac.svc.meta.test.JdbcUnit;
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


abstract class MetadataSearchApiTest implements IDalTestable {

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

    // Include this test case as a unit test
    @ExtendWith(JdbcUnit.class)
    static class Unit extends MetadataSearchApiTest {}

    // Include this test case for integration against different database backends
    @org.junit.jupiter.api.Tag("integration")
    @org.junit.jupiter.api.Tag("int-metadb")
    @ExtendWith(JdbcIntegration.class)
    static class Integration extends MetadataSearchApiTest {}

    @Rule
    final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private TracMetadataApiGrpc.TracMetadataApiBlockingStub searchApi;
    private TrustedMetadataApiGrpc.TrustedMetadataApiBlockingStub writeApi;

    @BeforeEach
    void setup() throws Exception {

        var serverName = InProcessServerBuilder.generateName();

        var readService = new MetadataReadService(dal);
        var writeService = new MetadataWriteService(dal);
        var searchService = new MetadataSearchService(dal);

        var publicApiImpl = new TracMetadataApi(readService, writeService, searchService);
        var trustedApiImpl = new TrustedMetadataApi(readService, writeService, searchService);

        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(InProcessServerBuilder
                .forName(serverName)
                .directExecutor()
                .addService(publicApiImpl)
                .addService(trustedApiImpl)
                .build()
                .start());

        searchApi = TracMetadataApiGrpc.newBlockingStub(
                // Create a client channel and register for automatic graceful shutdown.
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));

        writeApi = TrustedMetadataApiGrpc.newBlockingStub(
                // Create a client channel and register for automatic graceful shutdown.
                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
    }

    @Test
    void basicSearch() {

        var searchAttr = "basicSearch_WHICH_DROIDS";

        var obj1 = TestData.dummyDataDef();
        var obj2 = TestData.dummyDataDef();

        var tag1 = Tag.newBuilder()
                .setDefinition(obj1)
                .putAttrs(searchAttr, encodeValue("the_droids_you_are_looking_for"))
                .build();

        var tag2 = Tag.newBuilder()
                .setDefinition(obj2)
                .putAttrs(searchAttr, encodeValue("not_the_droids_you_are_looking_for"))
                .build();

        var save1 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(obj1)
                .addAllTagUpdates(tagUpdatesForAttrs(tag1.getAttrsMap()))
                .build();

        var save2 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(obj2)
                .addAllTagUpdates(tagUpdatesForAttrs(tag2.getAttrsMap()))
                .build();

        var id1 = writeApi.createObject(save1);

        // noinspection ResultOfMethodCallIgnored
        writeApi.createObject(save2);

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
                .setHeader(id1)
                .clearDefinition()
                .build();

        assertEquals(1, searchResult.getSearchResultCount());
        assertEquals(t1, searchResult.getSearchResult(0));
    }

    @Test
    void compoundSearch() {

        var searchAttr = "compoundSearch_WHICH_DROIDS";
        var searchAttr2 = "compoundSearch_WHERE_ARE_THEY";

        var obj1 = TestData.dummyDataDef();
        var obj2 = TestData.dummyDataDef();

        var tag1 = Tag.newBuilder()
                .setDefinition(obj1)
                .putAttrs(searchAttr, encodeValue("the_droids_you_are_looking_for"))
                .putAttrs(searchAttr2, encodeValue("a_galaxy_far_far_away"))
                .build();

        var tag2 = Tag.newBuilder()
                .setDefinition(obj2)
                .putAttrs(searchAttr, encodeValue("not_the_droids_you_are_looking_for"))
                .putAttrs(searchAttr2, encodeValue("under_your_nose"))
                .build();

        var save1 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(obj1)
                .addAllTagUpdates(tagUpdatesForAttrs(tag1.getAttrsMap()))
                .build();

        var save2 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(obj2)
                .addAllTagUpdates(tagUpdatesForAttrs(tag2.getAttrsMap()))
                .build();

        var id1 = writeApi.createObject(save1);

        // noinspection ResultOfMethodCallIgnored
        writeApi.createObject(save2);

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
                .setHeader(id1)
                .clearDefinition()
                .build();

        assertEquals(1, searchResult.getSearchResultCount());
        assertEquals(t1, searchResult.getSearchResult(0));
    }

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED"})
    void allObjectTypes(ObjectType objectType) {

        var searchAttr = "allObjectTypes_" + objectType.name();

        var obj1 = TestData.dummyDefinitionForType(objectType);
        var obj2 = TestData.dummyDefinitionForType(objectType);

        var tag1 = Tag.newBuilder()
                .setDefinition(obj1)
                .putAttrs(searchAttr, encodeValue("the_droids_you_are_looking_for"))
                .build();

        var tag2 = Tag.newBuilder()
                .setDefinition(obj2)
                .putAttrs(searchAttr, encodeValue("not_the_droids_you_are_looking_for"))
                .build();

        var save1 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setDefinition(obj1)
                .addAllTagUpdates(tagUpdatesForAttrs(tag1.getAttrsMap()))
                .build();

        var save2 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(objectType)
                .setDefinition(obj2)
                .addAllTagUpdates(tagUpdatesForAttrs(tag2.getAttrsMap()))
                .build();

        var id1 = writeApi.createObject(save1);

        // noinspection ResultOfMethodCallIgnored
        writeApi.createObject(save2);

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
                .setHeader(id1)
                .clearDefinition()
                .build();

        assertEquals(1, searchResult.getSearchResultCount());
        assertEquals(t1, searchResult.getSearchResult(0));
    }

    @ParameterizedTest
    @EnumSource(value = BasicType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"BASIC_TYPE_NOT_SET", "UNRECOGNIZED", "ARRAY", "MAP"})
    void allAttrTypes(BasicType attrType) {

        var attrToLookFor = "allAttrTypes_" + attrType.name();
        var valueToLookFor = objectOfType(attrType);
        var valueNotToLookFor = differentObjectOfSameType(attrType, valueToLookFor);

        var obj1 = TestData.dummyDataDef();
        var obj2 = TestData.dummyDataDef();

        var tag1 = Tag.newBuilder()
                .setDefinition(obj1)
                .putAttrs(attrToLookFor, encodeNativeObject(valueToLookFor))
                .build();

        var tag2 = Tag.newBuilder()
                .setDefinition(obj2)
                .putAttrs(attrToLookFor, encodeNativeObject(valueNotToLookFor))
                .build();

        var save1 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(obj1)
                .addAllTagUpdates(tagUpdatesForAttrs(tag1.getAttrsMap()))
                .build();

        var save2 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(obj2)
                .addAllTagUpdates(tagUpdatesForAttrs(tag2.getAttrsMap()))
                .build();

        var id1 = writeApi.createObject(save1);

        // noinspection ResultOfMethodCallIgnored
        writeApi.createObject(save2);

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
                .setHeader(id1)
                .clearDefinition()
                .build();

        assertEquals(1, searchResult.getSearchResultCount());
        assertEquals(t1, searchResult.getSearchResult(0));
    }

    @Test
    void temporalSearch() throws Exception {

        // Single test case to search as of a previous point in time

        var obj1 = dummyDataDef();

        var create1 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(obj1)
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("as_of_attr_1")
                        .setValue(MetadataCodec.encodeValue("initial_value")))
                .build();

        var header1 = writeApi.createObject(create1);
        var header2 = writeApi.createObject(create1);

        // Use a search timestamp after both objects have been created, but before either is updated
        var v1SearchTime = MetadataCodec.decodeDatetime(header2.getTagTimestamp()).plusNanos(5000);

        Thread.sleep(10);

        // Update only one of the two objects

        var update1 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(selectorForTag(header2))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("as_of_attr_1")
                        .setOperation(TagOperation.REPLACE_ATTR)
                        .setValue(MetadataCodec.encodeValue("updated_value")))
                .build();

        // noinspection ResultOfMethodCallIgnored
        writeApi.updateTag(update1);

        // Search without specifying as-of, should return only the object that has the original tag

        var searchRequest = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                        .setTerm(SearchTerm.newBuilder()
                        .setAttrName("as_of_attr_1")
                        .setAttrType(BasicType.STRING)
                        .setOperator(SearchOperator.EQ)
                        .setSearchValue(encodeValue("initial_value")))))
                .build();

        var result = searchApi.search(searchRequest);
        var resultHeader = result.getSearchResult(0).getHeader();

        Assertions.assertEquals(1, result.getSearchResultCount());
        Assertions.assertEquals(header1, resultHeader);

        // Now search with an as-of time before the update was applied, both objects should come back
        // The object created last should be top of the list

        var asOfSearch = searchRequest.toBuilder()
                .setSearchParams(searchRequest.getSearchParams().toBuilder()
                .setSearchAsOf(MetadataCodec.encodeDatetime(v1SearchTime)))
                .build();

        var asOfResult = searchApi.search(asOfSearch);
        var resultHeader2 = asOfResult.getSearchResult(0).getHeader();
        var resultHeader1 = asOfResult.getSearchResult(1).getHeader();

        Assertions.assertEquals(2, asOfResult.getSearchResultCount());
        Assertions.assertEquals(header1, resultHeader1);
        Assertions.assertEquals(header2, resultHeader2);
    }

    @Test
    void priorVersionsAndTags() {

        var v1Obj = dummyDataDef();
        var v2Obj = nextDataDef(v1Obj);

        var create1 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setDefinition(v1Obj)
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("api_prior_search_attr")
                        .setValue(MetadataCodec.encodeValue("initial_value")))
                .build();

        var v1t1Header = writeApi.createObject(create1);

        var create2 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(selectorForTag(v1t1Header))
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("api_prior_search_attr")
                        .setValue(MetadataCodec.encodeValue("modified_value")))
                .build();

        var v1t2Header = writeApi.updateTag(create2);

        var create3 = MetadataWriteRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setObjectType(ObjectType.DATA)
                .setPriorVersion(selectorForTag(v1t2Header))
                .setDefinition(v2Obj)
                .addTagUpdates(TagUpdate.newBuilder()
                        .setAttrName("api_prior_search_attr")
                        .setValue(MetadataCodec.encodeValue("modified_value")))
                .build();

        // noinspection ResultOfMethodCallIgnored
        writeApi.updateObject(create3);

        var searchRequest = MetadataSearchRequest.newBuilder()
                .setTenant(TEST_TENANT)
                .setSearchParams(SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName("api_prior_search_attr")
                        .setAttrType(BasicType.STRING)
                        .setOperator(SearchOperator.EQ)
                        .setSearchValue(encodeValue("initial_value"))))
                .setPriorVersions(true)
                .setPriorTags(true))
                .build();

        var result = searchApi.search(searchRequest);

        Assertions.assertEquals(1, result.getSearchResultCount());
        Assertions.assertEquals(v1t1Header, result.getSearchResult(0).getHeader());
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
