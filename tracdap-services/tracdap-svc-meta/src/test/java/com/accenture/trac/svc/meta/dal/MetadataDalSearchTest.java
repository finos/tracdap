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

package com.accenture.trac.svc.meta.dal;

import org.finos.tracdap.metadata.*;
import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.common.metadata.MetadataCodec;
import org.finos.tracdap.test.meta.IDalTestable;
import org.finos.tracdap.test.meta.JdbcUnit;
import org.finos.tracdap.test.meta.JdbcIntegration;

import org.finos.tracdap.test.meta.TestData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.Object;
import java.math.BigDecimal;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.finos.tracdap.common.metadata.MetadataCodec.encodeArrayValue;
import static org.finos.tracdap.common.metadata.MetadataCodec.encodeValue;
import static org.junit.jupiter.api.Assertions.*;


abstract class MetadataDalSearchTest implements IDalTestable {

    private IMetadataDal dal;

    public void setDal(IMetadataDal dal) {
        this.dal = dal;
    }

    @ExtendWith(JdbcUnit.class)
    static class Unit extends MetadataDalSearchTest {}

    @org.junit.jupiter.api.Tag("integration")
    @org.junit.jupiter.api.Tag("int-metadb")
    @ExtendWith(JdbcIntegration.class)
    static class Integration extends MetadataDalSearchTest {}


    // -----------------------------------------------------------------------------------------------------------------
    // EXAMPLE SEARCHES
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void basicStringSearch() throws Exception {

        var def1 = TestData.dummyDataDef();
        var def2 = TestData.dummyDataDef();

        var tag1 = TestData.dummyTag(def1, INCLUDE_HEADER)
                .toBuilder().clearAttrs()
                .putAttrs("rodent_type", encodeValue("bilge_rat"))
                .putAttrs("rodent_name", encodeValue("Ricky the Rat"))
                .build();

        var tag2 = TestData.dummyTag(def2, INCLUDE_HEADER)
                .toBuilder().clearAttrs()
                .putAttrs("rodent_type", encodeValue("house_mouse"))
                .putAttrs("rodent_name", encodeValue("Casandra McMouse"))
                .build();

        var save = CompletableFuture.completedFuture(0)
                .thenCompose(x -> dal.saveNewObject(TestData.TEST_TENANT, tag1))
                .thenCompose(x -> dal.saveNewObject(TestData.TEST_TENANT, tag2));

        unwrap(save);

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                    .setAttrName("rodent_type")
                    .setAttrType(BasicType.STRING)
                    .setOperator(SearchOperator.EQ)
                    .setSearchValue(encodeValue("bilge_rat"))))
                .build();

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        assertEquals(1, searchResult.size());

        // Search results should come back with no definition body
        var expectedResult = tag1.toBuilder()
                .clearDefinition()
                .build();

        assertEquals(expectedResult, searchResult.get(0));
    }

    @Test
    void complexCompoundSearch() throws Exception {

        // Example query to look for sales data which is not restricted by data protection requirements

        // dataset_class == "sales_report" AND (
        //      record_date < 2020-01-01 OR
        //      NOT ( data_classification IN ["pii", "confidential"] )
        // )

        var def1 = TestData.dummyDataDef();
        var def2 = TestData.dummyDataDef();
        var def3 = TestData.dummyDataDef();
        var def4 = TestData.dummyDataDef();

        var attrNames = List.of("dataset_class", "record_date", "data_classification");

        // Tag 1 should match, recent sales data with classification "internal"
        var tag1 = tagForDef(def1, attrNames, List.of(
                encodeValue("sales_report"),
                encodeValue(LocalDate.of(2018, 6, 1)),
                encodeValue("internal")));

        // Tag 2 should NOT match, it is a similar data set to tag 1 but with an extra classification flag for PII
        var tag2 = tagForDef(def2, attrNames, List.of(
                encodeValue("sales_report"),
                encodeValue(LocalDate.of(2018, 7, 1)),
                encodeArrayValue(List.of("internal", "pii"), TypeSystem.descriptor(BasicType.STRING))));

        // Tag 3 should match, it is confidential data but sufficiently long ago to be ok to use
        var tag3 = tagForDef(def3, attrNames, List.of(
                encodeValue("sales_report"),
                encodeValue(LocalDate.of(1995, 1, 1)),
                encodeValue("confidential")));

        // Tag 4 should NOT match, it is not a sales report
        var tag4 = tagForDef(def4, attrNames, List.of(
                encodeValue("team_social_fund"),
                encodeValue(LocalDate.of(2018, 6, 1)),
                encodeValue("internal")));

        var tags = List.of(tag1, tag2, tag3, tag4);
        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, tags));

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setLogical(LogicalExpression.newBuilder()
                .setOperator(LogicalOperator.AND)
                    .addExpr(searchTerm("dataset_class", BasicType.STRING, SearchOperator.EQ, encodeValue("sales_report")))
                    .addExpr(SearchExpression.newBuilder()
                        .setLogical(LogicalExpression.newBuilder()
                        .setOperator(LogicalOperator.OR)
                        .addExpr(searchTerm("record_date", BasicType.DATE, SearchOperator.LT, encodeValue(LocalDate.of(2000, 1, 1))))
                        .addExpr(SearchExpression.newBuilder()
                            .setLogical(LogicalExpression.newBuilder()
                            .setOperator(LogicalOperator.NOT)
                            .addExpr(searchTerm("data_classification", BasicType.STRING, SearchOperator.IN, encodeArrayValue(
                                    List.of("pii", "confidential"), TypeSystem.descriptor(BasicType.STRING))))))))))

                .build();

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        var t1 = clearDefinitionBody(tag1);
        var t3 = clearDefinitionBody(tag3);

        assertEquals(2, searchResult.size());
        assertEquals(Set.of(t1, t3), Set.copyOf(searchResult));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // CORE SEARCH FEATURES
    // -----------------------------------------------------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(value = ObjectType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"OBJECT_TYPE_NOT_SET", "UNRECOGNIZED"})
    void objectType(ObjectType objectType) throws Exception {

        // Create two objects with the same tags but different object type

        var otherType = objectType == ObjectType.DATA ? ObjectType.MODEL : ObjectType.DATA;

        var attrToLookFor = "object_type_test_for_" + objectType.name();
        var def1 = TestData.dummyDefinitionForType(objectType);
        var def2 = TestData.dummyDefinitionForType(otherType);

        var tag1 = tagForDef(def1, attrToLookFor, encodeValue("bilge_rat"));
        var tag2 = tagForDef(def2, attrToLookFor, encodeValue("bilge_rat"));
        var tags = List.of(tag1, tag2);

        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, tags));

        // Search should respect object type even if all other criteria are matched

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(objectType)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(attrToLookFor)
                        .setOperator(SearchOperator.EQ)
                        .setAttrType(BasicType.STRING)
                        .setSearchValue(MetadataCodec.encodeNativeObject("bilge_rat"))))
                .build();

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        var t1 = clearDefinitionBody(tag1);

        assertEquals(1, searchResult.size());
        assertEquals(t1, searchResult.get(0));
    }

    @Test
    void emptySearchResult() throws Exception {

        // Corner case - A search that does not match anything should return an empty result
        // It is not an error condition

        var searchAttrName = "missing_droids";
        var searchAttrValue = "the_droids_you_are_looking_for";

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                        .setTerm(SearchTerm.newBuilder()
                                .setAttrName(searchAttrName)
                                .setOperator(SearchOperator.EQ)
                                .setAttrType(BasicType.STRING)
                                .setSearchValue(MetadataCodec.encodeNativeObject(searchAttrValue))))
                .build();

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        assertEquals(0, searchResult.size());
    }


    // -----------------------------------------------------------------------------------------------------------------
    // SEARCH TERMS
    // -----------------------------------------------------------------------------------------------------------------

    // EQUALITY

    @ParameterizedTest
    @EnumSource(value = BasicType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"BASIC_TYPE_NOT_SET", "UNRECOGNIZED", "ARRAY", "MAP"})
    void searchTerm_eq(BasicType basicType) throws Exception {

        var attrToLookFor = "eq_search_test_" + basicType.name();
        var valueToLookFor = objectOfType(basicType);

        var testTags = equalityTestTags(basicType, attrToLookFor, valueToLookFor);

        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, testTags));

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(attrToLookFor)
                        .setOperator(SearchOperator.EQ)
                        .setAttrType(basicType)
                        .setSearchValue(MetadataCodec.encodeNativeObject(valueToLookFor))))
                .build();

        // Equality should match only one of the four tags in the test set
        // Everything must match - attr name, type and value

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        // Search results should come back with no definition body
        var tag1 = clearDefinitionBody(testTags.get(0));

        assertEquals(1, searchResult.size());
        assertEquals(tag1, searchResult.get(0));
    }

    @ParameterizedTest
    @EnumSource(value = BasicType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"BASIC_TYPE_NOT_SET", "UNRECOGNIZED", "BOOLEAN", "ARRAY", "MAP"})
    void searchTerm_eqArray(BasicType basicType) throws Exception {

        // Note: Boolean array attrs are not allowed
        // This should be rejected as invalid at the API level

        var attrToLookFor = "eq_array_search_test_" + basicType.name();
        var valueToLookFor = objectOfType(basicType);
        var testTags = equalityArrayTestTags(basicType, attrToLookFor, valueToLookFor);

        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, testTags));

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(attrToLookFor)
                        .setOperator(SearchOperator.EQ)
                        .setAttrType(basicType)
                        .setSearchValue(MetadataCodec.encodeNativeObject(valueToLookFor))))
                .build();

        // Equality should match only one of the four tags in the test set
        // Everything must match for at least one element in the array - attr name, type and value

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        // Search results should come back with no definition body
        var tag1 = clearDefinitionBody(testTags.get(0));

        assertEquals(1, searchResult.size());
        assertEquals(tag1, searchResult.get(0));
    }

    @ParameterizedTest
    @EnumSource(value = BasicType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"BASIC_TYPE_NOT_SET", "UNRECOGNIZED", "ARRAY", "MAP"})
    void searchTerm_ne(BasicType basicType) throws Exception {

        var markerAttr = "ne_search_marker_" + basicType.name();
        var attrToLookFor = "ne_search_test_" + basicType.name();
        var valueToLookFor = objectOfType(basicType);
        var testTags = equalityTestTags(basicType, attrToLookFor, valueToLookFor);

        // Searching with just a NE search term will match every object saved in this test class!
        // Apply a marker attr to each tag and use a logical AND to filter items created in this test case

        testTags = testTags.stream().map(t -> t.toBuilder()
            .putAttrs(markerAttr, encodeValue("negative_test_marker"))
            .build())
            .collect(Collectors.toList());

        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, testTags));

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setLogical(LogicalExpression.newBuilder()
                .setOperator(LogicalOperator.AND)
                .addExpr(searchTerm(markerAttr, BasicType.STRING, SearchOperator.EQ, encodeValue("negative_test_marker")))
                .addExpr(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(attrToLookFor)
                        .setOperator(SearchOperator.NE)
                        .setAttrType(basicType)
                        .setSearchValue(MetadataCodec.encodeNativeObject(valueToLookFor))))))
                .build();

        // The not-equals operator should match everything not matched by the equality operator
        // It follows that missing attributes or attributes with a different type do match this operator
        // Consistency can be expressed formally as {t : Attrs(t, A) != X} = {t : !( Attrs(t, A) = X )}

        // In this case we should match all but the first tag in the test set
        // For a single search term we are not concerned about order, so test the result as a set

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));
        var searchResultSet = Set.copyOf(searchResult);

        var expectedResult = Set.copyOf(testTags.subList(1, 4).stream()
                .map(this::clearDefinitionBody)
                .collect(Collectors.toList()));

        assertEquals(3, searchResult.size());
        assertEquals(expectedResult, searchResultSet);
    }

    @ParameterizedTest
    @EnumSource(value = BasicType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"BASIC_TYPE_NOT_SET", "UNRECOGNIZED", "BOOLEAN", "ARRAY", "MAP"})
    void searchTerm_neArray(BasicType basicType) throws Exception {

        // Note: Boolean array attrs are not allowed
        // This should be rejected as invalid at the API level

        var markerAttr = "ne_array_search_marker_" + basicType.name();
        var attrToLookFor = "ne_array_search_test_" + basicType.name();
        var valueToLookFor = objectOfType(basicType);
        var testTags = equalityArrayTestTags(basicType, attrToLookFor, valueToLookFor);

        // Searching with just a NE search term will match every object saved in this test class!
        // Apply a marker attr to each tag and use a logical AND to filter items created in this test case

        testTags = testTags.stream().map(t -> t.toBuilder()
                .putAttrs(markerAttr, encodeValue("negative_test_marker"))
                .build())
                .collect(Collectors.toList());

        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, testTags));

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setLogical(LogicalExpression.newBuilder()
                .setOperator(LogicalOperator.AND)
                .addExpr(searchTerm(markerAttr, BasicType.STRING, SearchOperator.EQ, encodeValue("negative_test_marker")))
                .addExpr(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(attrToLookFor)
                        .setOperator(SearchOperator.NE)
                        .setAttrType(basicType)
                        .setSearchValue(MetadataCodec.encodeNativeObject(valueToLookFor))))))
                .build();

        // The not-equals operator should match everything not matched by the equality operator
        // This condition applies for multi-valued attrs the same as for regular attrs
        // Consistency can be expressed formally as {t : Attrs(t, A) !in X} = {t : !( Attrs(t, A) in X )}
        // Single-value attrs are also covered by this general definition, just with |Attrs(t, A)| = 1

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));
        var searchResultSet = Set.copyOf(searchResult);

        var expectedResult = Set.copyOf(testTags.subList(1, 4).stream()
                .map(this::clearDefinitionBody)
                .collect(Collectors.toList()));

        assertEquals(3, searchResult.size());
        assertEquals(expectedResult, searchResultSet);
    }

    private List<Tag> equalityTestTags(BasicType basicType, String attrToLookFor, Object valueToLookFor) {

        var def1 = TestData.dummyDataDef();
        var def2 = TestData.nextDataDef(def1);
        var def3 = TestData.nextDataDef(def2);
        var def4 = TestData.nextDataDef(def3);

        var attr_value_2 = differentObjectOfSameType(basicType, valueToLookFor);
        var attr_value_3 = objectOfDifferentType(basicType);

        var tag1 = tagForDef(def1, attrToLookFor, MetadataCodec.encodeNativeObject(valueToLookFor));

        // Wrong attr value - should not match
        var tag2 = tagForDef(def2, attrToLookFor, MetadataCodec.encodeNativeObject(attr_value_2));

        // Wrong attr type - should not match
        var tag3 = tagForDef(def3, attrToLookFor, MetadataCodec.encodeNativeObject(attr_value_3));

        // Wrong attr name - should not match
        var tag4 = tagForDef(def4, attrToLookFor + "_NOT", MetadataCodec.encodeNativeObject(valueToLookFor));

        return List.of(tag1, tag2, tag3, tag4);
    }

    private List<Tag> equalityArrayTestTags(BasicType basicType, String attrToLookFor, Object valueYouAreLookingFor) {

        var arrayType = TypeSystem.descriptor(basicType);

        var def1 = TestData.dummyDataDef();
        var def2 = TestData.nextDataDef(def1);
        var def3 = TestData.nextDataDef(def2);
        var def4 = TestData.nextDataDef(def3);

        var attr_value_2 = differentObjectOfSameType(basicType, valueYouAreLookingFor);
        var attr_value_3 = differentObjectOfSameType(basicType, attr_value_2);
        var arrayYouAreLookingFor = MetadataCodec.encodeArrayValue(List.of(valueYouAreLookingFor, attr_value_2), arrayType);
        var notTheArrayYourLookingFor = MetadataCodec.encodeArrayValue(List.of(attr_value_2, attr_value_3), arrayType);

        var attr_value_4 = objectOfDifferentType(basicType);
        var wrongType = TypeSystem.descriptor(attr_value_4.getClass());
        var attr_value_5 = differentObjectOfSameType(wrongType.getBasicType(), attr_value_4);
        var arrayWrongType = MetadataCodec.encodeArrayValue(List.of(attr_value_4, attr_value_5), wrongType);

        var tag1 = tagForDef(def1, attrToLookFor, arrayYouAreLookingFor);

        // Wrong attr value - should not match
        var tag2 = tagForDef(def2, attrToLookFor, notTheArrayYourLookingFor);

        // Wrong attr type - should not match
        var tag3 = tagForDef(def3, attrToLookFor, MetadataCodec.encodeNativeObject(attr_value_3));

        // Wrong attr name - should not match
        var tag4 = tagForDef(def4, attrToLookFor + "_NOT", arrayWrongType);

        return List.of(tag1, tag2, tag3, tag4);
    }


    // INEQUALITY

    @ParameterizedTest
    @EnumSource(value = BasicType.class, mode = EnumSource.Mode.INCLUDE,
            names = {"INTEGER", "FLOAT", "DECIMAL", "DATE", "DATETIME"})
    void searchTerm_gt(BasicType basicType) throws Exception {

        // Create a set of ordered tags, with t0 < t1 < t2
        var attrToLookFor = "gt_search_test_" + basicType.name();
        var orderedValues = orderedAttrValues(basicType);
        var orderedTags = orderedTestTags(basicType, attrToLookFor, orderedValues);
        var valueToLookFor = orderedValues.get(1);  // Middle value in a list of 3

        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, orderedTags));

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(attrToLookFor)
                        .setOperator(SearchOperator.GT)
                        .setAttrType(basicType)
                        .setSearchValue(MetadataCodec.encodeNativeObject(valueToLookFor))))
                .build();

        // GT operator should match values > middleValue, i.e. t2

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        // Search results should come back with no definition body
        var t2 = clearDefinitionBody(orderedTags.get(2));

        assertEquals(1, searchResult.size());
        assertEquals(t2, searchResult.get(0));
    }

    @ParameterizedTest
    @EnumSource(value = BasicType.class, mode = EnumSource.Mode.INCLUDE,
            names = {"INTEGER", "FLOAT", "DECIMAL", "DATE", "DATETIME"})
    void searchTerm_ge(BasicType basicType) throws Exception {

        // Create a set of ordered tags, with t0 < t1 < t2
        var attrToLookFor = "ge_search_test_" + basicType.name();
        var orderedValues = orderedAttrValues(basicType);
        var orderedTags = orderedTestTags(basicType, attrToLookFor, orderedValues);
        var valueToLookFor = orderedValues.get(1);  // Middle value in a list of 3

        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, orderedTags));

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(attrToLookFor)
                        .setOperator(SearchOperator.GE)
                        .setAttrType(basicType)
                        .setSearchValue(MetadataCodec.encodeNativeObject(valueToLookFor))))
                .build();

        // GE operator should match values >= middleValue, i.e. t1 and t2

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        // Search results should come back with no definition body
        var t1 = clearDefinitionBody(orderedTags.get(1));
        var t2 = clearDefinitionBody(orderedTags.get(2));

        assertEquals(2, searchResult.size());
        assertEquals(Set.of(t1, t2), Set.copyOf(searchResult));
    }

    @ParameterizedTest
    @EnumSource(value = BasicType.class, mode = EnumSource.Mode.INCLUDE,
            names = {"INTEGER", "FLOAT", "DECIMAL", "DATE", "DATETIME"})
    void searchTerm_lt(BasicType basicType) throws Exception {

        // Create a set of ordered tags, with t0 < t1 < t2
        var attrToLookFor = "lt_search_test_" + basicType.name();
        var orderedValues = orderedAttrValues(basicType);
        var orderedTags = orderedTestTags(basicType, attrToLookFor, orderedValues);
        var valueToLookFor = orderedValues.get(1);  // Middle value in a list of 3

        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, orderedTags));

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(attrToLookFor)
                        .setOperator(SearchOperator.LT)
                        .setAttrType(basicType)
                        .setSearchValue(MetadataCodec.encodeNativeObject(valueToLookFor))))
                .build();

        // LT operator should match values < middleValue, i.e. t0

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        // Search results should come back with no definition body
        var t0 = clearDefinitionBody(orderedTags.get(0));

        assertEquals(1, searchResult.size());
        assertEquals(t0, searchResult.get(0));
    }

    @ParameterizedTest
    @EnumSource(value = BasicType.class, mode = EnumSource.Mode.INCLUDE,
            names = {"INTEGER", "FLOAT", "DECIMAL", "DATE", "DATETIME"})
    void searchTerm_le(BasicType basicType) throws Exception {

        // Create a set of ordered tags, with t0 < t1 < t2
        var attrToLookFor = "le_search_test_" + basicType.name();
        var orderedValues = orderedAttrValues(basicType);
        var orderedTags = orderedTestTags(basicType, attrToLookFor, orderedValues);
        var valueToLookFor = orderedValues.get(1);  // Middle value in a list of 3

        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, orderedTags));

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(attrToLookFor)
                        .setOperator(SearchOperator.LE)
                        .setAttrType(basicType)
                        .setSearchValue(MetadataCodec.encodeNativeObject(valueToLookFor))))
                .build();

        // LE operator should match values <= middleValue, i.e. t0 and t1

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        // Search results should come back with no definition body
        var t0 = clearDefinitionBody(orderedTags.get(0));
        var t1 = clearDefinitionBody(orderedTags.get(1));

        assertEquals(2, searchResult.size());
        assertEquals(Set.of(t0, t1), Set.copyOf(searchResult));
    }

    @ParameterizedTest
    @EnumSource(value = BasicType.class, mode = EnumSource.Mode.INCLUDE,
            names = {"INTEGER", "FLOAT", "DECIMAL", "DATE", "DATETIME"})
    void searchTerm_arraysOrderedSearch(BasicType basicType) throws Exception {

        // Perform an ordered search against multi-valued attrs
        // This should never match

        // Create a set of ordered values, with v0 < v1 < v2

        var attrToLookFor = "attr_to_look_for_INEQUALITY_ARRAY_" + basicType.name();
        var attrValues = orderedAttrValues(basicType);

        // Create a tag with a multi-valued attr
        var def = TestData.dummyDataDef();
        var tag = TestData.dummyTag(def, INCLUDE_HEADER)
                .toBuilder().clearAttrs()
                .putAttrs(attrToLookFor, MetadataCodec.encodeArrayValue(attrValues, TypeSystem.descriptor(basicType)))
                .build();

        unwrap(dal.saveNewObject(TestData.TEST_TENANT, tag));

        var inequalities = Set.of(
                SearchOperator.GT,
                SearchOperator.GE,
                SearchOperator.LT,
                SearchOperator.LE);

        // Test searches with all inequality operators
        // Use the middle of the ordered value list as the search term
        for (var operator : inequalities) {

            var v1 = attrValues.get(1);

            var searchParams = SearchParameters.newBuilder()
                    .setObjectType(ObjectType.DATA)
                    .setSearch(SearchExpression.newBuilder()
                    .setTerm(SearchTerm.newBuilder()
                            .setAttrName(attrToLookFor)
                            .setOperator(operator)
                            .setAttrType(basicType)
                            .setSearchValue(MetadataCodec.encodeNativeObject(v1))))
                    .build();

            var result = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

            // Inequalities should never match against an array value
            // It is not valid to perform ordered comparison of a single value against a set

            assertEquals(0, result.size());
        }
    }

    private List<Tag> orderedTestTags(BasicType basicType, String attrToLookFor, List<Object> attrValues) {

        var def1 = TestData.dummyDataDef();
        var def2 = TestData.nextDataDef(def1);
        var def3 = TestData.nextDataDef(def2);
        var def4 = TestData.dummyModelDef();

        var defs = List.of(def1, def2, def3);
        var tags = new ArrayList<Tag>();

        for (var i = 0; i < defs.size(); i++) {

            var tag = TestData.dummyTag(defs.get(i), INCLUDE_HEADER)
                    .toBuilder()
                    .clearAttrs()
                    .putAttrs(attrToLookFor, MetadataCodec.encodeNativeObject(attrValues.get(i)))
                    .build();

            tags.add(tag);
        }

        // Also add a tag with the wrong type
        // This should not get picked up by any of the inequality operators
        var attrValueWrongType = objectOfDifferentType(basicType);
        var tagWrongType = TestData.dummyTag(def4, INCLUDE_HEADER)
                .toBuilder()
                .clearAttrs()
                .putAttrs(attrToLookFor, MetadataCodec.encodeNativeObject(attrValueWrongType))
                .build();

        tags.add(tagWrongType);

        return tags;
    }

    private List<Object> orderedAttrValues(BasicType basicType) {

        switch (basicType) {

            case INTEGER: return List.of(-1, 2, 3);
            case FLOAT: return List.of(-1.0, 2.0, 3.0);

            case DECIMAL: return List.of(
                    new BigDecimal("-123.456"),
                    BigDecimal.valueOf(0),
                    new BigDecimal("123.456"));

            case DATE: return List.of(
                    LocalDate.now().minusWeeks(1),
                    LocalDate.now(),
                    LocalDate.now().plusWeeks(1));

            case DATETIME: return List.of(
                    OffsetDateTime.now(ZoneOffset.UTC).minusWeeks(1),
                    OffsetDateTime.now(ZoneOffset.UTC),
                    OffsetDateTime.now(ZoneOffset.UTC).plusWeeks(1))

                    // Metadata datetime attrs are at microsecond precision
                    .stream().map(TestData::truncateMicrosecondPrecision)
                    .collect(Collectors.toList());

            default:
                throw new RuntimeException("Cannot create ordered values for unordered type " + basicType.toString());
        }
    }


    // IN OPERATOR

    @ParameterizedTest
    @EnumSource(value = BasicType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"BASIC_TYPE_NOT_SET", "UNRECOGNIZED", "BOOLEAN", "ARRAY", "MAP"})
    void searchTerm_in(BasicType basicType) throws Exception {

        // Note: IN query for BOOLEAN attr is not allowed
        // This should be rejected as invalid at the API level

        var def1 = TestData.dummyDataDef();
        var def2 = TestData.nextDataDef(def1);
        var def3 = TestData.nextDataDef(def2);
        var def4 = TestData.nextDataDef(def3);
        var def5 = TestData.nextDataDef(def4);

        var attrToLookFor = "attr_to_look_for_IN_" + basicType.name();
        var valueToLookFor = objectOfType(basicType);
        var attr_value_2 = differentObjectOfSameType(basicType, valueToLookFor);
        var attr_value_3 = differentObjectOfSameType(basicType, attr_value_2);
        var attr_value_4 = objectOfDifferentType(basicType);

        var tag1 = tagForDef(def1, attrToLookFor, MetadataCodec.encodeNativeObject(valueToLookFor));
        var tag2 = tagForDef(def2, attrToLookFor, MetadataCodec.encodeNativeObject(attr_value_2));

        // Tags 3, 4, 5 should not match
        var tag3 = tagForDef(def3, attrToLookFor, MetadataCodec.encodeNativeObject(attr_value_3));
        var tag4 = tagForDef(def4, attrToLookFor, MetadataCodec.encodeNativeObject(attr_value_4));
        var tag5 = tagForDef(def5, "not_" + attrToLookFor, MetadataCodec.encodeNativeObject(valueToLookFor));

        var tags = List.of(tag1, tag2, tag3, tag4, tag5);
        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, tags));

        var searchValues = List.of(valueToLookFor, attr_value_2);

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(attrToLookFor)
                        .setOperator(SearchOperator.IN)
                        .setAttrType(basicType)
                        .setSearchValue(MetadataCodec.encodeArrayValue(searchValues, TypeSystem.descriptor(basicType)))))
                .build();

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        var t1 = clearDefinitionBody(tag1);
        var t2 = clearDefinitionBody(tag2);

        assertEquals(2, searchResult.size());
        assertEquals(Set.of(t1, t2), Set.copyOf(searchResult));
    }

    @ParameterizedTest
    @EnumSource(value = BasicType.class, mode = EnumSource.Mode.EXCLUDE,
                names = {"BASIC_TYPE_NOT_SET", "UNRECOGNIZED", "BOOLEAN", "ARRAY", "MAP"})
    void searchTerm_inArray(BasicType basicType) throws Exception {

        // Note: IN query for BOOLEAN attr is not allowed
        // Note: Also BOOLEAN array attrs are not allowed
        // This should be rejected as invalid at the API level

        var def1 = TestData.dummyDataDef();
        var def2 = TestData.nextDataDef(def1);
        var def3 = TestData.nextDataDef(def2);
        var def4 = TestData.nextDataDef(def3);
        var def5 = TestData.nextDataDef(def4);

        var attrToLookFor = "attr_to_look_for_IN_ARRAY_" + basicType.name();
        var valueToLookFor = objectOfType(basicType);
        var attr_value_2 = differentObjectOfSameType(basicType, valueToLookFor);
        var attr_value_3 = differentObjectOfSameType(basicType, attr_value_2);
        var attr_value_4 = differentObjectOfSameType(basicType, attr_value_2);
        var attr_value_5 = objectOfDifferentType(basicType);
        var attr_value_6 = differentObjectOfSameType(TypeSystem.basicType(attr_value_5.getClass()), attr_value_5);

        var array1 = List.of(valueToLookFor, attr_value_3);
        var array2 = List.of(attr_value_2, attr_value_4);
        var array3 = List.of(attr_value_3, attr_value_4);
        var array4 = List.of(attr_value_5, attr_value_6);

        var typeToLookFor = TypeSystem.descriptor(basicType);
        var otherType = TypeSystem.descriptor(attr_value_5.getClass());

        var tag1 = tagForDef(def1, attrToLookFor, MetadataCodec.encodeArrayValue(array1, typeToLookFor));
        var tag2 = tagForDef(def2, attrToLookFor, MetadataCodec.encodeArrayValue(array2, typeToLookFor));

        // Tags 3, 4, 5 should not match
        var tag3 = tagForDef(def3, attrToLookFor, MetadataCodec.encodeArrayValue(array3, typeToLookFor));
        var tag4 = tagForDef(def4, attrToLookFor, MetadataCodec.encodeArrayValue(array4, otherType));
        var tag5 = tagForDef(def5, "not_" + attrToLookFor, MetadataCodec.encodeArrayValue(array1, typeToLookFor));

        var tags = List.of(tag1, tag2, tag3, tag4, tag5);
        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, tags));

        var searchValues = List.of(valueToLookFor, attr_value_2);

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(attrToLookFor)
                        .setOperator(SearchOperator.IN)
                        .setAttrType(basicType)
                        .setSearchValue(MetadataCodec.encodeArrayValue(searchValues, typeToLookFor))))
                .build();

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        var t1 = clearDefinitionBody(tag1);
        var t2 = clearDefinitionBody(tag2);

        assertEquals(2, searchResult.size());
        assertEquals(Set.of(t1, t2), Set.copyOf(searchResult));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // LOGICAL OPERATORS
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void logicalAnd_singleValues() throws Exception {

        var attrName1 = "search_attr_AND_SINGLE_1";
        var attrName2 = "search_attr_AND_SINGLE_2";
        var attrNames = List.of(attrName1, attrName2);

        var def1 = TestData.dummyDataDef();
        var def2 = TestData.nextDataDef(def1);
        var def3 = TestData.nextDataDef(def2);
        var def4 = TestData.nextDataDef(def3);

        var tag1 = tagForDef(def1, attrNames, List.of(encodeValue("match_1"), encodeValue("match_2")));
        var tag2 = tagForDef(def2, attrNames, List.of(encodeValue("match_1"), encodeValue("not_match_2")));
        var tag3 = tagForDef(def3, attrNames, List.of(encodeValue("not_match_1"), encodeValue("match_2")));
        var tag4 = tagForDef(def4, attrNames, List.of(encodeValue("not_match_1"), encodeValue("not_match_2")));

        var tags = List.of(tag1, tag2, tag3, tag4);
        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, tags));

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setLogical(LogicalExpression.newBuilder()
                        .setOperator(LogicalOperator.AND)
                        .addExpr(searchTerm(attrName1, BasicType.STRING, SearchOperator.EQ, encodeValue("match_1")))
                        .addExpr(searchTerm(attrName2, BasicType.STRING, SearchOperator.EQ, encodeValue("match_2")))))
                .build();

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        var t1 = clearDefinitionBody(tag1);

        assertEquals(1, searchResult.size());
        assertEquals(Set.of(t1), Set.copyOf(searchResult));
    }

    @Test
    void logicalAnd_arrayValues() throws Exception {

        var attrName1 = "search_attr_AND_ARRAY_1";
        var attrName2 = "search_attr_AND_ARRAY_2";
        var attrNames = List.of(attrName1, attrName2);

        var def1 = TestData.dummyDataDef();
        var def2 = TestData.nextDataDef(def1);
        var def3 = TestData.nextDataDef(def2);
        var def4 = TestData.nextDataDef(def3);

        var match1List = List.of("match_1", "match_1a", "match_1b");
        var match2List = List.of("match_2", "match_2a", "match_2b");
        var notMatch1List = List.of("match_1a", "match_1b");
        var notMatch2List = List.of("match_2a", "match_2b");
        var typeDesc = TypeSystem.descriptor(BasicType.STRING);

        var tag1 = tagForDef(def1, attrNames, List.of(encodeArrayValue(match1List, typeDesc), encodeArrayValue(match2List, typeDesc)));
        var tag2 = tagForDef(def2, attrNames, List.of(encodeArrayValue(match1List, typeDesc), encodeArrayValue(notMatch2List, typeDesc)));
        var tag3 = tagForDef(def3, attrNames, List.of(encodeArrayValue(notMatch1List, typeDesc), encodeArrayValue(match2List, typeDesc)));
        var tag4 = tagForDef(def4, attrNames, List.of(encodeArrayValue(notMatch1List, typeDesc), encodeArrayValue(notMatch2List, typeDesc)));

        var tags = List.of(tag1, tag2, tag3, tag4);
        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, tags));

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setLogical(LogicalExpression.newBuilder()
                        .setOperator(LogicalOperator.AND)
                        .addExpr(searchTerm(attrName1, BasicType.STRING, SearchOperator.EQ, encodeValue("match_1")))
                        .addExpr(searchTerm(attrName2, BasicType.STRING, SearchOperator.EQ, encodeValue("match_2")))))
                .build();

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        var t1 = clearDefinitionBody(tag1);

        assertEquals(1, searchResult.size());
        assertEquals(Set.of(t1), Set.copyOf(searchResult));
    }

    @Test
    void logicalAnd_mixedValues() throws Exception {

        var attrName1 = "search_attr_AND_MIXED_1";
        var attrName2 = "search_attr_AND_MIXED_2";
        var attrNames = List.of(attrName1, attrName2);

        var def1 = TestData.dummyDataDef();
        var def2 = TestData.nextDataDef(def1);
        var def3 = TestData.nextDataDef(def2);
        var def4 = TestData.nextDataDef(def3);

        var match2List = List.of("match_2", "match_2a", "match_2b");
        var notMatch2List = List.of("match_2a", "match_2b");
        var typeDesc = TypeSystem.descriptor(BasicType.STRING);

        var tag1 = tagForDef(def1, attrNames, List.of(encodeValue("match_1"), encodeArrayValue(match2List, typeDesc)));
        var tag2 = tagForDef(def2, attrNames, List.of(encodeValue("match_1"), encodeArrayValue(notMatch2List, typeDesc)));
        var tag3 = tagForDef(def3, attrNames, List.of(encodeValue("not_match_1"), encodeArrayValue(match2List, typeDesc)));
        var tag4 = tagForDef(def4, attrNames, List.of(encodeValue("not_match_1"), encodeArrayValue(notMatch2List, typeDesc)));

        var tags = List.of(tag1, tag2, tag3, tag4);
        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, tags));

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setLogical(LogicalExpression.newBuilder()
                        .setOperator(LogicalOperator.AND)
                        .addExpr(searchTerm(attrName1, BasicType.STRING, SearchOperator.EQ, encodeValue("match_1")))
                        .addExpr(searchTerm(attrName2, BasicType.STRING, SearchOperator.EQ, encodeValue("match_2")))))
                .build();

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        var t1 = clearDefinitionBody(tag1);

        assertEquals(1, searchResult.size());
        assertEquals(Set.of(t1), Set.copyOf(searchResult));
    }

    @Test
    void logicalOr_singleValues() throws Exception {

        var attrName1 = "search_attr_OR_SINGLE_1";
        var attrName2 = "search_attr_OR_SINGLE_2";
        var attrNames = List.of(attrName1, attrName2);

        var def1 = TestData.dummyDataDef();
        var def2 = TestData.nextDataDef(def1);
        var def3 = TestData.nextDataDef(def2);
        var def4 = TestData.nextDataDef(def3);

        var tag1 = tagForDef(def1, attrNames, List.of(encodeValue("match_1"), encodeValue("match_2")));
        var tag2 = tagForDef(def2, attrNames, List.of(encodeValue("match_1"), encodeValue("not_match_2")));
        var tag3 = tagForDef(def3, attrNames, List.of(encodeValue("not_match_1"), encodeValue("match_2")));
        var tag4 = tagForDef(def4, attrNames, List.of(encodeValue("not_match_1"), encodeValue("not_match_2")));

        var tags = List.of(tag1, tag2, tag3, tag4);
        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, tags));

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setLogical(LogicalExpression.newBuilder()
                        .setOperator(LogicalOperator.OR)
                        .addExpr(searchTerm(attrName1, BasicType.STRING, SearchOperator.EQ, encodeValue("match_1")))
                        .addExpr(searchTerm(attrName2, BasicType.STRING, SearchOperator.EQ, encodeValue("match_2")))))
                .build();

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        var t1 = clearDefinitionBody(tag1);
        var t2 = clearDefinitionBody(tag2);
        var t3 = clearDefinitionBody(tag3);

        assertEquals(3, searchResult.size());
        assertEquals(Set.of(t1, t2, t3), Set.copyOf(searchResult));
    }

    @Test
    void logicalOr_arrayValues() throws Exception {

        var attrName1 = "search_attr_OR_ARRAY_1";
        var attrName2 = "search_attr_OR_ARRAY_2";
        var attrNames = List.of(attrName1, attrName2);

        var def1 = TestData.dummyDataDef();
        var def2 = TestData.nextDataDef(def1);
        var def3 = TestData.nextDataDef(def2);
        var def4 = TestData.nextDataDef(def3);

        var match1List = List.of("match_1", "match_1a", "match_1b");
        var match2List = List.of("match_2", "match_2a", "match_2b");
        var notMatch1List = List.of("match_1a", "match_1b");
        var notMatch2List = List.of("match_2a", "match_2b");
        var typeDesc = TypeSystem.descriptor(BasicType.STRING);

        var tag1 = tagForDef(def1, attrNames, List.of(encodeArrayValue(match1List, typeDesc), encodeArrayValue(match2List, typeDesc)));
        var tag2 = tagForDef(def2, attrNames, List.of(encodeArrayValue(match1List, typeDesc), encodeArrayValue(notMatch2List, typeDesc)));
        var tag3 = tagForDef(def3, attrNames, List.of(encodeArrayValue(notMatch1List, typeDesc), encodeArrayValue(match2List, typeDesc)));
        var tag4 = tagForDef(def4, attrNames, List.of(encodeArrayValue(notMatch1List, typeDesc), encodeArrayValue(notMatch2List, typeDesc)));

        var tags = List.of(tag1, tag2, tag3, tag4);
        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, tags));

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setLogical(LogicalExpression.newBuilder()
                        .setOperator(LogicalOperator.OR)
                        .addExpr(searchTerm(attrName1, BasicType.STRING, SearchOperator.EQ, encodeValue("match_1")))
                        .addExpr(searchTerm(attrName2, BasicType.STRING, SearchOperator.EQ, encodeValue("match_2")))))
                .build();

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        var t1 = clearDefinitionBody(tag1);
        var t2 = clearDefinitionBody(tag2);
        var t3 = clearDefinitionBody(tag3);

        assertEquals(3, searchResult.size());
        assertEquals(Set.of(t1, t2, t3), Set.copyOf(searchResult));
    }

    @Test
    void logicalOr_mixedValues() throws Exception {

        var attrName1 = "search_attr_OR_MIXED_1";
        var attrName2 = "search_attr_OR_MIXED_2";
        var attrNames = List.of(attrName1, attrName2);

        var def1 = TestData.dummyDataDef();
        var def2 = TestData.nextDataDef(def1);
        var def3 = TestData.nextDataDef(def2);
        var def4 = TestData.nextDataDef(def3);

        var match2List = List.of("match_2", "match_2a", "match_2b");
        var notMatch2List = List.of("match_2a", "match_2b");
        var typeDesc = TypeSystem.descriptor(BasicType.STRING);

        var tag1 = tagForDef(def1, attrNames, List.of(encodeValue("match_1"), encodeArrayValue(match2List, typeDesc)));
        var tag2 = tagForDef(def2, attrNames, List.of(encodeValue("match_1"), encodeArrayValue(notMatch2List, typeDesc)));
        var tag3 = tagForDef(def3, attrNames, List.of(encodeValue("not_match_1"), encodeArrayValue(match2List, typeDesc)));
        var tag4 = tagForDef(def4, attrNames, List.of(encodeValue("not_match_1"), encodeArrayValue(notMatch2List, typeDesc)));

        var tags = List.of(tag1, tag2, tag3, tag4);
        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, tags));

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setLogical(LogicalExpression.newBuilder()
                        .setOperator(LogicalOperator.OR)
                        .addExpr(searchTerm(attrName1, BasicType.STRING, SearchOperator.EQ, encodeValue("match_1")))
                        .addExpr(searchTerm(attrName2, BasicType.STRING, SearchOperator.EQ, encodeValue("match_2")))))
                .build();

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        var t1 = clearDefinitionBody(tag1);
        var t2 = clearDefinitionBody(tag2);
        var t3 = clearDefinitionBody(tag3);

        assertEquals(3, searchResult.size());
        assertEquals(Set.of(t1, t2, t3), Set.copyOf(searchResult));
    }

    @Test
    void logicalNot_singleValue() throws Exception {

        // Logical NOT will match everything already created by this test class!
        // Wrap the NOT operation in a logical AND to filter for items created for this individual test

        var markerName = "search_attr_NOT_SINGLE_marker";
        var attrName = "search_attr_NOT_SINGLE";

        var def1 = TestData.dummyDataDef();
        var def2 = TestData.nextDataDef(def1);

        var tag1 = tagForDef(def1, List.of(attrName, markerName), List.of(
                encodeValue("droids_you_are_looking_for"),
                encodeValue("negative_test_marker")));

        var tag2 = tagForDef(def2, List.of(attrName, markerName), List.of(
                encodeValue("not_the_droids_you_are_looking_for"),
                encodeValue("negative_test_marker")));

        var tags = List.of(tag1, tag2);
        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, tags));

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setLogical(LogicalExpression.newBuilder()
                    .setOperator(LogicalOperator.AND)
                    .addExpr(searchTerm(markerName, BasicType.STRING, SearchOperator.EQ, encodeValue("negative_test_marker")))
                    .addExpr(SearchExpression.newBuilder()
                        .setLogical(LogicalExpression.newBuilder()
                        .setOperator(LogicalOperator.NOT)
                        .addExpr(searchTerm(attrName, BasicType.STRING, SearchOperator.EQ, encodeValue("droids_you_are_looking_for")))))))
                .build();

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        var t2 = clearDefinitionBody(tag2);

        assertEquals(1, searchResult.size());
        assertEquals(t2, searchResult.get(0));
    }

    @Test
    void logicalNot_arrayValue() throws Exception {

        // Logical NOT will match everything already created by this test class!
        // Wrap the NOT operation in a logical AND to filter for items created for this individual test

        var markerName = "search_attr_NOT_ARRAY_marker";
        var attrName = "search_attr_NOT_ARRAY";

        var def1 = TestData.dummyDataDef();
        var def2 = TestData.nextDataDef(def1);

        var matchList = List.of("match_1", "match_1a", "match_1b");
        var notMatchList = List.of("match_1a", "match_1b");
        var typeDesc = TypeSystem.descriptor(BasicType.STRING);

        var tag1 = tagForDef(def1, List.of(attrName, markerName), List.of(
                encodeArrayValue(matchList, typeDesc),
                encodeValue("negative_test_marker")));

        var tag2 = tagForDef(def2, List.of(attrName, markerName), List.of(
                encodeArrayValue(notMatchList, typeDesc),
                encodeValue("negative_test_marker")));

        var tags = List.of(tag1, tag2);
        unwrap(dal.saveNewObjects(TestData.TEST_TENANT, tags));

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setLogical(LogicalExpression.newBuilder()
                    .setOperator(LogicalOperator.AND)
                    .addExpr(searchTerm(markerName, BasicType.STRING, SearchOperator.EQ, encodeValue("negative_test_marker")))
                    .addExpr(SearchExpression.newBuilder()
                        .setLogical(LogicalExpression.newBuilder()
                        .setOperator(LogicalOperator.NOT)
                        .addExpr(searchTerm(attrName, BasicType.STRING, SearchOperator.EQ, encodeValue("match_1")))))))
                .build();

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        var t2 = clearDefinitionBody(tag2);

        assertEquals(1, searchResult.size());
        assertEquals(t2, searchResult.get(0));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // VERSIONS, TEMPORALITY AND ORDERING
    // -----------------------------------------------------------------------------------------------------------------

    @Test
    void latestVersionByDefault() throws Exception {

        var attrToLookFor = "latest_version_search_attr";
        var valueToLookFor = "same_value_all_versions";

        var markerName = "latest_version_search_marker";
        var markerValue = "negative_test_marker";

        var attrNames = List.of(attrToLookFor, markerName);
        var attrValues = List.of(encodeValue(valueToLookFor), encodeValue(markerValue));

        var defV1 = TestData.dummyDataDef();
        var defV2 = TestData.nextDataDef(defV1);

        var tagV1T1 = tagForDef(defV1, attrNames, attrValues);
        var tagV1T2 = TestData.nextTag(tagV1T1, UPDATE_TAG_VERSION);
        var tagV2T1 = tagForNextObject(tagV1T2, defV2, INCLUDE_HEADER);
        var tagV2T2 = TestData.nextTag(tagV2T1, UPDATE_TAG_VERSION);

        unwrap(CompletableFuture.completedFuture(true)
                .thenCompose(x -> dal.saveNewObject(TEST_TENANT, tagV1T1))
                .thenCompose(x -> dal.saveNewTag(TEST_TENANT, tagV1T2))
                .thenCompose(x -> dal.saveNewVersion(TEST_TENANT, tagV2T1))
                .thenCompose(x -> dal.saveNewTag(TEST_TENANT, tagV2T2)));

        // A regular search with one EQ search term
        // This should bring back only the latest version / tag if no other behaviour is specified

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(attrToLookFor)
                        .setOperator(SearchOperator.EQ)
                        .setAttrType(BasicType.STRING)
                        .setSearchValue(encodeValue(valueToLookFor))))
                .build();

        var searchResult = unwrap(dal.search(TestData.TEST_TENANT, searchParams));

        // Also perform a negative search
        // The JDBC implementation uses sub-queries
        // Check that version flags are handled properly there

        var searchParams2 = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setLogical(LogicalExpression.newBuilder()
                .setOperator(LogicalOperator.AND)
                .addExpr(searchTerm(markerName, BasicType.STRING, SearchOperator.EQ, encodeValue(markerValue)))
                .addExpr(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(attrToLookFor)
                        .setOperator(SearchOperator.NE)
                        .setAttrType(BasicType.STRING)
                        .setSearchValue(encodeValue("not_the_droids_you_are_looking_for"))))))
                .build();

        var searchResult2 = unwrap(dal.search(TestData.TEST_TENANT, searchParams2));

        var v2t2 = clearDefinitionBody(tagV2T2);

        assertEquals(1, searchResult.size());
        assertEquals(v2t2, searchResult.get(0));

        assertEquals(1, searchResult2.size());
        assertEquals(v2t2, searchResult2.get(0));
    }

    @Test
    void priorVersionsFlag() throws Exception {

        var v1Obj = dummyDataDef();
        var v2Obj = nextDataDef(v1Obj);
        var v3Obj = nextDataDef(v2Obj);

        var v1Tag= dummyTag(v1Obj, INCLUDE_HEADER).toBuilder()
                .putAttrs("dal_prior_version_attr", MetadataCodec.encodeValue("the_droids_you_are_looking_for"))
                .build();

        var v2Tag = tagForNextObject(v1Tag, v2Obj, INCLUDE_HEADER).toBuilder()
                .putAttrs("dal_prior_version_attr", MetadataCodec.encodeValue("the_droids_you_are_looking_for"))
                .build();

        var v3Tag = tagForNextObject(v2Tag, v3Obj, INCLUDE_HEADER).toBuilder()
                .putAttrs("dal_prior_version_attr", MetadataCodec.encodeValue("not_the_droids_you_are_looking_for"))
                .build();

        unwrap(dal.saveNewObject(TEST_TENANT, v1Tag));
        unwrap(dal.saveNewVersion(TEST_TENANT, v2Tag));
        unwrap(dal.saveNewVersion(TEST_TENANT, v3Tag));

        var searchExpr = SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                .setAttrName("dal_prior_version_attr")
                .setAttrType(BasicType.STRING)
                .setOperator(SearchOperator.EQ)
                .setSearchValue(MetadataCodec.encodeValue("the_droids_you_are_looking_for")));

        var searchWithoutFlag = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(searchExpr)
                .build();

        var result = unwrap(dal.search(TEST_TENANT, searchWithoutFlag));

        Assertions.assertEquals(0, result.size());

        // Set the prior versions flag
        // Latest matching version should be returned, in this case v2

        var searchPriorVersions = searchWithoutFlag.toBuilder()
                .setPriorVersions(true)
                .build();

        var resultPriorVersions = unwrap(dal.search(TEST_TENANT, searchPriorVersions));

        Assertions.assertEquals(1, resultPriorVersions.size());
        Assertions.assertEquals(v2Tag.getHeader(), resultPriorVersions.get(0).getHeader());
    }

    @Test
    void priorTagsFlag() throws Exception {

        var v1Obj = dummyDataDef();

        var t1Tag= dummyTag(v1Obj, INCLUDE_HEADER).toBuilder()
                .putAttrs("dal_prior_tag_attr", MetadataCodec.encodeValue("the_droids_you_are_looking_for"))
                .build();

        var t2Tag = nextTag(t1Tag, UPDATE_TAG_VERSION).toBuilder()
                .putAttrs("dal_prior_tag_attr", MetadataCodec.encodeValue("the_droids_you_are_looking_for"))
                .build();

        var t3Tag = nextTag(t2Tag, UPDATE_TAG_VERSION).toBuilder()
                .putAttrs("dal_prior_tag_attr", MetadataCodec.encodeValue("not_the_droids_you_are_looking_for"))
                .build();

        unwrap(dal.saveNewObject(TEST_TENANT, t1Tag));
        unwrap(dal.saveNewTag(TEST_TENANT, t2Tag));
        unwrap(dal.saveNewTag(TEST_TENANT, t3Tag));

        var searchExpr = SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                .setAttrName("dal_prior_tag_attr")
                .setAttrType(BasicType.STRING)
                .setOperator(SearchOperator.EQ)
                .setSearchValue(MetadataCodec.encodeValue("the_droids_you_are_looking_for")))
                .build();

        var searchWithoutFlag = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(searchExpr)
                .build();

        var result = unwrap(dal.search(TEST_TENANT, searchWithoutFlag));

        Assertions.assertEquals(0, result.size());

        // Set the prior tags flag
        // Latest matching tag should be returned, in this case t2

        var searchPriorTags = searchWithoutFlag.toBuilder()
                .setPriorTags(true)
                .build();

        var resultPriorTags = unwrap(dal.search(TEST_TENANT, searchPriorTags));

        Assertions.assertEquals(1, resultPriorTags.size());
        Assertions.assertEquals(t2Tag.getHeader(), resultPriorTags.get(0).getHeader());
    }

    @Test
    void temporalSearch_basic() throws Exception {

        // Single test case to search as of a previous point in time

        // Extra object so that will still match after V1 is updated

        var unchangedObj = nextDataDef(dummyDataDef());
        var unchangedTag = TestData.dummyTag(unchangedObj, INCLUDE_HEADER).toBuilder()
                .putAttrs("dal_as_of_attr_1", MetadataCodec.encodeValue("initial_value"))
                .build();

        dal.saveNewObject(TEST_TENANT, unchangedTag);

        // Ensure unchanged tag has a creation timestamp that is before the versioned object
        Thread.sleep(10);

        // Now create the object that will be versioned

        var obj1 = dummyDataDef();

        var v1Tag = TestData.dummyTag(obj1, INCLUDE_HEADER).toBuilder()
                .putAttrs("dal_as_of_attr_1", MetadataCodec.encodeValue("initial_value"))
                .build();

        dal.saveNewObject(TEST_TENANT, v1Tag);

        // Use a search timestamp after both objects have been created, but before either is updated
        var v1SearchTime = MetadataCodec.decodeDatetime(v1Tag.getHeader().getTagTimestamp()).plusNanos(5000);

        Thread.sleep(10);
        var v2Timestamp = Instant.now().atOffset(ZoneOffset.UTC);

        var v2Tag = v1Tag.toBuilder()
                .setHeader(v1Tag.getHeader().toBuilder()
                .setObjectVersion(2)
                .setObjectTimestamp(MetadataCodec.encodeDatetime(v2Timestamp))
                .setTagVersion(1)
                .setTagTimestamp(MetadataCodec.encodeDatetime(v2Timestamp)))
                .putAttrs("dal_as_of_attr_1", MetadataCodec.encodeValue("updated_value"))
                .build();

        dal.saveNewVersion(TEST_TENANT, v2Tag);

        // Search without specifying as-of, should return only the extra object that has the original tag

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName("dal_as_of_attr_1")
                        .setAttrType(BasicType.STRING)
                        .setOperator(SearchOperator.EQ)
                        .setSearchValue(encodeValue("initial_value"))))
                .build();

        var result = unwrap(dal.search(TEST_TENANT, searchParams));
        var resultHeader = result.get(0).getHeader();

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(unchangedTag.getHeader(), resultHeader);

        // Now search with an as-of time before the update was applied, both objects should come back
        // The object created last should be top of the list

        var asOfSearch = searchParams.toBuilder()
                .setSearchAsOf(MetadataCodec.encodeDatetime(v1SearchTime))
                .build();

        var asOfResult = unwrap(dal.search(TEST_TENANT, asOfSearch));
        var resultHeader1 = asOfResult.get(0).getHeader();
        var resultHeader2 = asOfResult.get(1).getHeader();

        // The versioned tag should be returned first in the list
        // This is because results are returned with the most recently updated first

        Assertions.assertEquals(2, asOfResult.size());
        Assertions.assertEquals(v1Tag.getHeader(), resultHeader1);
        Assertions.assertEquals(unchangedTag.getHeader(), resultHeader2);
    }

    @Test
    void temporalSearch_fullHistory() throws Exception {

        // Search back through every point in the history of an object

        var v1Obj = dummyDataDef();
        var v2Obj = nextDataDef(v1Obj);

        var v1t1Tag = dummyTag(v1Obj, INCLUDE_HEADER).toBuilder()
                .putAttrs("dal_as_of_attr_2", MetadataCodec.encodeValue("as_of_search_test"))
                .build();

        Thread.sleep(10);
        var v1t2Tag = nextTag(v1t1Tag, UPDATE_TAG_VERSION);

        Thread.sleep(10);
        var v2t1Tag = tagForNextObject(v1t2Tag, v2Obj, INCLUDE_HEADER);

        Thread.sleep(10);
        var v2t2Tag = nextTag(v2t1Tag, UPDATE_TAG_VERSION);

        var saveAll = CompletableFuture.completedFuture(0)
                .thenCompose(x_ -> dal.saveNewObject(TEST_TENANT, v1t1Tag))
                .thenCompose(x_ -> dal.saveNewTag(TEST_TENANT, v1t2Tag))
                .thenCompose(x_ -> dal.saveNewVersion(TEST_TENANT, v2t1Tag))
                .thenCompose(x_ -> dal.saveNewTag(TEST_TENANT, v2t2Tag));

        unwrap(saveAll);

        var preCreateTime = MetadataCodec.decodeDatetime(v1t1Tag.getHeader().getTagTimestamp()).minusNanos(5000);
        var v1t1Time = MetadataCodec.decodeDatetime(v1t1Tag.getHeader().getTagTimestamp()).plusNanos(5000);
        var v1t2Time = MetadataCodec.decodeDatetime(v1t2Tag.getHeader().getTagTimestamp()).plusNanos(5000);
        var v2t1Time = MetadataCodec.decodeDatetime(v2t1Tag.getHeader().getTagTimestamp()).plusNanos(5000);
        var v2t2Time = MetadataCodec.decodeDatetime(v2t2Tag.getHeader().getTagTimestamp()).plusNanos(5000);

        var searchExpr = SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                .setAttrName("dal_as_of_attr_2")
                .setAttrType(BasicType.STRING)
                .setOperator(SearchOperator.EQ)
                .setSearchValue(MetadataCodec.encodeValue("as_of_search_test")))
                .build();

        // Series of searches stepping back through time
        // Should always return one object, which is the latest before the search time

        var search1 = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(searchExpr)
                .setSearchAsOf(MetadataCodec.encodeDatetime(v2t2Time))
                .build();

        var result1 = unwrap(dal.search(TEST_TENANT, search1));

        Assertions.assertEquals(1, result1.size());
        Assertions.assertEquals(v2t2Tag.getHeader(), result1.get(0).getHeader());

        var search2 = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(searchExpr)
                .setSearchAsOf(MetadataCodec.encodeDatetime(v2t1Time))
                .build();

        var result2 = unwrap(dal.search(TEST_TENANT, search2));

        Assertions.assertEquals(1, result2.size());
        Assertions.assertEquals(v2t1Tag.getHeader(), result2.get(0).getHeader());

        var search3 = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(searchExpr)
                .setSearchAsOf(MetadataCodec.encodeDatetime(v1t2Time))
                .build();

        var result3 = unwrap(dal.search(TEST_TENANT, search3));

        Assertions.assertEquals(1, result3.size());
        Assertions.assertEquals(v1t2Tag.getHeader(), result3.get(0).getHeader());

        var search4 = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(searchExpr)
                .setSearchAsOf(MetadataCodec.encodeDatetime(v1t1Time))
                .build();

        var result4 = unwrap(dal.search(TEST_TENANT, search4));

        Assertions.assertEquals(1, result4.size());
        Assertions.assertEquals(v1t1Tag.getHeader(), result4.get(0).getHeader());

        // Stepping back before the object was created should give an empty search result

        var search5 = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(searchExpr)
                .setSearchAsOf(MetadataCodec.encodeDatetime(preCreateTime))
                .build();

        var result5 = unwrap(dal.search(TEST_TENANT, search5));

        Assertions.assertEquals(0, result5.size());
    }

    @Test
    void temporalSearch_negativeConditions() throws Exception {

        // Use the NOT operator in a temporal search
        // To check for bugs applying the temporal conditions to negative search criteria

        var obj1 = dummyDataDef();
        var obj2 = nextDataDef(obj1);

        var obj1t1Tag = dummyTag(obj1, INCLUDE_HEADER).toBuilder()
                .putAttrs("dal_as_of_attr_3", MetadataCodec.encodeValue("as_of_search_test"))
                .putAttrs("dal_as_of_attr_4", MetadataCodec.encodeValue("not_the_droids_you_are_looking_for"))
                .build();

        var obj2t1Tag = dummyTag(obj2, INCLUDE_HEADER).toBuilder()
                .putAttrs("dal_as_of_attr_3", MetadataCodec.encodeValue("as_of_search_test"))
                .putAttrs("dal_as_of_attr_4", MetadataCodec.encodeValue("the_droids_you_are_looking_for"))
                .build();

        unwrap(dal.saveNewObject(TEST_TENANT, obj1t1Tag));
        unwrap(dal.saveNewObject(TEST_TENANT, obj2t1Tag));

        var t1Time = MetadataCodec.decodeDatetime(obj2t1Tag.getHeader().getTagTimestamp()).plusNanos(5000);

        Thread.sleep(10);

        var obj1t2Tag = nextTag(obj1t1Tag, UPDATE_TAG_VERSION).toBuilder()
                .putAttrs("dal_as_of_attr_3", MetadataCodec.encodeValue("as_of_search_test"))
                .putAttrs("dal_as_of_attr_4", MetadataCodec.encodeValue("the_droids_you_are_looking_for"))
                .build();

        var obj2t2Tag = nextTag(obj2t1Tag, UPDATE_TAG_VERSION).toBuilder()
                .putAttrs("dal_as_of_attr_3", MetadataCodec.encodeValue("as_of_search_test"))
                .putAttrs("dal_as_of_attr_4", MetadataCodec.encodeValue("not_the_droids_you_are_looking_for"))
                .build();

        unwrap(dal.saveNewTag(TEST_TENANT, obj1t2Tag));
        unwrap(dal.saveNewTag(TEST_TENANT, obj2t2Tag));

        var searchExpr = SearchExpression.newBuilder()
                .setLogical(LogicalExpression.newBuilder()
                .setOperator(LogicalOperator.AND)
                .addExpr(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                    .setAttrName("dal_as_of_attr_3")
                    .setAttrType(BasicType.STRING)
                    .setOperator(SearchOperator.EQ)
                    .setSearchValue(MetadataCodec.encodeValue("as_of_search_test"))))
                .addExpr(SearchExpression.newBuilder()
                .setLogical(LogicalExpression.newBuilder()
                    .setOperator(LogicalOperator.NOT)
                    .addExpr(SearchExpression.newBuilder()
                    .setTerm(SearchTerm.newBuilder()
                        .setAttrName("dal_as_of_attr_4")
                        .setAttrType(BasicType.STRING)
                        .setOperator(SearchOperator.EQ)
                        .setSearchValue(MetadataCodec.encodeValue("not_the_droids_you_are_looking_for")))))))
                .build();

        // First search without as-of time, should match obj1, t2

        var search1 = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(searchExpr)
                .build();

        var result1 = unwrap(dal.search(TEST_TENANT, search1));

        Assertions.assertEquals(1, result1.size());
        Assertions.assertEquals(obj1t2Tag.getHeader(), result1.get(0).getHeader());

        // Searching as-of a time before the tag updates, should match obj2, t1

        var search2 = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(searchExpr)
                .setSearchAsOf(MetadataCodec.encodeDatetime(t1Time))
                .build();

        var result2 = unwrap(dal.search(TEST_TENANT, search2));

        Assertions.assertEquals(1, result2.size());
        Assertions.assertEquals(obj2t1Tag.getHeader(), result2.get(0).getHeader());
    }

    @Test
    void temporalSearch_negativeConditions_priorVersionsAndTags() throws Exception {

        // Combine prior versions and tags in a temporal search
        // Also include a negative search condition

        var v1Obj = dummyDataDef();
        var v2Obj = nextDataDef(v1Obj);
        var v3Obj = nextDataDef(v2Obj);

        var v1t1 = dummyTag(v1Obj, INCLUDE_HEADER).toBuilder()
                .putAttrs("dal_combined_test_attr_1", MetadataCodec.encodeValue("match_me"))
                .putAttrs("dal_combined_test_attr_2", MetadataCodec.encodeValue("not_the_droids_you_are_looking_for"))
                .build();

        Thread.sleep(10);

        // This version / tag will match all attrs

        var v1t2 = nextTag(v1t1, UPDATE_TAG_VERSION).toBuilder()
                .putAttrs("dal_combined_test_attr_1", MetadataCodec.encodeValue("match_me"))
                .putAttrs("dal_combined_test_attr_2", MetadataCodec.encodeValue("the_droids_you_are_looking_for"))
                .build();

        // This version / tag will also match all attrs and supersedes v1t2

        var v1t3 = nextTag(v1t2, UPDATE_TAG_VERSION).toBuilder()
                .putAttrs("dal_combined_test_attr_1", MetadataCodec.encodeValue("match_me"))
                .putAttrs("dal_combined_test_attr_2", MetadataCodec.encodeValue("the_droids_you_are_looking_for"))
                .build();

        Thread.sleep(10);

        var v2t1 = tagForNextObject(v1t2, v2Obj, INCLUDE_HEADER).toBuilder()
                .putAttrs("dal_combined_test_attr_1", MetadataCodec.encodeValue("match_me"))
                .putAttrs("dal_combined_test_attr_2", MetadataCodec.encodeValue("not_the_droids_you_are_looking_for"))
                .build();

        Thread.sleep(10);

        var v1t4 = nextTag(v1t3, UPDATE_TAG_VERSION).toBuilder()
                .putAttrs("dal_combined_test_attr_1", MetadataCodec.encodeValue("match_me"))
                .putAttrs("dal_combined_test_attr_2", MetadataCodec.encodeValue("not_the_droids_you_are_looking_for"))
                .build();

        Thread.sleep(10);

        // This version / tag will match all attrs but will be filtered out by as-of time

        var v3t1 = tagForNextObject(v2t1, v3Obj, INCLUDE_HEADER).toBuilder()
                .putAttrs("dal_combined_test_attr_1", MetadataCodec.encodeValue("match_me"))
                .putAttrs("dal_combined_test_attr_2", MetadataCodec.encodeValue("the_droids_you_are_looking_for"))
                .build();

        // Save everything

        unwrap(dal.saveNewObject(TEST_TENANT, v1t1));
        unwrap(dal.saveNewTag(TEST_TENANT, v1t2));
        unwrap(dal.saveNewTag(TEST_TENANT, v1t3));
        unwrap(dal.saveNewVersion(TEST_TENANT, v2t1));
        unwrap(dal.saveNewTag(TEST_TENANT, v1t4));
        unwrap(dal.saveNewVersion(TEST_TENANT, v3t1));

        var searchExpr = SearchExpression.newBuilder()
                .setLogical(LogicalExpression.newBuilder()
                .setOperator(LogicalOperator.AND)
                .addExpr(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName("dal_combined_test_attr_1")
                        .setAttrType(BasicType.STRING)
                        .setOperator(SearchOperator.EQ)
                        .setSearchValue(MetadataCodec.encodeValue("match_me"))))
                .addExpr(SearchExpression.newBuilder()
                        .setLogical(LogicalExpression.newBuilder()
                        .setOperator(LogicalOperator.NOT)
                        .addExpr(SearchExpression.newBuilder()
                        .setTerm(SearchTerm.newBuilder()
                                .setAttrName("dal_combined_test_attr_2")
                                .setAttrType(BasicType.STRING)
                                .setOperator(SearchOperator.EQ)
                                .setSearchValue(MetadataCodec.encodeValue("not_the_droids_you_are_looking_for")))))))
                .build();

        var searchTime = MetadataCodec.decodeDatetime(v3t1.getHeader().getTagTimestamp()).minusNanos(5000);

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(searchExpr)
                .setSearchAsOf(MetadataCodec.encodeDatetime(searchTime))
                .setPriorVersions(true)
                .setPriorTags(true)
                .build();

        var result = unwrap(dal.search(TEST_TENANT, searchParams));

        assertEquals(1, result.size());
        assertEquals(v1t3.getHeader(), result.get(0).getHeader());
    }

    @Test
    void resultOrdering() throws Exception {

        var obj1 = dummyDataDef();

        var obj1Tag = TestData.dummyTag(obj1, INCLUDE_HEADER).toBuilder()
                .putAttrs("dal_search_ordering_test", MetadataCodec.encodeValue("some_value"))
                .build();

        Thread.sleep(10);

        var obj2 = nextDataDef(obj1);
        var obj2Tag = TestData.dummyTag(obj2, INCLUDE_HEADER).toBuilder()
                .putAttrs("dal_search_ordering_test", MetadataCodec.encodeValue("some_value"))
                .build();

        // Save objects in the wrong order to try to confuse the DAL

        dal.saveNewObject(TEST_TENANT, obj2Tag);
        dal.saveNewObject(TEST_TENANT, obj1Tag);

        var searchParams = SearchParameters.newBuilder()
                .setObjectType(ObjectType.DATA)
                .setSearch(SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName("dal_search_ordering_test")
                        .setAttrType(BasicType.STRING)
                        .setOperator(SearchOperator.EQ)
                        .setSearchValue(encodeValue("some_value"))))
                .build();

        var result = unwrap(dal.search(TEST_TENANT, searchParams));

        // Results should come back with obj2 at the top, since it has the most recent timestamp

        assertEquals(2, result.size());
        assertEquals(obj2Tag.getHeader(), result.get(0).getHeader());
        assertEquals(obj1Tag.getHeader(), result.get(1).getHeader());
    }


    // -----------------------------------------------------------------------------------------------------------------
    // HELPERS
    // -----------------------------------------------------------------------------------------------------------------

    private Tag tagForDef(ObjectDefinition def, String attrName, Value attrValue) {

        return Tag.newBuilder()
                .setHeader(TestData.newHeader(def.getObjectType()))
                .setDefinition(def)
                .putAttrs(attrName, attrValue)
                .putAttrs("more_than_one_attr", encodeValue(true))
                .build();
    }

    private Tag tagForDef(ObjectDefinition def, List<String> attrNames, List<Value> attrValues) {

        var tag = Tag.newBuilder()
                .setHeader(TestData.newHeader(def.getObjectType()))
                .setDefinition(def);

        for (var i = 0; i < attrNames.size(); i++)
            tag = tag.putAttrs(attrNames.get(i), attrValues.get(i));

        return tag.build();
    }

    private Tag clearDefinitionBody(Tag fullTag) {

        return fullTag
                .toBuilder()
                .clearDefinition()
                .build();
    }

    private SearchExpression.Builder searchTerm(
            String attrName, BasicType attrType,
            SearchOperator operator, Value searchValue) {

        return SearchExpression.newBuilder()
                .setTerm(SearchTerm.newBuilder()
                        .setAttrName(attrName)
                        .setAttrType(attrType)
                        .setOperator(operator)
                        .setSearchValue(searchValue));
    }
}
