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

import org.finos.tracdap.common.metadata.TypeSystem;
import org.finos.tracdap.common.validation.core.ValidationContext;
import org.finos.tracdap.common.validation.core.ValidationType;
import org.finos.tracdap.common.validation.core.Validator;
import org.finos.tracdap.metadata.*;

import com.google.protobuf.Descriptors;

import java.util.List;

import static org.finos.tracdap.common.validation.core.ValidatorUtils.field;


@Validator(type = ValidationType.STATIC)
public class SearchValidator {

    private static final List<SearchOperator> ORDERED_OPERATORS = List.of(
            SearchOperator.LT,
            SearchOperator.LE,
            SearchOperator.GT,
            SearchOperator.GE);

    private static final List<SearchOperator> EQUALITY_OPERATORS = List.of(
            SearchOperator.EQ,
            SearchOperator.NE,
            SearchOperator.IN);

    private static final List<BasicType> ORDERED_TYPES = List.of(
            BasicType.INTEGER,
            BasicType.FLOAT,
            BasicType.DECIMAL,
            BasicType.DATE,
            BasicType.DATETIME);

    private static final List<BasicType> CONTINUOUS_TYPES = List.of(
            BasicType.FLOAT,
            BasicType.DECIMAL,
            BasicType.DATETIME);

    private static final Descriptors.Descriptor SEARCH_PARAMETERS;
    private static final Descriptors.FieldDescriptor SP_OBJECT_TYPE;
    private static final Descriptors.FieldDescriptor SP_SEARCH;
    private static final Descriptors.FieldDescriptor SP_SEARCH_ASOF;
    private static final Descriptors.FieldDescriptor SP_PRIOR_VERSIONS;
    private static final Descriptors.FieldDescriptor SP_PRIOR_TAGS;

    private static final Descriptors.Descriptor SEARCH_EXPRESSION;
    private static final Descriptors.OneofDescriptor SE_EXPR;
    private static final Descriptors.FieldDescriptor SE_TERM;
    private static final Descriptors.FieldDescriptor SE_LOGICAL;

    private static final Descriptors.Descriptor SEARCH_TERM;
    private static final Descriptors.FieldDescriptor ST_ATTR_NAME;
    private static final Descriptors.FieldDescriptor ST_ATTR_TYPE;
    private static final Descriptors.FieldDescriptor ST_OPERATOR;
    private static final Descriptors.FieldDescriptor ST_SEARCH_VALUE;

    private static final Descriptors.Descriptor LOGICAL_EXPRESSION;
    private static final Descriptors.FieldDescriptor LE_OPERATOR;
    private static final Descriptors.FieldDescriptor LE_EXPR;

    static {

        SEARCH_PARAMETERS = SearchParameters.getDescriptor();
        SP_OBJECT_TYPE = field(SEARCH_PARAMETERS, SearchParameters.OBJECTTYPE_FIELD_NUMBER);
        SP_SEARCH = field(SEARCH_PARAMETERS, SearchParameters.SEARCH_FIELD_NUMBER);
        SP_SEARCH_ASOF = field(SEARCH_PARAMETERS, SearchParameters.SEARCHASOF_FIELD_NUMBER);
        SP_PRIOR_VERSIONS = field(SEARCH_PARAMETERS, SearchParameters.PRIORVERSIONS_FIELD_NUMBER);
        SP_PRIOR_TAGS = field(SEARCH_PARAMETERS, SearchParameters.PRIORTAGS_FIELD_NUMBER);

        SEARCH_EXPRESSION = SearchExpression.getDescriptor();
        SE_EXPR = field(SEARCH_EXPRESSION, SearchExpression.TERM_FIELD_NUMBER).getContainingOneof();
        SE_TERM = field(SEARCH_EXPRESSION, SearchExpression.TERM_FIELD_NUMBER);
        SE_LOGICAL = field(SEARCH_EXPRESSION, SearchExpression.LOGICAL_FIELD_NUMBER);

        SEARCH_TERM = SearchTerm.getDescriptor();
        ST_ATTR_NAME = field(SEARCH_TERM, SearchTerm.ATTRNAME_FIELD_NUMBER);
        ST_ATTR_TYPE = field(SEARCH_TERM, SearchTerm.ATTRTYPE_FIELD_NUMBER);
        ST_OPERATOR = field(SEARCH_TERM, SearchTerm.OPERATOR_FIELD_NUMBER);
        ST_SEARCH_VALUE = field(SEARCH_TERM, SearchTerm.SEARCHVALUE_FIELD_NUMBER);

        LOGICAL_EXPRESSION = LogicalExpression.getDescriptor();
        LE_OPERATOR = field(LOGICAL_EXPRESSION, LogicalExpression.OPERATOR_FIELD_NUMBER);
        LE_EXPR = field(LOGICAL_EXPRESSION, LogicalExpression.EXPR_FIELD_NUMBER);
    }


    @Validator
    public static ValidationContext searchParameters(SearchParameters msg, ValidationContext ctx) {

        ctx = ctx.push(SP_OBJECT_TYPE)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, ObjectType.class)
                .pop();

        // Always require a search expression
        // Note - there is an open issue to relax this, i.e. allow searches for all objects of a given type

        ctx = ctx.push(SP_SEARCH)
                .apply(CommonValidators::optional)
                .apply(SearchValidator::searchExpression, SearchExpression.class)
                .pop();

        ctx = ctx.push(SP_SEARCH_ASOF)
                .apply(CommonValidators::optional)
                .apply(TypeSystemValidator::datetimeValue, DatetimeValue.class)
                .apply(TypeSystemValidator::notInTheFuture, DatetimeValue.class)
                .pop();

        ctx = ctx.push(SP_PRIOR_VERSIONS)
                .apply(CommonValidators::optional)
                .pop();

        ctx = ctx.push(SP_PRIOR_TAGS)
                .apply(CommonValidators::optional)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext searchExpression(SearchExpression msg, ValidationContext ctx) {

        ctx = ctx.pushOneOf(SE_EXPR)
                .apply(CommonValidators::required)
                .applyOneOf(SE_TERM, SearchValidator::searchTerm, SearchTerm.class)
                .applyOneOf(SE_LOGICAL, SearchValidator::logicalExpression, LogicalExpression.class)
                .pop();

        return ctx;
    }

    @Validator
    public static ValidationContext searchTerm(SearchTerm msg, ValidationContext ctx) {

        ctx = ctx.push(ST_ATTR_NAME)
                .apply(CommonValidators::required)
                .apply(CommonValidators::identifier)
                .pop();

        ctx = ctx.push(ST_ATTR_TYPE)
                .applyIf(!msg.getOperator().equals(SearchOperator.EXISTS), CommonValidators::required)
                .applyIf(msg.getOperator().equals(SearchOperator.EXISTS), CommonValidators::optional)
                .apply(CommonValidators::nonZeroEnum, BasicType.class)
                .apply(CommonValidators::primitiveType, BasicType.class)
                .pop();

        ctx = ctx.push(ST_OPERATOR)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, SearchOperator.class)
                .pop();

        ctx = ctx.push(ST_SEARCH_VALUE)
                .applyIf(!msg.getOperator().equals(SearchOperator.EXISTS), CommonValidators::required)
                .applyIf(msg.getOperator().equals(SearchOperator.EXISTS), CommonValidators::omitted)
                .apply(TypeSystemValidator::value, Value.class)
                .pop();

        // Only attempt type comparisons if there are no other failures for the search term
        // In particular, do not attempt type inference on invalid types!

        if (!ctx.failed()) {
            ctx = ctx.apply(SearchValidator::operatorMatchesType, SearchTerm.class);
            ctx = ctx.applyIf(!msg.getOperator().equals(SearchOperator.EXISTS), SearchValidator::valueMatchesType, SearchTerm.class);
        }

        return ctx;
    }

    @Validator
    public static ValidationContext logicalExpression(LogicalExpression msg, ValidationContext ctx) {

        ctx = ctx.push(LE_OPERATOR)
                .apply(CommonValidators::required)
                .apply(CommonValidators::nonZeroEnum, LogicalOperator.class)
                .pop();

        ctx = ctx.pushRepeated(LE_EXPR)
                .apply(CommonValidators::listNotEmpty)
                .pop();

        if (msg.getOperator() == LogicalOperator.NOT && msg.getExprCount() > 1)
            ctx.error("Logical NOT expression cannot have multiple sub-expressions");

        return ctx;
    }

    private static ValidationContext operatorMatchesType(SearchTerm msg, ValidationContext ctx) {

        if (ORDERED_OPERATORS.contains(msg.getOperator()) && !ORDERED_TYPES.contains(msg.getAttrType())) {

            var err = String.format(
                    "Search operation [%s] is not allowed on attribute type [%s] (ordered operation on unordered type)",
                    msg.getOperator(), msg.getAttrType());

            ctx = ctx.error(err);
        }

        if (EQUALITY_OPERATORS.contains(msg.getOperator()) && CONTINUOUS_TYPES.contains(msg.getAttrType())) {

            var err = String.format(
                    "Search operation [%s] is not allowed on attribute type [%s] (equality comparison on continuous type)",
                    msg.getOperator(), msg.getAttrType());

            ctx = ctx.error(err);
        }

        return ctx;
    }

    private static ValidationContext valueMatchesType(SearchTerm msg, ValidationContext ctx) {

        // Only called if the individual fields in the search term are already validated
        // In particular, do not attempt type inference on invalid types!

        var searchValueType = TypeSystem.descriptor(msg.getSearchValue());

        if (msg.getOperator() == SearchOperator.IN && searchValueType.getBasicType() != BasicType.ARRAY) {

            var err = String.format(
                    "Search operation [%s] requires an [%s] search value",
                    msg.getOperator(), BasicType.ARRAY);

            return ctx.error(err);
        }

        var searchValueBasicType = msg.getOperator() == SearchOperator.IN
                ? searchValueType.getArrayType().getBasicType()
                : searchValueType.getBasicType();

        if (searchValueBasicType != msg.getAttrType()) {

            var err = String.format(
                    "Search value type [%s] does not match attribute type [%s]",
                    searchValueBasicType, msg.getAttrType());

            return ctx.error(err);
        }

        return ctx;
    }
}
