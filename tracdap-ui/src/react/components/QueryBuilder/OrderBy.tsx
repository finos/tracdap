/**
 * A component that allows the user to set variables to use to order by in a SQL query.
 *
 * @module OrderBy
 * @category Component
 */

import Col from "react-bootstrap/Col";
import {configuration} from "./config_query_builder";
import {convertSchemaToOptions} from "../../utils/utils_schema";
import {HeaderTitle} from "../HeaderTitle";
import PropTypes from "prop-types";
import {type QueryBuilderStoreState, setOrderBy, setOrdering} from "./queryBuilderStore";
import React, {useMemo} from "react";
import Row from "react-bootstrap/Row";
import {SelectOption} from "../SelectOption";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the OrderBy component.
 */
export interface Props {

    /**
     * The object key of the data the user has selected to query, the key include the object version. The
     * store has a separate area for each dataset that the user has loaded up into QueryBuilder.
     */
    objectKey: string
    /**
     *  The key in the QueryBuilderStore to use to save the data to / get the data from.
     */
    storeKey: keyof QueryBuilderStoreState["uses"]
}

export const OrderBy = (props: Props) => {

    const {storeKey, objectKey} = props

    // Get what we need from the store
    const {variables, ordering} = useAppSelector(state => state["queryBuilderStore"].uses[storeKey].query[objectKey].orderByTab)
    const {schema} = useAppSelector(state => state["queryBuilderStore"].uses[storeKey].query[objectKey].inputData)

    const {orderingOptions} = configuration

    // The schema converted to a set of options
    const variableOptions = useMemo(() => convertSchemaToOptions(schema || [], true, false, false), [schema])

    return (

        <React.Fragment>
            <Row>
                <Col xs={12} lg={9} className={"mb-4 mt-5"}>

                    Please select one or more variables you want to add to the query to order the query results by. By default the results
                    will be sorted in ascending order but this can be set if needed in the table below.

                </Col>
            </Row>
            <Row>
                <Col xs={12} md={6}>
                    <SelectOption basicType={trac.STRING}
                                  isClearable={true}
                                  isDispatched={true}
                                  isMulti={true}
                                  mustValidate={false}
                                  onChange={setOrderBy}
                                  options={variableOptions}
                                  validateOnMount={false}
                                  value={variables}
                    />
                </Col>
            </Row>

            {variables.length > 0 &&
                <React.Fragment>
                    <HeaderTitle text={"Set ordering"} type={"h5"} outerClassName={"mt-5 mb-3"}/>
                    {/*todo MAKE THIS format it as a table*/}
                    <Row>
                        <Col xs={12} lg={4}>
                            {variables.map(variable =>
                                <SelectOption basicType={trac.STRING}
                                              className={"py-1"}
                                              id={variable.value}
                                              key={variable.label}
                                              labelText={variable.label}
                                              labelPosition={"top"}
                                              mustValidate={false}
                                              onChange={setOrdering}
                                              options={orderingOptions}
                                              validateOnMount={false}
                                              value={ordering[variable.value] || null}
                                />
                            )}
                        </Col>
                    </Row>
                </React.Fragment>
            }
        </React.Fragment>
    )
};

OrderBy.propTypes = {

    objectKey: PropTypes.string.isRequired,
    storeKey: PropTypes.string.isRequired
};