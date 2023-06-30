/**
 * A component that allows the user to set variables to use to group by in a SQL query.
 *
 * @module GroupBy
 * @category Component
 */

import Col from "react-bootstrap/Col";
import {convertSchemaToOptions} from "../../utils/utils_schema";
import PropTypes from "prop-types";
import {type QueryBuilderStoreState, setGroupBy} from "./queryBuilderStore";
import React, {useMemo} from "react";
import Row from "react-bootstrap/Row";
import {SelectOption} from "../SelectOption";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the FunctionButton component.
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

export const GroupBy = (props: Props) => {

    const {storeKey, objectKey} = props

    // Get what we need from the store
    const {variables} = useAppSelector(state => state["queryBuilderStore"].uses[storeKey].query[objectKey].groupByTab)

    const {schema} = useAppSelector(state => state["queryBuilderStore"].uses[storeKey].query[objectKey].inputData)

    // The schema converted to a set of options
    const variableOptions = useMemo(() => convertSchemaToOptions(schema || [], true, false, true), [schema])

    return (

        <Row>
            <Col xs={12} lg={9} className={"mb-4 mt-5"}>

                Please select one or more variables you want to add to the query to group the query results by. Only columns marked as
                categorical in the dataset schema will appear in the list below.

            </Col>
            <Col xs={12} md={6}>
                <SelectOption basicType={trac.STRING}
                              isClearable={true}
                              isDispatched={true}
                              isMulti={true}
                              mustValidate={false}
                              onChange={setGroupBy}
                              options={variableOptions}
                              validateOnMount={false}
                              value={variables}
                />
            </Col>
        </Row>
    )
};

GroupBy.propTypes = {

    objectKey: PropTypes.string.isRequired,
    storeKey: PropTypes.string.isRequired
};