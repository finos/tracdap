/**
 * A component that allows the user to set a limit to the number of rows returned from a SQL query.
 *
 * @module Limit
 * @category Component
 */

import Col from "react-bootstrap/Col";
import {General} from "../../../config/config_general"
import PropTypes from "prop-types";
import {type QueryBuilderStoreState, setLimit} from "./queryBuilderStore";
import React from "react";
import Row from "react-bootstrap/Row";
import {SelectValue} from "../SelectValue";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the Limit component.
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

export const Limit = (props: Props) => {

    const {storeKey, objectKey} = props

    // Get what we need from the store
    const {limit} = useAppSelector(state => state["queryBuilderStore"].uses[storeKey].query[objectKey].limitTab)

    return (

        <Row>
            <Col xs={12} lg={9} className={"mb-4 mt-5"}>

                You can use the input below to change the number of rows returned from the query, queries returning fewer
                rows will run more quickly. However there is a hard limit to the number of rows that can be downloaded from
                large datasets as this can cause performance issues. The maximum allowed is {General.numberOfDataDownloadRows}.

            </Col>
            <Col xs={12} md={6} lg={4}>
                <SelectValue basicType={trac.INTEGER}
                             maximumValue={General.numberOfDataDownloadRows}
                             minimumValue={1}
                             mustValidate={true}
                             onChange={setLimit}
                             showValidationMessage={true}
                             value={limit}
                             validationChecked={true}
                             validateOnMount={false}
                />
            </Col>
        </Row>
    )
};

Limit.propTypes = {

    objectKey: PropTypes.string.isRequired,
    storeKey: PropTypes.string.isRequired
};