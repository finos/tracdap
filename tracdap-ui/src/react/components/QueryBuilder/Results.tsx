/**
 * A component that allows the user to see the results of their query.
 *
 * @module Results
 * @category Component
 */

import Col from "react-bootstrap/Col";
import {convertSecondsToHrMinSec} from "../../utils/utils_number";
import ErrorBoundary from "../ErrorBoundary";
import {getTableState, type QueryBuilderStoreState} from "./queryBuilderStore";
import {HeaderTitle} from "../HeaderTitle";
import {Loading} from "../Loading";
import PropTypes from "prop-types";
import React from "react";
import Row from "react-bootstrap/Row";
import {Run} from "./Run";
import {ShowHideDetails} from "../ShowHideDetails";
import {Table} from "../Table/Table";
import {useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the Results component.
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

export const Results = (props: Props) => {

    // TODO build the metadata object to pass to the table.

    const {storeKey, objectKey} = props

    // Get what we need from the store
    const {status, duration} = useAppSelector(state => state["queryBuilderStore"].uses[storeKey].query[objectKey].execution)
    const {data, queryThatRan, schema, tableState} = useAppSelector(state => state["queryBuilderStore"].uses[storeKey].query[objectKey].result)

    return (

        <React.Fragment>

            <HeaderTitle type={"h3"} outerClassName={"mt-5 mb-3"} text={status !== "succeeded" ? "" : "Query Results"}>
                <Run storeKey={storeKey} objectKey={objectKey}/>
            </HeaderTitle>

            {status === "pending" && <Loading text={"Query running"}/>}

            {status === "succeeded" && Array.isArray(data) &&

                <React.Fragment>

                    <ShowHideDetails linkText={"query details"} classNameOuter={"pt-2 pb-4"}>

                        <Row>
                            <Col>
                                Below is the query that ran to create the data below:
                            </Col>
                        </Row>
                        <pre className={"code-lite mt-3"}>
                            {queryThatRan}
                        </pre>

                    </ShowHideDetails>

                    {/*TODO add method to retain table state*/}
                    {Array.isArray(data) && Array.isArray(schema) &&
                        <ErrorBoundary>
                            <Table data={data}
                                   footerComponent={<span>{duration != null ? `Your query took ${convertSecondsToHrMinSec(duration, true)}` : ""}</span>}
                                   initialShowGlobalFilter={false}
                                   isEditable={false}
                                   isTracData={false}
                                   noDataMessage={"There are no query results"}
                                   noOfRowsSelectable={0}
                                   savedTableState={tableState}
                                   saveTableStateFunction={getTableState}
                                   schema={schema}
                                   showExportButtons={true}
                                // tag={tag}
                            />
                        </ErrorBoundary>
                        // TODO add schema editor
                    }
                </React.Fragment>
            }

        </React.Fragment>
    )

};

Results.propTypes = {

    objectKey: PropTypes.string.isRequired,
    storeKey: PropTypes.string.isRequired
};