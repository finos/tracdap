/**
 * A component that allows the user to set a where clause to the number of rows returned from a SQL query.
 *
 * @module Where
 * @category Component
 */

import PropTypes from "prop-types";
import {type QueryBuilderStoreState} from "./queryBuilderStore";
import React from "react";
import {TextBlock} from "../TextBlock";
import {useAppSelector} from "../../../types/types_hooks";
import {WhereClauseBuilder} from "../WhereClauseBuilder/WhereClauseBuilder";

/**
 * An interface for the props of the Where component.
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

export const Where = (props: Props) => {

    const {storeKey, objectKey} = props

    // Get what we need from the store
    const {schema} = useAppSelector(state => state["queryBuilderStore"].uses[storeKey].query[objectKey].inputData)

    return (

        <React.Fragment>
            <TextBlock>
                Use the button below to add a where clause to your query, multiple rules can be combined into a single clause.
            </TextBlock>

            <WhereClauseBuilder objectKey={objectKey}
                                schema={schema || []}
                                storeKey={storeKey}
                // A query can only have 1 where clause so the index is always zero
                                whereIndex={0}
            />
        </React.Fragment>
    )
};

Where.propTypes = {

    objectKey: PropTypes.string.isRequired,
    storeKey: PropTypes.string.isRequired
};