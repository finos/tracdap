/**
 * A component that allows the user to run their SQL query.
 *
 * @module Run
 * @category Component
 */

import {Button} from "../Button";
import {checkAndRunQuery, type QueryBuilderStoreState} from "./queryBuilderStore";
import PropTypes from "prop-types";
import React from "react";
import {useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the Run component.
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

export const Run = (props: Props) => {

    const {storeKey, objectKey} = props

    // Get what we need from the store
    const {status} = useAppSelector(state => state["queryBuilderStore"].uses[storeKey].query[objectKey].execution)
    const {"trac-tenant" : tenant} = useAppSelector(state => state["applicationStore"].cookies)

    return (

        <Button ariaLabel={"Run query"}
                className={"min-width-px-150"}
                disabled={status === "pending"}
                id={tenant}
                isDispatched={true}
                onClick={checkAndRunQuery}
                variant={"info"}
        >
            Run query
        </Button>
    )
};

Run.propTypes = {

    objectKey: PropTypes.string.isRequired,
    storeKey: PropTypes.string.isRequired
};