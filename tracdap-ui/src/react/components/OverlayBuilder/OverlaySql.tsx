/**
 * A component that shows the user the SQL corresponding to an overlay.
 *
 * @module OverlaySql
 * @category Component
 */

import {calculateOverlayString, type ChangeTab, type OverlayBuilderStoreState} from "./overlayBuilderStore";
import PropTypes from "prop-types";
import React, {useMemo} from "react";
import {useAppSelector} from "../../../types/types_hooks";
import {type WhereClause, WhereClauseBuilderStoreState} from "../WhereClauseBuilder/whereClauseBuilderStore";

/**
 * An interface for the props of the OverlaySql component.
 */
export interface Props {

    /**
     * The index or position of the overlay in the array of overlays for the given overlayKey.
     */
    overlayIndex: number
    /**
     * A unique reference to the item having overlays applied to it. In a flow this can be the output node key as these are unique.
     */
    overlayKey: string
    /**
     *  The key in the QueryBuilderStore to use to save the data to / get the data from.
     */
    storeKey: keyof OverlayBuilderStoreState["uses"] & keyof WhereClauseBuilderStoreState["uses"]
    /**
     * The index of the where clause, individual objectKey value can have multiple where clauses. For
     * example in OverlayBuilder the objectKey is the dataset key, a dataset can have multiple overlays
     * defined, each with a where clause with multiple rules.
     */
    whereIndex: number
}

export const OverlaySql = (props: Props) => {

    const {overlayIndex, overlayKey, storeKey, whereIndex} = props

    // Get what we need from the store
    const changeTab: ChangeTab = useAppSelector(state => state["overlayBuilderStore"].uses[storeKey].change[overlayKey].changeTab[overlayIndex])
    const whereClause: WhereClause = useAppSelector(state => state["whereClauseBuilderStore"].uses[storeKey].whereClause[overlayKey])

    // Calculate the full query from the user interface UI as a string
    const overlayQuery = useMemo(() => calculateOverlayString(changeTab, whereClause?.whereTab[whereIndex]), [changeTab, whereClause?.whereTab, whereIndex])

    return (

        <pre className={"code-lite py-5"}>
            {overlayQuery}
        </pre>
    )
};

OverlaySql.propTypes = {

    overlayIndex: PropTypes.number.isRequired,
    overlayKey: PropTypes.string.isRequired,
    storeKey: PropTypes.string.isRequired,
    whereIndex: PropTypes.number.isRequired
};