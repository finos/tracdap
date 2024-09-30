/**
 * A component that shows button that when clicked on makes an API search based on the attributes that the user has
 * added to the filters.
 *
 * @module DoSearchButton
 * @category Component
 */

import {Button} from "../Button";
import {doSearch} from "./findInTracStore";
import type {FindInTracStoreState} from "./findInTracStore";
import {Icon} from "../Icon";
import PropTypes from "prop-types";
import React, {useCallback} from "react";
import {useAppDispatch, useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the DoSearchButton component.
 */
export interface Props {

    /**
     *  The key in the FindInTracStore to use to save the data to/get the data from.
     */
    storeKey: keyof FindInTracStoreState["uses"]
}

export const DoSearchButton = (props: Props) => {

    const {storeKey} = props

    // Get what we need from the store
    const {[storeKey]: storeUser, [storeKey]: {selectedTab}} = useAppSelector(state => state["findInTracStore"].uses)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * The function that runs the search, this is run automatically when the page loads and then when the user clicks
     * on the search buttons.
     */
    const handleSearch = useCallback(() => {

        dispatch(doSearch({storeKey}))

    }, [dispatch, storeKey])

    return (

        <Button ariaLabel={"Run search"}
                className={"min-width-px-150 float-end"}
                loading={Boolean(storeUser[selectedTab].status === "pending")}
                onClick={handleSearch}
                isDispatched={false}
        >
            <Icon ariaLabel={"false"}
                  className={"me-2"}
                  icon={"bi-search"}
            />
            Update search
        </Button>
    )
};

DoSearchButton.propTypes = {

    storeKey: PropTypes.string.isRequired,
};