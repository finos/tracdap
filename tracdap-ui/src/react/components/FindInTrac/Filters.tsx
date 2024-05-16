/**
 * A component that shows tabs which allow the user to switch between different methods of searching in TRAC.
 *
 * @module Filters
 * @category Component
 */

import type {FindInTracStoreState} from "./findInTracStore";
import PropTypes from "prop-types";
import React, {memo} from "react";
import {SearchByAttributes} from "./SearchByAttributes";
import {SearchByObjectId} from "./SearchByObjectId";
import {setTab} from "./findInTracStore";
import Tab from "react-bootstrap/Tab";
import Tabs from "react-bootstrap/Tabs";
import {useAppDispatch, useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the Filters component.
 */
export interface Props {

    /**
     *  The key in the FindInTracStore to use to save the data to/ get the data from.
     */
    storeKey: keyof FindInTracStoreState["uses"]
}

const FiltersInner = (props: Props) => {

    const {storeKey} = props

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store
    const {
        selectedTab,
    } = useAppSelector(state => state["findInTracStore"].uses[storeKey])

    /**
     * A function that runs when the user changes the tab between the searching by attributes or object ID/key. We
     * need to control the tab selected because we need to know what results to show in the table.
     * @param key - The key of the tab selected.
     */
    const handleTabChange = (key: string | null): void => {
        if (key) dispatch(setTab({storeKey, selectedTab: key}))
    }

    return (

        <Tabs activeKey={selectedTab}
              defaultActiveKey={"searchByAttributes"}
              id={"select-search-type"}
              onSelect={handleTabChange}
        >

            <Tab eventKey="searchByAttributes" title="Search by attributes"
                 className={"tab-content-bordered pt-3 pb-3 mb-4"}>

                <SearchByAttributes storeKey={storeKey}/>

            </Tab>

            <Tab className={"tab-content-bordered pt-4 pb-4 mb-4"}
                 eventKey="searchByObjectId"
                 title="Search by object ID"
            >
                <SearchByObjectId storeKey={storeKey}/>

            </Tab>
        </Tabs>
    )
};

FiltersInner.propTypes = {

    storeKey: PropTypes.string.isRequired,
};

export const Filters = memo(FiltersInner);