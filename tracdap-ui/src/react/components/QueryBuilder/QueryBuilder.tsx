/**
 * A component that allows the user to create an SQL query statement to run on a dataset.
 *
 * @module QueryBuilder
 * @category Component
 */

import {createQueryEntry, type QueryBuilderStoreState, setCurrentlyLoadedQuery} from "./queryBuilderStore";
import {createUniqueObjectKey} from "../../utils/utils_trac_metadata";
import {Editor} from "./Editor";
import {GroupBy} from "./GroupBy";
import {Limit} from "./Limit";
import {Loading} from "../Loading";
import {OrderBy} from "./OrderBy";
import PropTypes from "prop-types";
import React, {useEffect} from "react";
import {Results} from "./Results";
import {Select} from "./Select";
import Tab from "react-bootstrap/Tab";
import Tabs from "react-bootstrap/Tabs";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../types/types_hooks";
import {Where} from "./Where";

/**
 * An interface for the props of the QueryBuilder component.
 */
export interface Props {

    /**
     * The metadata of the dataset that the user has selected to query, this includes the schema.
     */
    metadata: trac.metadata.ITag
    /**
     *  The key in the QueryBuilderStore to use to save the data to / get the data from.
     */
    storeKey: keyof QueryBuilderStoreState["uses"]
}

export const QueryBuilder = (props: Props) => {

    console.log("Rendering QueryBuilder")

    const {metadata, storeKey} = props

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Typescript hygiene
    if (!metadata.header) throw new Error("The selected dataset for the QueryBuilder component does not have a header tag.")
    
    // A unique key for the object which takes account of the dataset version
    const objectKey = createUniqueObjectKey(metadata.header, false)

    /**
     * A hook that runs when the page mounts that checks if there is an entry in the store
     * for the selected dataset and if not creates one with all the default settings. This
     * causes an extra re-render on loading the QueryBuilder component but enables us to eliminate
     * a lot of re-renders while the user is using the tool. It also sets the current part of the
     * store in use.
     */
    useEffect(() => {

        // Update the query builder store
        dispatch(setCurrentlyLoadedQuery({storeKey, objectKey}))
        dispatch(createQueryEntry({storeKey, objectKey, metadata}))

    }, [dispatch, storeKey, metadata, objectKey])
    
    // Get what we need from the store
    const {objectKey: currentlyLoadedObjectKey} = useAppSelector(state => state["queryBuilderStore"].currentlyLoaded)

    // If in entry for the dataset does not yet exist don't show the interface. The useEffect hook will create one.
    if (currentlyLoadedObjectKey == null || currentlyLoadedObjectKey !== objectKey) {
        return (<Loading/>)
    }

    return (

        <React.Fragment>
            <Editor storeKey={storeKey} objectKey={objectKey}/>

            <Tabs id={"queryBuilder"} defaultActiveKey="select" className={"mt-4"}>
                <Tab className={"tab-content-bordered pb-4 min-height-tab-pane "} title={"Select"} eventKey={"select"}>
                    <Select storeKey={storeKey} objectKey={objectKey}/>
                </Tab>
                <Tab className={"tab-content-bordered pb-4 min-height-tab-pane "} title={"Where"} eventKey={"where"}>
                    <Where storeKey={storeKey} objectKey={objectKey}/>
                </Tab>
                <Tab className={"tab-content-bordered pb-4 min-height-tab-pane "} title={"Group by"} eventKey={"groupBy"}>
                    <GroupBy storeKey={storeKey} objectKey={objectKey}/>
                </Tab>
                <Tab className={"tab-content-bordered pb-4 min-height-tab-pane "} title={"Order by"} eventKey={"orderBy"}>
                    <OrderBy storeKey={storeKey} objectKey={objectKey}/>
                </Tab>
                <Tab className={"tab-content-bordered pb-4 min-height-tab-pane "} title={"Limit"} eventKey={"limit"}>
                    <Limit storeKey={storeKey} objectKey={objectKey}/>
                </Tab>
            </Tabs>

            <Results storeKey={storeKey} objectKey={objectKey}/>

        </React.Fragment>
    )
};

QueryBuilder.propTypes = {

    metadata: PropTypes.object.isRequired,
    storeKey: PropTypes.string.isRequired
};