/**
 * A component that allows the user to search for objects in TRAC, they can select filters and perform searches.
 * They can then see summary metadata for their selections or click to load the object in a dedicated viewer.
 *
 * @module FindInTrac
 * @category Component
 */

import {doSearch, type FindInTracStoreState, getTableState, saveSelectedRowsAndGetTags} from "./findInTracStore";
import {Filters} from "./Filters";
import {Loading} from "../Loading";
import PropTypes from "prop-types";
import React, {useEffect, useMemo} from "react";
import {ShowHideDetails} from "../ShowHideDetails";
import {ShowSelectedObjectDetails} from "./ShowSelectesObjectDetails";
import type {SortingState} from "@tanstack/react-table";
import {Table} from "../Table/Table";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {Types} from "../../../config/config_trac_classifications";
import {useAppDispatch, useAppSelector} from "../../../types/types_hooks";
import {type Variants} from "../../../types/types_general";
import {ViewItemButton} from "./ViewItemButton";

const badgeColumns = ["jobStatus"]
const initialSort: SortingState = [{id: "updatedTime", desc: true}]
const uniqueRowIdentifier = ["objectId", "objectVersion", "tagVersion"]

// Create a lookup between the job status and the colour of the badge in the table
// This uses the config options
let badgeColours: Record<string, Variants> = {}

Types.tracJobStatuses.map(jobStatus => {
    badgeColours[jobStatus.value] = jobStatus.details.variant
})

// This is a made up tag for the search result table data that is used when downloading the data in the Table
// component, these pieces of information are used to set the download name and also set the initial hidden
// columns, basically spoofing as if the table was actually a dataset in TRAC.
const tag: trac.metadata.ITag = {
    "header": {
        "objectType": trac.ObjectType.DATA
    },
    "attrs": {
        "key": {
            "type": {
                "basicType": trac.STRING
            },
            "stringValue": "export_of_trac_search_results"
        },
        "hidden_columns": {
            "type": {"basicType": trac.BasicType.ARRAY, "arrayType": {"basicType": trac.STRING}},
            "arrayValue": {
                "items": [
                    {"type": {"basicType": trac.BasicType.STRING}, "stringValue": "key"},
                    {"type": {"basicType": trac.BasicType.STRING}, "stringValue": "objectType"},
                    {"type": {"basicType": trac.BasicType.STRING}, "stringValue": "objectTypeAsString"},
                    {"type": {"basicType": trac.BasicType.STRING}, "stringValue": "objectVersion"},
                    {"type": {"basicType": trac.BasicType.STRING}, "stringValue": "tagVersion"},
                    {"type": {"basicType": trac.BasicType.STRING}, "stringValue": "objectId"},
                    {"type": {"basicType": trac.BasicType.STRING}, "stringValue": "createdBy"},
                    {"type": {"basicType": trac.BasicType.STRING}, "stringValue": "creationTime"},
                    {"type": {"basicType": trac.BasicType.STRING}, "stringValue": "modelPath"}
                ]
            }
        }
    }
}

/**
 * An interface for the props of the FindInTrac component.
 */
export interface Props {

    /**
     *  The key in the FindInTracStore to use to save the data to/ get the data from.
     */
    storeKey: keyof FindInTracStoreState["uses"]
}

export const FindInTrac = (props: Props) => {

    console.log("Rendering FindInTrac")

    const {storeKey} = props

    // Get what we need from the store
    const {schema} = useAppSelector(state => state["findInTracStore"].resultsSettings)

    // Because this component includes a table we want to pay attention to the optimisation, when we destructure we
    // add a function to define if a render should happen when the properties are extracted.
    const {
        searchByAttributes, searchByObjectId, showFiltersOnLoad, selectedTab,
        selectedResults: {searchByAttributes: searchByAttributesSelected, searchByObjectId: searchByObjectIdSelected}
    } = useAppSelector(state => state["findInTracStore"].uses[storeKey], (prev, next) => {
        return prev.selectedResults.searchByObjectId === next.selectedResults.searchByObjectId && prev.selectedResults.searchByAttributes === next.selectedResults.searchByAttributes && prev.showFiltersOnLoad === next.showFiltersOnLoad && prev.selectedTab === next.selectedTab && prev.searchByAttributes === next.searchByAttributes && prev.searchByObjectId === next.searchByObjectId
    })

    const {allowClickThrough} = useAppSelector(state => state["findInTracStore"].uses[storeKey].options)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // This is a bit bloated, but it means that we avoid loading the root of the store user and causing additional
    // renders
    const initialSearchComplete = selectedTab === "searchByAttributes" ? searchByAttributes.initialSearchComplete : searchByObjectId.initialSearchComplete
    const status = selectedTab === "searchByAttributes" ? searchByAttributes.status : searchByObjectId.status
    const objectTypeResultsAreFor = selectedTab === "searchByAttributes" ? searchByAttributes.objectTypeResultsAreFor : searchByObjectId.objectTypeResultsAreFor
    const results = selectedTab === "searchByAttributes" ? searchByAttributes.table.results : searchByObjectId.table.results
    const tableState = selectedTab === "searchByAttributes" ? searchByAttributes.table.tableState : searchByObjectId.table.tableState
    const selectedResults = selectedTab === "searchByAttributes" ? searchByAttributesSelected : searchByObjectIdSelected

    console.log(`LOG :: Search status is ${status}`)

    /**
     * A hook that runs the search function when the component loads, populating the table with the results from the
     * default search. We only have to do this for the searchByAttributes tab.
     */
    useEffect(() => {

        if (!searchByAttributes.initialSearchComplete) {
            console.log("LOG :: Running search call")
            dispatch(doSearch({storeKey}))
        }

    }, [dispatch, searchByAttributes.initialSearchComplete, storeKey])

    // We only show a loading icon on the first search per type of search, after that there will be a table of
    // results showing that is updated after the search completes
    const isFirstSearchRunning = initialSearchComplete === false && status === "pending"

    // The dataset has different fields depending on the type of object searched for, so we filter out the items in the
    // schema that we don't need for the search that has been done. We useMemo because the filter returns a new schema
    // each render if we do not.
    const finalSchema = useMemo(() => schema.filter(variable => {

        if (objectTypeResultsAreFor === trac.ObjectType.JOB) {
            return variable.fieldName && !["modelRepository", "modelPath"].includes(variable.fieldName)
        } else if (objectTypeResultsAreFor === trac.ObjectType.MODEL) {
            return variable.fieldName && !["jobType", "jobStatus"].includes(variable.fieldName)
        } else {
            return variable.fieldName && !["modelRepository", "modelPath", "jobType", "jobStatus"].includes(variable.fieldName)
        }

    }), [objectTypeResultsAreFor, schema])

    return (

        <React.Fragment>

            <ShowHideDetails linkText={"search filters"} showOnOpen={showFiltersOnLoad}>
                <Filters storeKey={storeKey}/>
            </ShowHideDetails>

            {isFirstSearchRunning &&
                <Loading/>
            }

            {/*Only show the table after a search has completed */}
            {!isFirstSearchRunning && status !== "idle" &&
                <React.Fragment>

                    <Table badgeColours={badgeColours}
                           badgeColumns={badgeColumns}
                           data={results}
                           footerComponent={allowClickThrough ? <ViewItemButton storeKey={storeKey}/> : undefined}
                           getSelectedRowsFunction={saveSelectedRowsAndGetTags}
                           noDataMessage={"There are no search results"}
                           noOfRowsSelectable={1}
                           id={selectedTab}
                           initialShowGlobalFilter={false}
                           initialShowInformation={true}
                           initialSort={initialSort}
                           isTracData={false}
                           savedTableState={tableState}
                           saveTableStateFunction={getTableState}
                           schema={finalSchema}
                           selectedRows={selectedResults}
                           showExportButtons={true}
                           storeKey={storeKey}
                           tag={tag}
                           uniqueRowIdentifier={uniqueRowIdentifier}
                    />

                    <ShowSelectedObjectDetails storeKey={storeKey}/>
                </React.Fragment>
            }
        </React.Fragment>
    )
};

FindInTrac.propTypes = {

    storeKey: PropTypes.string.isRequired,
};