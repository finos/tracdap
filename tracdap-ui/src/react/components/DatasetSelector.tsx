/**
 * A component that shows a menu for the input datasets for a flow or input/output datasets from a job that
 * allows the user to pick one and view the data in a table or a chart depending on the attributes of the data.
 *
 * @module DatasetSelector
 * @category Component
 */

import * as arrow from "apache-arrow";
import type {DataRow, SearchOption} from "../../types/types_general";
import {DatasetSelectorNavBar} from "./DatasetSelectorNavBar";
import {General} from "../../config/config_general";
import {downloadDataAsStream} from "../utils/utils_trac_api";
import {Loading} from "./Loading";
import {isTagOption} from "../utils/utils_trac_type_chckers";
import Nav from "react-bootstrap/Nav";
import Navbar from "react-bootstrap/Navbar";
import PropTypes from "prop-types";
import React, {useCallback, useState} from "react";
import {showToast} from "../utils/utils_general";
import type {SingleValue} from "react-select";
import {Table} from "./Table/Table";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../types/types_hooks";

/**
 * An interface for the props of the DatasetSelector component.
 */
export interface Props {

    /**
     * The metadata tags for the datasets to show in the menu, keyed by the key used in the job.
     */
    datasets?: Record<string, trac.metadata.ITag> | Record<string, SingleValue<SearchOption>>
}

/**
 * An interface for the state of the DatasetSelectorNavBar component.
 */
export interface State {

    /**
     * The data for the selected option.
     */
    data: undefined | DataRow[],
    /**
     * Whether the data is being downloaded.
     */
    isDownloading: boolean,
    /**
     * The schema for the downloaded data.
     */
    schema: undefined | trac.metadata.ISchemaDefinition,
    /**
     * The metadata tag for the selected dataset.
     */
    tag: undefined | trac.metadata.ITag
}

const initialState = {
    data: undefined,
    isDownloading: false,
    schema: undefined,
    tag: undefined
}

export const DatasetSelector = (props: Props) => {

    const {datasets} = props

    // Get what we need from the store
    const {"trac-tenant": tenant} = useAppSelector(state => state["applicationStore"].cookies)

    const [{data, isDownloading, schema, tag}, setDataInfo] = useState<State>(initialState)

    // A function that gets the data as a stream and converts the arrow file to a JSON array.
    const handleGetData = useCallback((key: string | null) => {

        async function fetchMyAPI(tenant: string, tagSelector: trac.metadata.ITagSelector): Promise<{ data: DataRow[], schema: trac.metadata.ISchemaDefinition }> {

            // datasets?.[key] can be null for optional inputs that are given a key but the user can set a null value
            if (!datasets || key == null || datasets?.[key] === null) throw new Error("The dataset to fetch or the key for the dataset were not available.")

            // The selected dataset, can be a TRAC metadata tag or an option
            const selectedDataset = datasets?.[key]

            // Get the metadata tag depending on the type of selectedDataset
            const tag = (isTagOption(selectedDataset) ? selectedDataset.tag : selectedDataset) ?? undefined

            // Update the UI to show the selected data option in the dropdown as soon as possible, otherwise it is unclear what data is being shown
            // Also block the UI while it is downloading and show a loading icon
            setDataInfo(prevState => ({...prevState, isDownloading: true, tag, data: undefined}))

            // Request data in arrow format to improve performance
            const {content: data, schema} = await downloadDataAsStream({tenant, tagSelector, format: "application/vnd.apache.arrow.stream"});
            let table = arrow.tableFromIPC(data);

            // Limit the amount of data displayed on-screen
            if (General.tables.displayTableLimit !== null) {
                const limit = Math.min(table.numRows, General.tables.displayTableLimit);
                table = table.slice(0, limit);
            }

            // Convert arrow data back into native JS format
            // Eventually it would be good to work on the arrow data directly
            // However, this is still a lot faster than parsing JSON
            return {data: table.toArray(), schema};
        }

        if (!datasets || key == null || datasets[key] == null) return

        // The selected dataset, can be a TRAC metadata tag or an option
        const selectedDataset = datasets?.[key]

        // Get the metadata tag depending on the type of selectedDataset
        const tagSelector = isTagOption(selectedDataset) ? selectedDataset.tag.header : selectedDataset?.header

        if (tagSelector != null && tenant !== undefined) fetchMyAPI(tenant, tagSelector).then((results) => {

            setDataInfo(prevState => ({...prevState, isDownloading: false, data: results.data, schema: results.schema}))

        }).catch(error => {

            setDataInfo(prevState => ({...prevState, isDownloading: false, data: undefined}))
            showToast("error", {message: "Getting the data failed", details: typeof error === "string" ? error : undefined}, "fetchMyAPI/rejected")
            console.error(error)
        })

    }, [datasets, tenant])

    return (

        <React.Fragment>

            {data && schema?.table?.fields &&
                <React.Fragment>

                    <Table data={data}
                           headerComponent={<DatasetSelectorNavBar datasets={datasets}
                                                                   disabled={isDownloading}
                                                                   handleGetData={handleGetData}
                                                                   tag={tag}/>}
                           initialShowGlobalFilter={false}
                           initialShowInformation={true}
                           isEditable={false}
                           isTracData={true}
                           schema={schema.table.fields}
                           showExportButtons={true}
                           tag={tag}
                    />
                </React.Fragment>
            }

            {(!data || !schema?.table?.fields) &&
                <React.Fragment>
                    <Navbar bg="table" expand={true} className={"ps-2 pe-2 mt-3 table-navbar"}>
                        <Navbar.Collapse>

                            <Nav>
                                <DatasetSelectorNavBar datasets={datasets}
                                                       disabled={isDownloading}
                                                       handleGetData={handleGetData}
                                                       tag={tag}
                                />
                            </Nav>
                        </Navbar.Collapse>
                    </Navbar>

                    {isDownloading &&
                        <Loading text={"Downloading ..."}/>
                    }
                </React.Fragment>
            }

        </React.Fragment>
    )
};

DatasetSelector.propTypes = {

    datasets: PropTypes.object
};