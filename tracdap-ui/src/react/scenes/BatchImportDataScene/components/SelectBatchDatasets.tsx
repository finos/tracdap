/**
 * A component that shows the user a summary of each dataset that can be batched into a load onto the environment
 * they are using. For datasets that require it the user much provide dates for the imports.
 * @module
 * @category BatchDataImportScene component
 */

import BatchDatasetHeader from "./BatchDatasetHeader";
import BatchDatasetRow from "./BatchDatasetRow";
import Col from "react-bootstrap/Col";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {Link} from "react-router-dom";
import {Loading} from "../../../components/Loading";
import React, {useCallback, useEffect, useState} from "react";
import {reconcileToBatchImportDataUpdate} from "../store/batchImportDataStore";
import Row from "react-bootstrap/Row";
import {SelectValue} from "../../../components/SelectValue";
import {ButtonPayload, SelectValuePayload, UiBatchImportDataRow} from "../../../../types/types_general";
import Table from "react-bootstrap/Table";
import {TextBlock} from "../../../components/TextBlock";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";
import {BatchDatasetDetailsModal} from "./BatchDatasetDetailsModal";

// Some Bootstrap grid layouts
const mdGrid = {span: 6, offset: 6}
const lgGrid = {span: 5, offset: 7}

/**
 * A function that runs when the user changes the search term, it returns true if certain pieces of information
 * about the dataset include the search term.
 * @param item - The dataset information.
 * @param searchTerm - The user's search term.
 * @returns Whether the search term is found in the dataset information.
 */
const filterDatasets = (item: UiBatchImportDataRow, searchTerm: null | string) => {

    searchTerm = searchTerm ? searchTerm.toUpperCase() : null

    return searchTerm === null || searchTerm === "" || (item !== null && ((item["DATASET_NAME"] || "").toUpperCase().includes(searchTerm) || (item["DATASET_DESCRIPTION"] || "").toUpperCase().includes(searchTerm)))
}

/**
 * A component that shows when there are no batch data imports defined.
 */
const WidgetTableNoRows = () => (

    <tr>
        <td colSpan={6} className={"text-center text-nowrap"}>
            There are no batch data imports configured.
        </td>
    </tr>
)

/**
 * A component that shows when there are no results from the user's search.
 */
const WidgetTableNoSearchResult = () => (

    <tr>
        <td colSpan={6} className={"text-center text-nowrap"}>
            There are no search results.
        </td>
    </tr>
)

export const SelectBatchDatasets = () => {

    // Get what we need from te store
    const {data} = useAppSelector(state => state["applicationSetupStore"].tracItems.items.ui_batch_import_data.currentDefinition)
    const {status: getItemsStatus} = useAppSelector(state => state["applicationSetupStore"].tracItems.getSetupItems)
    const {selectedImportData, selectedDates, validation: {validationChecked}} = useAppSelector(state => state["batchImportDataStore"])

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A hook listening for changes to the batch data import definitions, this is owned and edited in the {@link ApplicationSetupScene}.
     * This is needed because if the dataset is updated then some of the user's selections in this component may not be valid, and we
     * need a way of updating if this is the case.
     */
    useEffect(() => {

        // Don't run if no datasets have been selected yet
        dispatch(reconcileToBatchImportDataUpdate(data))

    }, [data, dispatch])

    // State for storing the search term set by the user.
    const [searchTerm, setSearchTerm] = useState<null | string>(null);
    // State for whether to show the modal with the full description of the data that can be batch-loaded.
    const [viewDetails, setViewDetails] = useState<{ show: boolean, batchDatasetId: null | string }>({show: false, batchDatasetId: null});

    /**
     * A function that stores the dataset ID clicked on by the user to view in the modal and then
     * toggles the modal to show the details.
     */
    const handleViewDetails = useCallback((payload: ButtonPayload) => {

        const {name} = payload

        if (typeof name === "string") {

            setViewDetails({show: true, batchDatasetId: name})
        }

    }, [])

    /**
     * A function that handles the user changing the search term.
     * @param payload - The payload from the {@link SelectValue} component that
     * contains the search term.
     */
    const handleSearchChange = useCallback((payload: SelectValuePayload) => {

        setSearchTerm(payload.value === null ? null : payload.value.toString())

    }, [])

    const filteredData = data.filter(item => filterDatasets(item, searchTerm))

    return (

        <React.Fragment>
            {getItemsStatus === "pending" && <Loading text={"Please wait ..."}/>}

            {getItemsStatus === "succeeded" &&
                <React.Fragment>
                    <HeaderTitle type={"h3"} text={"Select datasets to load"}/>

                    <TextBlock>
                        The table below shows all of the datasets that are available to load into TRAC. These are moved
                        across from their source system to the TRAC landing area on a regular basis. The list of data that
                        can be batch loaded can be edited in the <Link to={"/admin/setup"}>Application setup</Link>.
                        Dataset loads marked as disabled are still shown but can&apos;t be selected.
                    </TextBlock>

                    <Row>
                        <Col xs={12} md={mdGrid} lg={lgGrid}>
                            <SelectValue basicType={trac.STRING}
                                         className={"mb-2 w-100"}
                                         isDispatched={false}
                                         labelText={"Search:"}
                                         labelPosition={"left"}
                                         onChange={handleSearchChange}
                                         mustValidate={false}
                                         showValidationMessage={false}
                                         validateOnMount={false}
                                         value={searchTerm}
                            />
                        </Col>
                    </Row>

                    <Table className={"dataHtmlTable fs-7 my-3"} responsive>
                        <BatchDatasetHeader/>
                        <tbody>
                        {data.length === 0 && <WidgetTableNoRows/>}
                        {filteredData.map(row => (

                            <BatchDatasetRow date={row.DATASET_ID && selectedDates.hasOwnProperty(row.DATASET_ID) ? selectedDates[row.DATASET_ID] : null}
                                             key={row.DATASET_ID}
                                             load={Boolean(row.DATASET_ID !== null && selectedImportData.hasOwnProperty(row.DATASET_ID))}
                                             row={row}
                                             toggleModal={handleViewDetails}
                                             validationChecked={validationChecked}
                            />

                        ))}
                        {data.length > 0 && filteredData.length === 0 && <WidgetTableNoSearchResult/>}

                        </tbody>
                    </Table>
                </React.Fragment>
            }

            {/*A modal that lets the user view the complete record for a batch import definition, they can also see this by just selecting*/}
            {/*the option but we allow them to be viewed separately*/}
            {viewDetails.batchDatasetId &&
                <BatchDatasetDetailsModal batchDatasetId={viewDetails.batchDatasetId} show={viewDetails.show} toggle={setViewDetails}/>
            }
        </React.Fragment>
    )
};