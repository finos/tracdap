/**
 * A component that shows the user a summary of the user selected data for the batch import. These are shown in
 * a carousel.
 * @module
 * @category BatchDataImportScene component
 */

import {BatchDatasetDetails} from "./BatchDatasetDetails";
import Carousel from "react-bootstrap/Carousel";
import Col from "react-bootstrap/Col";
import {HeaderTitle} from "../../../components/HeaderTitle";
import React, {useMemo} from "react";
import Row from "react-bootstrap/Row";
import {TextBlock} from "../../../components/TextBlock";
import {useAppSelector} from "../../../../types/types_hooks";

// Some Bootstrap grid layouts
const singleGrid = {span: 10, offset: 1}
const multipleGrid = 12

export const BatchLoadSummary = () => {

    // Get what we need from the stores
    const {data} = useAppSelector(state => state["applicationSetupStore"].tracItems.items.ui_batch_import_data.currentDefinition)
    const {selectedImportData} = useAppSelector(state => state["batchImportDataStore"])

    // The records from the import batch definitions that the user has selected
    const selectedBatchDataItems = useMemo(() => (

        data.filter(row => row.DATASET_ID !== null && Object.keys(selectedImportData).includes(row.DATASET_ID))

    ), [data, selectedImportData])

    // The number of records to show
    const numberOfSelections = selectedBatchDataItems.length

    return (

        <React.Fragment>
            {numberOfSelections > 0 &&

                <React.Fragment>
                    <HeaderTitle type={"h3"} text={"Full details"}/>

                    <TextBlock>
                        The table below shows further information about what will be loaded and the reconciliation information
                        that will be checked for each batch import selected. Note that the TRAC application can not see what
                        datasets have been moved into the landing area so loads for any datasets not transferred will fail.
                    </TextBlock>

                    <Row>
                        {/*Indent left and right if there is just one, otherwise the carousel will indent it*/}
                        <Col xs={12} lg={numberOfSelections === 1 ? singleGrid : multipleGrid} className={"pt-2 pb-3"}>

                            <Carousel controls={Boolean(numberOfSelections > 1)}
                                      indicators={Boolean(numberOfSelections > 1 && numberOfSelections < 15)}
                                      interval={null}
                                      variant="dark"
                            >
                                {selectedBatchDataItems.map((row, i) => (

                                    // The "px-5" is to make the control arrows show outside the table, there is a Boostrap scss variable that
                                    // controls their width
                                    <Carousel.Item key={row.DATASET_ID || i} className={`${numberOfSelections < 2 ? "" : "px-5"}`}>
                                        <BatchDatasetDetails batchImportDefinition={row}/>
                                    </Carousel.Item>
                                ))}
                            </Carousel>

                        </Col>
                    </Row>
                </React.Fragment>
            }
        </React.Fragment>
    )
};