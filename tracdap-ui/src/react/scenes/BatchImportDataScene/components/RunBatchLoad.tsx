/**
 * A component that runs the batch upload.
 * @module
 * @category BatchDataImportScene component
 */

import Col from "react-bootstrap/Col";
import {ConfirmButton} from "../../../components/ConfirmButton";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {Icon} from "../../../components/Icon";
import {Link} from "react-router-dom";
import {objectsContainsValue} from "../../../utils/utils_object";
import React from "react";
import Row from "react-bootstrap/Row";
import {runImportJobs, setValidationChecked} from "../store/batchImportDataStore";
import {showToast, sOrNot} from "../../../utils/utils_general";
import {TextBlock} from "../../../components/TextBlock";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

export const RunBatchLoad = () => {

    // Get what we need from the stores
    const {status: getItemsStatus} = useAppSelector(state => state["applicationSetupStore"].tracItems.getSetupItems)
    const {selectedImportData, validation} = useAppSelector(state => state["batchImportDataStore"])

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // The number of items to upload
    const numberOfSelections = Object.keys(selectedImportData).length

    /**
     * A function that checks whether the dates meet their validation rules before initiating a model load into TRAC.
     */
    const handleRunImports = (): void => {

        console.log(validation)
        console.log(objectsContainsValue(validation.isValid, false))
        if (objectsContainsValue(validation.isValid, false)) {

            dispatch(setValidationChecked(true))
            showToast("error", "There are problems with some of your dates, please fix the issues shown and try again.", "not-valid")

        } else {

            setValidationChecked(false)
            dispatch(runImportJobs())
        }
    }

    return (

        <React.Fragment>
            {numberOfSelections > 0 &&
                <React.Fragment>
                    <HeaderTitle type={"h3"} text={"Start uploading"}/>

                    <TextBlock>
                        Clicking the button below will start your {numberOfSelections > 0 ? numberOfSelections : ""} upload{sOrNot(numberOfSelections)}.
                        Before each upload is started each is checked to see if the dataset has already been stored. You can view the status of each load
                        by checking the <Link to={"/find-a-job"}>Find a Job</Link> page.
                    </TextBlock>

                    <Row>
                        <Col>
                            <ConfirmButton ariaLabel={"Start batch upload"}
                                           className={"min-width-px-100 float-end"}
                                           description={"Are you sure that you want to start your uploads?"}
                                           disabled={Boolean(getItemsStatus !== "succeeded" || numberOfSelections === 0)}
                                           onClick={handleRunImports}
                            >
                                <Icon ariaLabel={false}
                                      className={"me-2"}
                                      icon={"bi-upload"}
                                />
                                Start batch load
                            </ConfirmButton>
                        </Col>
                    </Row>
                </React.Fragment>
            }
        </React.Fragment>
    )
};