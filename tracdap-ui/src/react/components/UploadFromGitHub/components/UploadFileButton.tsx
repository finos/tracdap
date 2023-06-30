/**
 * A component that shows a button that initiates the load of a model into TRAC.
 *
 * @module UploadFileButton
 * @category Component
 */

import {Button} from "../../Button";
import Col from "react-bootstrap/Col";
import {General} from "../../../../config/config_general";
import {HeaderTitle} from "../../HeaderTitle";
import {Link} from "react-router-dom";
import {objectsContainsValue} from "../../../utils/utils_object";
import PropTypes from "prop-types";
import React from "react";
import Row from "react-bootstrap/Row";
import {setValidationChecked} from "../../SetAttributes/setAttributesStore";
import {showToast} from "../../../utils/utils_general";
import {TextBlock} from "../../TextBlock";
import {type UploadFromGitHubStoreState, uploadModelToTrac, uploadSchemaToTrac} from "../store/uploadFromGitHubStore";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

/**
 * An interface for the props of the UploadFileButton component.
 */
export interface Props {

    /**
     * The key in the UploadFromGitHubStore to get the state for this component
     */
    storeKey: keyof UploadFromGitHubStoreState["uses"]
    /**
     * The type of file being selected, it is used in messages to the user.
     */
    uploadType: "model" | "schema"
}

export const UploadFileButton = (props: Props) => {

    const {storeKey, uploadType} = props

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store
    const {isValid} = useAppSelector(state => state["setAttributesStore"].uses[storeKey].attributes.validation)
    const {file, upload} = useAppSelector(state => state["uploadFromGitHubStore"].uses[storeKey])
    const {status, model: {tracModelClassOptions}, schema: {errorMessages}} = useAppSelector(state => state["uploadFromGitHubStore"].uses[storeKey].file)

    /**
     * A function that checks whether the attributes meet their validation rules before initiating a model load into TRAC.
     */
    const handleUploadToTrac = (): void => {

        if (objectsContainsValue(isValid, false)) {

            dispatch(setValidationChecked({storeKey, value: true}))
            showToast("error", "There are problems with some of your attributes, please fix the issues shown and try again.", "not-valid")

        } else {

            dispatch(setValidationChecked({storeKey, value: false}))
            if (storeKey === "uploadAModel") {
                dispatch(uploadModelToTrac({storeKey}))
            } else if (storeKey === "uploadASchema") {
                dispatch(uploadSchemaToTrac({storeKey}))
            }
        }
    }

    let show: boolean = false
    if (storeKey === "uploadAModel") {

        // tracModelClassOptions is an array of the classes in the code file that use the TRAC model API, at
        // least one is needed for a model to load successfully, this is only needed for models
        show = Boolean(file.status !== "idle" && tracModelClassOptions.length > 0)

    } else if (storeKey === "uploadASchema") {

        show = Boolean(status !== "idle" && errorMessages.length === 0)
    }

    // Whether the button should be disabled, we initially set it using common criteria to all use cases
    let disabled: boolean = Boolean(upload.status === "pending" || file.status !== "succeeded")

    // Now set the use case specific logic
    if (storeKey === "uploadAModel") {

        disabled = disabled || Boolean(!General.loading.allowCopies.model && file.alreadyInTrac.foundInTrac && file.alreadyInTrac.tag)

    } else if (storeKey === "uploadASchema") {

        disabled = disabled || Boolean(!General.loading.allowCopies.schema && file.alreadyInTrac.foundInTrac && file.alreadyInTrac.tag)
    }

    return (
        <React.Fragment>
            {show &&

                <React.Fragment>

                    <HeaderTitle type={"h3"} text={`Load ${uploadType}`}/>
                    {/*TODO add a My jobs page*/}
                    <TextBlock>
                        Click on the button below to load the file into TRAC, a message will be shown if the upload was
                        successfully submitted.

                        {storeKey === "uploadAModel" &&
                            <span>
                                To see whether the job completed or not the progress of the job can be tracked in the <Link to={"/find-a-job"}>Find a Job</Link> and
                                &apos;My jobs&apos; pages.
                                </span>
                        }
                    </TextBlock>

                    <Row>
                        <Col>
                            <Button ariaLabel={`Upload ${uploadType}`}
                                    className={"min-width-px-150"}
                                    disabled={disabled}
                                    isDispatched={false}
                                    loading={Boolean(upload.status === "pending")}
                                    onClick={handleUploadToTrac}
                            >
                                Upload {uploadType}
                            </Button>
                        </Col>
                    </Row>

                </React.Fragment>
            }
        </React.Fragment>
    )
};

UploadFileButton.propTypes = {

    storeKey: PropTypes.string.isRequired,
    uploadType: PropTypes.oneOf(["model", "schema"]).isRequired
};