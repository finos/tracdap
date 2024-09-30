/**
 * A component that shows a button that initiates the load of a flow into TRAC.
 *
 * @module UploadModel
 * @category UploadAModelScene component
 */
import {Button} from "../../../components/Button";
import Col from "react-bootstrap/Col";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {objectsContainsValue} from "../../../utils/utils_object";
import React from "react";
import Row from "react-bootstrap/Row";
import {setValidationChecked} from "../../../components/SetAttributes/setAttributesStore";
import {showToast} from "../../../utils/utils_general";
import {TextBlock} from "../../../components/TextBlock";
import {uploadFlowToTrac} from "../store/uploadAFlowStore";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

export const UploadFlow = () => {

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store
    const {isValid} = useAppSelector(state => state["setAttributesStore"].uses.runAFlow.attributes.validation)
    const {status: uploadStatus} = useAppSelector(state => state["uploadAFlowStore"].upload)
    const {status: importStatus} = useAppSelector(state => state["uploadAFlowStore"].import)

    /**
     * A function that checks whether the attributes meet their validation rules before initiating a model load into TRAC.
     */
    const handleUploadToTrac = (): void => {

        if (objectsContainsValue(isValid, false)) {

            dispatch(setValidationChecked({storeKey: "uploadAFlow", value: true}))
            showToast("error", "There are problems with some of your attributes, please fix the issues shown and try again.", "not-valid")

        } else {

            dispatch(setValidationChecked({storeKey: "uploadAFlow", value: false}))
            dispatch(uploadFlowToTrac())
        }
    }

    /**
     * A function that calculates whether the upload button should be disabled or not, the logic is quite complex as
     * it needs to look across the whole scene.
     */
    const isDisabled = () : boolean => {

        // // If the file import is not completed or an existing load to TRAC is underway
        // const first = (uploadStatus === "pending" || importStatus !== "succeeded")
        // // Or we are trying to prohibit duplication, and we have found the same file in )TRAC
        // const second = (!General.loading.allowCopies.flow && foundInTrac && tag)
        // return Boolean(first || second)
        return false
    }

    return (
        <React.Fragment>
            {importStatus !== "idle" &&

                <React.Fragment>
                    <HeaderTitle type={"h3"} text={`Load flow`}/>

                    <TextBlock>
                        Click on the button below to load the flow into TRAC, a message will be shown if the upload
                        was successfully submitted. To see whether the job completed or not the progress of the job can
                        be tracked in the &apos;Find a job&apos; and &apos;My jobs&apos; pages.
                    </TextBlock>

                    <Row>
                        <Col>
                            <Button ariaLabel={"Upload dataset"}
                                    className={"min-width-px-150"}
                                    disabled={isDisabled()}
                                    isDispatched={false}
                                    loading={Boolean(uploadStatus === "pending")}
                                    onClick={handleUploadToTrac}
                            >
                                Upload flow
                            </Button>
                        </Col>
                    </Row>

                </React.Fragment>
            }
        </React.Fragment>
    )
};

export default UploadFlow;
