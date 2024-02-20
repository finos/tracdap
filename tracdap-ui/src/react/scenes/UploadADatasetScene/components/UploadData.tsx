/**
 * A component that shows a button that initiates the load of a dataset into TRAC.
 *
 * @module UploadData
 * @category UploadADatasetScene component
 */

import {Button} from "../../../components/Button";
import Col from "react-bootstrap/Col";
import {deleteFile, uploadDataToTrac} from "../store/uploadADatasetStore";
import {General} from "../../../../config/config_general";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {objectsContainsValue} from "../../../utils/utils_object";
import {ProgressModal} from "../../../components/ProgressModal";
import React, {useState} from "react";
import Row from "react-bootstrap/Row";
import {resetAttributesToDefaults, setValidationChecked} from "../../../components/SetAttributes/setAttributesStore";
import {showToast} from "../../../utils/utils_general";
import {StreamingPayload} from "../../../../types/types_general";
import {TextBlock} from "../../../components/TextBlock";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

export const UploadData = () => {

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store
    const {isValid} = useAppSelector(state => state["setAttributesStore"].uses.uploadADataset.attributes.validation)
    const {status: uploadStatus} = useAppSelector(state => state["uploadADatasetStore"].upload)
    const {status: importStatus} = useAppSelector(state => state["uploadADatasetStore"].import)
    const {foundInTrac, isExistingSchemaTheSame, isSuggestedSchemaTheSame, tag} = useAppSelector(state => state["uploadADatasetStore"].alreadyInTrac)
    const {selectedTab, selectedOption: selectedExistingSchema, options} = useAppSelector(state => state["uploadADatasetStore"].existingSchemas)
    const {selectedOption: selectedPriorVersion, isPriorVersionSchemaTheSame} = useAppSelector(state => state["uploadADatasetStore"].priorVersion)

    /**
     * A hook for the state of the ProgressModal
     */
    const [modalState, setModalState] = useState<StreamingPayload & { show: boolean, allowUserToCloseModal: boolean }>({
        message: '',
        show: false,
        allowUserToCloseModal: false,
        duration: 0,
        completed: 0,
        toDo: 0
    })

    /**
     * A function that checks whether the attributes meet their validation rules before initiating a dataset load into TRAC.
     */
    const handleUploadToTrac = (): void => {

        if (objectsContainsValue(isValid, false)) {

            dispatch(setValidationChecked({storeKey: "uploadADataset", value: true}))
            showToast("error", "There are problems with some of your attributes, please fix the issues shown and try again.", "not-valid")

        } else {

            dispatch(setValidationChecked({storeKey: "uploadADataset", value: false}))
            dispatch(uploadDataToTrac({onStart, onProgress, onComplete, onError}))
        }
    }

    /**
     * A function that is passed as the function that runs when streaming a dataset to load into TRAC. It runs in
     * the {@link importCsvDataFromReference} function. It runs when the stream starts.
     */
    const onStart = ({toDo, completed, message}: StreamingPayload) => {
        setModalState((prevState) => ({...prevState, ...{toDo, completed, message, show: true, allowUserToCloseModal: false}}))
    }

    /**
     * A function that is passed as the function that runs when streaming a dataset to load into TRAC. It runs in
     * the {@link importCsvDataFromReference} function. It runs when the stream completes. It updates the progress
     * modal to show how far through the load the upload is.
     */
    const onComplete = ({toDo, completed, message, duration}: StreamingPayload) => {

        // Clear the state of the scene
        dispatch(deleteFile())
        dispatch(resetAttributesToDefaults({storeKey: "uploadADataset"}))

        // If the duration of the load is less than 20 seconds then auto close the modal. The user really only wants to
        // have feedback if it is very long, and they come back they want to see the progress. 'duration' is in milliseconds.
        setModalState((prevState) => ({...prevState, ...{show: (duration ?? 0) > 20000, allowUserToCloseModal: true, toDo, completed, message}}))
    }

    /**
     * A function that is passed as the function that runs when streaming a dataset to load into TRAC. It runs in
     * the {@link importCsvDataFromReference} function. It runs when the stream errors.
     */
    const onProgress = ({toDo, completed, message}: StreamingPayload) => {
        setModalState((prevState) => ({...prevState, ...{toDo, completed, message}}))
    }

    /**
     * A function that is passed as the function that runs when streaming a dataset to load into TRAC. It runs in
     * the {@link importCsvDataFromReference} function. It runs when the stream errors.
     */
    const onError = ({toDo, message}: StreamingPayload) => {
        // The to Do value can be set as -1 for an error to tigger an error message
        setModalState((prevState) => ({...prevState, message, allowUserToCloseModal: true, toDo}))
    }

    /**
     * A function that calculates whether the upload button should be disabled or not, the logic is quite complex as
     * it needs to look across the whole scene.
     */
    const isDisabled = (): boolean => {

        // If the file import is not completed or an existing load to TRAC is underway
        const first = (uploadStatus === "pending" || importStatus !== "succeeded")
        // Or the user has the suggested schema tab open, we are trying to prohibit duplication, and we have found the same file in TRAC with the same schema
        const second = (!General.loading.allowCopies.data && selectedTab === "suggested" && foundInTrac && tag && isSuggestedSchemaTheSame)
        // Or the user has the existing schema tab open, there are either no options or no selected option
        const third = selectedTab === "existing" && (options.length === 0 || !selectedExistingSchema)
        // Or the user has the existing schema tab open, they have selected an existing schema, but it matches the one in the loaded dataset
        const fourth = (!General.loading.allowCopies.data && selectedTab === "existing" && foundInTrac && tag && isExistingSchemaTheSame)
        // Or they have selected a prior version of the dataset and the schema does not match
        const fifth = (selectedPriorVersion && !isPriorVersionSchemaTheSame)
        return Boolean(first || second || third || fourth || fifth)
    }

    return (
        <React.Fragment>
            {importStatus !== "idle" &&

                <React.Fragment>
                    <HeaderTitle type={"h3"} text={`Load dataset`}/>

                    <TextBlock>
                        Click on the button below to load the dataset into TRAC, a message will be shown if the upload
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
                                Upload dataset
                            </Button>
                        </Col>
                    </Row>

                </React.Fragment>
            }

            <ProgressModal allowUserToCloseModal={modalState.allowUserToCloseModal}
                           completed={modalState.completed}
                           modalText={modalState.toDo < 0 ? "An error occurred" : modalState.completed === modalState.toDo && modalState.allowUserToCloseModal ? "You can now exit" : "Please do not refresh the page or navigate away"}
                           progressBarText={modalState.message}
                           show={modalState.show}
                           toDo={modalState.toDo}
                           toggle={setModalState}
            />
        </React.Fragment>
    )
};