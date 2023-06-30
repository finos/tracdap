/**
 * A component that controls the modal that allows the user to select a local file to load as a dataset.
 * @module
 * @category Component
 */

import {Button} from "../../../components/Button";
import Col from "react-bootstrap/Col";
import {ConfirmButton} from "../../../components/ConfirmButton";
import {deleteFile, saveFileToStore} from "../store/uploadADatasetStore";
import {FileImportModal} from "../../../components/FileImportModal/FileImportModal";
import {General} from "../../../../config/config_general";
import React, {useCallback, useState} from "react";
import {resetAttributesToDefaults} from "../../../components/SetAttributes/setAttributesStore";
import Row from "react-bootstrap/Row";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

export const SelectFile = () => {

    console.log("Rendering SelectFileButton")

    const [showImportModal, setShowImportModal] = useState(false)

    // Get what we need from the store
    const {status} = useAppSelector(state => state["uploadADatasetStore"].import)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A function that runs when the user opens or closes the upload modal.
     */
    const onToggleImportModal = useCallback(() => {

        setShowImportModal(!showImportModal)

    }, [showImportModal])

    /**
     * A function that handles the request to delete the loaded file information. Both stores need updating, hence the handler.
     */
    const handleDeleteFile = useCallback(() => {
        
        dispatch(deleteFile())
        dispatch(resetAttributesToDefaults({storeKey: "uploadADataset"}))
        
    }, [dispatch])

    return (
        <React.Fragment>

            <Row className={"pb-3"}>
                <Col xs={"auto"}>
                    <Button ariaLabel={"Select file to load"}
                            className={"my-4 min-width-px-150"}
                            isDispatched={false}
                            onClick={onToggleImportModal}
                            variant={"info"}
                    >
                        Select file
                    </Button>

                    {status === "succeeded" &&
                        <ConfirmButton ariaLabel={"Remove file"}
                                       className={"my-4 ms-3 min-width-px-150"}
                                       description={"Are you sure that you want to remove the loaded file, all schema and attribute changes will be lost?"}
                                       onClick={handleDeleteFile}
                                       variant={"secondary"}
                        >
                            Remove file
                        </ConfirmButton>
                    }
                </Col>
                <Col className={"fs-13 my-auto"}>
                    {status !== "succeeded" &&
                        "No file selected"
                    }
                </Col>
            </Row>

            <FileImportModal allowedFileTypes={General.tables.importFileTypes}
                             dispatchedReturnImportedData={saveFileToStore}
                             onToggleModal={onToggleImportModal}
                             show={showImportModal}
            />

        </React.Fragment>
    )
};