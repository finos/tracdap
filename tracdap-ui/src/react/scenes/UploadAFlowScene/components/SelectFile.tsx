import {Button} from "../../../components/Button";
import Col from "react-bootstrap/Col";
import {ConfirmButton} from "../../../components/ConfirmButton";
import {deleteFile, saveFileToStore} from "../store/uploadAFlowStore";
import {FileImportModal} from "../../../components/FileImportModal/FileImportModal";
import React, {useCallback, useState} from "react";
import Row from "react-bootstrap/Row";
import {TextBlock} from "../../../components/TextBlock";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

// This is the prop to the FileImportModal, we set it here outside the component so that the array does
// not trigger a rerender of FileImportModal if this component re-renders.
const allowedFileTypes: string[] = ["json"]

/**
 * A component that controls the modal that allows the user to select a local file to load as a flow.
 */
const SelectFile = () => {

    console.log("Rendering SelectFileButton")

    const [showImportModal, setShowImportModal] = useState(false)

    // Get what we need from the store
    const {status} = useAppSelector(state => state["uploadAFlowStore"].import)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A function that runs when the user opens or closes the upload modal.
     */
    const onToggleImportModal = useCallback(() => {

        setShowImportModal(!showImportModal)

    }, [showImportModal])

    /**
     * A function that handles the request to delete the loaded flow information. Both stores need updating, hence the handler.
     */
    const handleDeleteFile = useCallback(() => {

        dispatch(deleteFile())
        // TODO move to the component
        //dispatch(resetAttributes({storeKey: "uploadADataset"}))

    }, [dispatch])

    return (
        <React.Fragment>

            <TextBlock>
                This tool can be used to load json files into TRAC and save them as a flow. These flows will
                be available to run from the &apos;Run a flow&apos; page. First select the file that you want to upload.
            </TextBlock>

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
                                       description={"Are you sure that you want to remove the loaded file, all attribute changes will be lost?"}
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

            <FileImportModal allowedFileTypes={allowedFileTypes}
                             dispatchedReturnImportedData={saveFileToStore}
                             importAsAFile={true}
                             onToggleModal={onToggleImportModal}
                             show={showImportModal}
            />
        </React.Fragment>
    )
};

export default SelectFile;