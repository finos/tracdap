import {Button} from "./Button";
import Col from "react-bootstrap/Col";
// import ConfirmButton from "../../../components/ConfirmButton";
// import {deleteFile, saveFileToStore} from "../store/uploadADatasetStore";
import {FileImportModal} from "./FileImportModal/FileImportModal";
import {General} from "../../config/config_general";
import React, {useCallback, useState} from "react";
import Row from "react-bootstrap/Row";
import {useAppDispatch, useAppSelector} from "../../types/types_hooks";
import {FileImportModalPayload} from "../../types/types_general";
import {saveFile} from "../utils/utils_trac_api";
import Modal from "react-bootstrap/Modal";
import {commasAndOrs, showToast} from "../utils/utils_general";
import {DragAndDrop} from "./DragAndDrop";
import {Icon} from "./Icon";
import {SelectOption} from "./SelectOption";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {Alert} from "./Alert";
import {SetAttributes} from "./SetAttributes/SetAttributes";
import {setAttributesAutomatically, setValidationChecked} from "./SetAttributes/setAttributesStore";
import {uploadDataToTrac} from "../scenes/UploadADatasetScene/store/uploadADatasetStore";
import {File} from "./FileTree/File";
import {objectsContainsValue} from "../utils/utils_object";
import {humanReadableFileSize} from "../utils/utils_number";
import {createTagsFromAttributes} from "../utils/utils_attributes_and_parameters";

/**
 * A component that allows a local file to be selected, loaded into TRAC and attached to another object.
 */
const ButtonToAttachDocument = () => {

    console.log("Rendering SelectFileButton")

    const [showImportModal, setShowImportModal] = useState(false)
    const [showAttributesModal, setShowAttributesModal] = useState(false)
    const [filePayload, setFilePayload] = useState<null | FileImportModalPayload>(null)

    // Get what we need from the store
    const {["trac-tenant"]: tenant} = useAppSelector(state => state["applicationStore"].cookies)

    /**
     * A function that runs when the user opens or closes the upload modal.
     */
    const onToggleImportModal = useCallback(() => {

        setShowImportModal(!showImportModal)

    }, [showImportModal])

    const {
        processedAttributes,
        values,
        validation: {isValid}
    } = useAppSelector(state => state["setAttributesStore"].uses["attachDocument"].attributes)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()


    /**
     * A function that checks whether the attributes meet their validation rules before initiating a model load into TRAC.
     */
    const handleUploadToTrac = (): void => {

        if (objectsContainsValue(isValid, false)) {

            dispatch(setValidationChecked({storeKey: "attachDocument", value: true}))
            showToast("error", "There are problems with some of your attributes, please fix the issues shown and try again.", "not-valid")

        } else if (filePayload !== null && tenant !== undefined) {

            dispatch(setValidationChecked({storeKey: "attachDocument", value: false}))

            saveFile(tenant, {
                // TODO fix this so that there is a warning that it is not an array
                content: ArrayBuffer.isView(filePayload.data) ? filePayload.data : new Uint8Array([]),
                mimeType: filePayload.fileInfo.mimeType,
                name: "this_is_a_test",
                // name: filePayload.fileInfo.fileName.replace(/\s/g, "_"),
                attrs: createTagsFromAttributes(processedAttributes, values),
                priorVersion: undefined,
                size: filePayload.fileInfo.sizeInBytes,
            })

            // TODO put in promise chain
            setShowAttributesModal(false)
        }
    }

    return (
        <React.Fragment>

            <Row className={"pb-3"}>
                <Col xs={"auto"}>
                    <Button ariaLabel={"Select file to attach"}
                            className={"my-4 min-width-px-150"}
                            isDispatched={false}
                            onClick={onToggleImportModal}
                            variant={"info"}
                    >
                        Attach new document
                    </Button>
                </Col>
                {/*<Col className={"fs-13 my-auto"}>*/}
                {/*    {status !== "succeeded" &&*/}
                {/*        "No file selected"*/}
                {/*    }*/}
                {/*</Col>*/}
            </Row>

            <Modal onHide={() => setShowAttributesModal(false)}
                   show={showAttributesModal}
                // TODO Reset the state when the modal is closed
                //onExit={() => updateFileImportState({type: "reset"})}
                // Prevent the modal closing by clicking outside
                   backdrop={"static"}
                   keyboard={false}
                   size={"xl"}
            >

                <Modal.Header closeButton>
                    <Modal.Title>
                        Set file attributes
                    </Modal.Title>
                </Modal.Header>

                <Modal.Body className={"mx-5"}>

                    <SetAttributes show={true} storeKey={"attachDocument"}/>

                </Modal.Body>

                <Modal.Footer className={"mt-2"}>

                    <Button ariaLabel={"Close"}
                            isDispatched={false}
                            onClick={() => setShowAttributesModal(false)}
                            variant={"secondary"}>
                        Close
                    </Button>

                    <Button ariaLabel={"Upload file"}
                            isDispatched={false}
                            disabled={filePayload === null}
                        // loading={returning}
                            onClick={handleUploadToTrac}
                            variant={"info"}
                    >
                        <Icon ariaLabel={false}
                              className={"me-2"}
                              icon={"bi-file-earmark-arrow-up"}
                        />
                        Upload file
                    </Button>

                </Modal.Footer>
            </Modal>

            <FileImportModal allowedFileTypes={General.files.importFileTypes}
                             importAsAFile={true}
                             onToggleModal={onToggleImportModal}
                             returnImportedData={(payload: FileImportModalPayload) => {

                                 dispatch(setAttributesAutomatically({
                                     storeKey: "attachDocument",
                                     values: {
                                         name: payload.fileInfo.fileName,
                                         original_file_name: payload.fileInfo.fileName,
                                         original_file_size: payload.fileInfo.sizeInBytes,
                                         original_file_modified_date: payload.fileInfo.lastModifiedTracDate
                                     }
                                 }))

                                 setFilePayload(payload)
                                 setShowAttributesModal(true)
                             }}
                             show={showImportModal}
            />
        </React.Fragment>
    )
};

export default ButtonToAttachDocument;