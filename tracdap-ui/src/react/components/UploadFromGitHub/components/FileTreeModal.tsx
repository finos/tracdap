/**
 * A component that shows a modal that allows the user to select either a model or a dataset from a GitHub
 * repository.
 *
 * @module SelectFile
 * @category Component
 */

import {Button} from "../../Button";
import Col from "react-bootstrap/Col";
import {commasAndAnds} from "../../../utils/utils_general";
import {FileTree} from "../../FileTree/FileTree";
import {Icon} from "../../Icon";
import Modal from "react-bootstrap/Modal";
import React, {useMemo} from "react";
import Row from "react-bootstrap/Row";
import {type SelectedFileDetails} from "../../../../types/types_general";
import {SelectToggle} from "../../SelectToggle";
import {toggleShowUnselectableFiles, type UploadFromGitHubStoreState} from "../store/uploadFromGitHubStore";
import {useAppSelector} from "../../../../types/types_hooks";
import PropTypes from "prop-types";

// Some Bootstrap grid layouts
const lgGrid = {span: 8, offset: 2}
const mdGrid = {span: 10, offset: 1}
const xsGrid = {span: 12, offset: 0}

/**
 * An interface for the props of the FileTreeModal component.
 */
export interface Props {

    /**
     * A function that reverts the selected file held locally to the value in the Redux store and closes
     * the modal.
     */
    revertToStoreAndToggle: () => void
    /**
     The details of the selected file.
     */
    selectedFileDetails: null | SelectedFileDetails
    /**
     A function that saves the details of the selected file to the parent component's state.
     */
    setSelectedFileDetails: React.Dispatch<React.SetStateAction<null | SelectedFileDetails>>
    /**
     * Whether to show the modal.
     */
    show: boolean
    /**
     * The key in the UploadFromGitHubStore to get the state for this component
     */
    storeKey: keyof UploadFromGitHubStoreState["uses"]
    /**
     * A function that stores the selected file in the Redux store and closes the modal.
     */
    updateStoreAndToggle: () => void
    /**
     * The type of file being selected, it is used in messages to the user.
     */
    uploadType: "model" | "schema"
}

export const FileTreeModal = (props: Props) => {

    const {
        revertToStoreAndToggle,
        selectedFileDetails,
        setSelectedFileDetails,
        show,
        storeKey,
        updateStoreAndToggle,
        uploadType
    } = props

    // Get what we need from the store
    const {
        fileTree,
        folderTree,
        showUnselectableFiles,
        treeHasSelectableItem,
        treeHasUnselectableItem
    } = useAppSelector(state => state["uploadFromGitHubStore"].uses[storeKey].tree)

    const {
        modelFileExtensions
    } = useAppSelector(state => state["uploadFromGitHubStore"].uses[storeKey].repository)

    // If the user has selected a file and closes the modal, then opens it we want the folders to that file to be
    // open on reloading the modal. This sets the folders to set as open.
    const openFoldersOnLoad = useMemo(() : string[]=> {

        if (show) {

            let openFoldersOnLoad = []

            if (selectedFileDetails) {

                const pathAsArray = selectedFileDetails.path?.split('/') ?? []

                // Remove the file from the path
                pathAsArray.pop()

                // Paths to add
                const maxPathsToAdd = pathAsArray.length

                // Add each step in the path as a folder to have open
                for (let i = 1; i <= maxPathsToAdd; i++) {

                    openFoldersOnLoad.push(pathAsArray.join("/"))
                    pathAsArray.pop()
                }
            }

            return openFoldersOnLoad

        } else {

            return []
        }

    }, [selectedFileDetails, show])

    return (

        <Modal size={"lg"} show={show} onHide={revertToStoreAndToggle}>

            <Modal.Header closeButton>
                <Modal.Title>
                    Select a {uploadType} to upload
                </Modal.Title>
            </Modal.Header>

            <Modal.Body>

                <React.Fragment>

                    {modelFileExtensions && modelFileExtensions.length > 0 &&
                        <Row>
                            <Col xs={xsGrid} md={mdGrid} lg={lgGrid} className={"mt-3 mb-4"}>
                                <Row className={"border py-3"}>
                                    <Col xs={"auto"}>
                                        <SelectToggle labelText={"Show all files"}
                                                      labelPosition={"top"}
                                                      onChange={toggleShowUnselectableFiles}
                                                      value={showUnselectableFiles}
                                                      isDispatched={true}
                                                      mustValidate={false}
                                                      validateOnMount={false}
                                        />
                                    </Col>
                                    <Col className={"fs-13 my-auto pt-1"}>
                                        Only certain types of files can be loaded as
                                        a {uploadType} ({commasAndAnds(modelFileExtensions.map(extension => `.${extension}`))}).
                                        By default the file tree is filtered to only show these files but if you want to
                                        see all of the files then this option will change the filtering.
                                    </Col>
                                </Row>
                            </Col>
                        </Row>
                    }

                    <Row>
                        <Col className={"pl-0"} xs={xsGrid} md={mdGrid} lg={lgGrid}>

                            {!treeHasSelectableItem && treeHasUnselectableItem &&
                                <span className={"d-block mb-3"}>This commit does not contain a {uploadType} that can be uploaded.</span>
                            }

                            {!treeHasSelectableItem && !treeHasUnselectableItem &&
                                <span className={"d-block mb-3"}>This commit does not contain any files.</span>
                            }

                            {fileTree && folderTree &&
                                <FileTree allowMultipleMenusOpen={true}
                                          fileTree={fileTree}
                                          folderTree={folderTree}
                                          openFoldersOnLoad={openFoldersOnLoad}
                                          onSelectFile={setSelectedFileDetails}
                                          showUnselectableFiles={showUnselectableFiles}
                                          selectedFileDetails={selectedFileDetails}
                                />
                            }

                        </Col>
                    </Row>

                </React.Fragment>

            </Modal.Body>

            <Modal.Footer>
                <Button ariaLabel={"Close file selector"}
                        isDispatched={false}
                        onClick={revertToStoreAndToggle}
                        variant={"secondary"}
                >
                    Close
                </Button>
                <Button ariaLabel={"Save selected file"}
                        isDispatched={false}
                        onClick={updateStoreAndToggle}
                        variant={"info"}
                >
                    <Icon icon={"bi-save"}
                           ariaLabel={false}
                           className={"me-2"}
                    />
                    Save
                </Button>
            </Modal.Footer>

        </Modal>
    )
};

FileTreeModal.propTypes = {

    revertToStoreAndToggle: PropTypes.func.isRequired,
    selectedFileDetails: PropTypes.object,
    setSelectedFileDetails: PropTypes.func.isRequired,
    show: PropTypes.bool.isRequired,
    storeKey: PropTypes.string.isRequired,
    uploadType: PropTypes.oneOf(['model', 'schema']).isRequired
};