/**
 * A component that controls a modal that allows the user to select either a model or a dataset from a GitHub
 * repository.
 *
 * @module SelectFile
 * @category Component
 */

import {Button} from "../../Button";
import Col from "react-bootstrap/Col";
import {FileTreeModal} from "./FileTreeModal";
import {getFile, type UploadFromGitHubStoreState} from "../store/uploadFromGitHubStore";
import {HeaderTitle} from "../../HeaderTitle";
import {type SelectedFileDetails} from "../../../../types/types_general";
import PropTypes from "prop-types";
import React, {useEffect, useState} from "react";
import Row from "react-bootstrap/Row";
import {TextBlock} from "../../TextBlock";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

/**
 * An interface for the props of the SelectFileButton component.
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

export const SelectFileButton = (props: Props) => {

    const {storeKey, uploadType} = props

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store
    const {status, selectedOption} = useAppSelector(state => state["uploadFromGitHubStore"].uses[storeKey].tree)

    // Whether the file tree modal is open or not
    const [show, toggle] = useState(false)

    // The selected file before being saved to the store
    const [selectedFileDetails, setSelectedFileDetails] = useState<null | SelectedFileDetails>(selectedOption)

    /**
     * A hook that keeps the state of this component in sync with changes in the store. If you don't do this
     * then when you save the details of the selected file in the store it's out of sync in the state, this state
     * is used by the file tree to know which folders to show as open and which file to so as highlighted.
     */
    useEffect((): void => {

        setSelectedFileDetails(selectedOption)

    }, [selectedOption])

    /**
     * A function that stores the selected file in the Redux store and closes the modal. This is
     * needed because if someone opens the modal, selects a file and then closes it without clicking
     * save you would expect no file to be selected. Only when the modal is exited using save would
     * you expect the file to be saved.
     */
    const updateStoreAndToggle = (): void => {

        dispatch(getFile({selectedFileDetails, storeKey}))
        toggle(!show)
    }

    /**
     * A function that reverts the selected file held locally to the value in the Redux store and closes
     * the modal. This is needed to keep the two in sync in case the user selected a file but did not save
     * the selection.
     */
    const revertToStoreAndToggle = (): void => {

        setSelectedFileDetails(selectedOption)
        toggle(!show)
    }

    return (
        <React.Fragment>
            {status !== "idle" &&

                <React.Fragment>

                    <HeaderTitle type={"h3"} text={`Select your ${uploadType} to load`}/>

                    <TextBlock>
                        Click on the button below to load the file tree of your chosen commit and select
                        the {uploadType} that you want to load.
                    </TextBlock>

                    <Row className={"pb-3"}>
                        <Col xs={"auto"}>
                            <Button ariaLabel={"Open file selector"}
                                    className={"min-width-px-150"}
                                    disabled={Boolean(status !== "succeeded")}
                                    isDispatched={false}
                                    onClick={() => toggle(!show)}
                            >
                                Select file
                            </Button>
                        </Col>
                        <Col className={"fs-13 my-auto"}>
                            {selectedOption == null &&
                                "No file selected"
                            }
                        </Col>
                    </Row>

                    <FileTreeModal revertToStoreAndToggle={revertToStoreAndToggle}
                                   selectedFileDetails={selectedFileDetails}
                                   setSelectedFileDetails={setSelectedFileDetails}
                                   show={show}
                                   storeKey={storeKey}
                                   updateStoreAndToggle={updateStoreAndToggle}
                                   uploadType={uploadType}
                    />
                </React.Fragment>
            }
        </React.Fragment>
    )
};

SelectFileButton.propTypes = {

    storeKey: PropTypes.string.isRequired,
    uploadType: PropTypes.oneOf(["model", "schema"]).isRequired
};