/**
 * A component that allows the user to set metadata for the selected file to upload to TRAC. This is justa wrapper
 * for a component in order to optimize the rendering.
 *
 * @module SetMetadata
 * @category Component
 */

import PropTypes from "prop-types";
import React from "react";
import {SetAttributes} from "../../SetAttributes/SetAttributes";
import {useAppSelector} from "../../../../types/types_hooks";
import {type UploadFromGitHubStoreState} from "../store/uploadFromGitHubStore";

/**
 * An interface for the props of the SetMetadata component.
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

export const SetMetadata = (props: Props) => {

    const {storeKey, uploadType} = props

    // Get what we need from the store
    const {tracModelClassOptions} = useAppSelector(state => state["uploadFromGitHubStore"].uses[storeKey].file.model)
    const {errorMessages, fields} = useAppSelector(state => state["uploadFromGitHubStore"].uses[storeKey].file.schema)
    const {status} = useAppSelector(state => state["uploadFromGitHubStore"].uses[storeKey].file)

    // Show if we have ever got a file's info, we do this rather than succeeded to stop the UI jerking as
    // components are hidden if they change file
    let show: boolean = false
    if (storeKey === "uploadAModel") {

        show = Boolean(status !== "idle" && tracModelClassOptions.length > 0)

    } else if (storeKey === "uploadASchema") {

        show = Boolean(status !== "idle" && fields != null && errorMessages.length === 0)
    }

    return (

        <SetAttributes show={show}
                       storeKey={storeKey}
                       title={`Set ${uploadType} attributes`}
        >
            When loading a {uploadType} it is tagged with metadata or attributes that help users understand
            what the item contains and helps the application handle it correctly. The editor below allows
            you to set this metadata. If you make a mistake or want to change the information about an item
            in TRAC then the Admin tool can be used to update the tags.
        </SetAttributes>
    )
};

SetMetadata.propTypes = {

    storeKey: PropTypes.string.isRequired,
    uploadType: PropTypes.oneOf(["model", "schema"]).isRequired
};