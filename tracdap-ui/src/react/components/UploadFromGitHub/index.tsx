/**
 * A component that enables the user to upload files from a GitHub repository as
 * objects into TRAC.
 *
 * @module UploadFromGitHub
 * @category Component
 */

import {CommitInformation} from "./components/CommitInformation";
import {GitHubLogin} from "./components/GitHubLogin";
import {LoggedInUser} from "./components/LoggedInUser";
import PropTypes from "prop-types";
import React from "react";
import {RepositoryInfo} from "./components/RepositoryInfo";
import {SelectBranch} from "./components/SelectBranch";
import {SelectCommit} from "./components/SelectCommit";
import {SelectedFileDetails} from "./components/SelectedFileDetails";
import {SelectFileButton} from "./components/SelectFileButton";
import {SelectRepository} from "./components/SelectRepository";
import {SetMetadata} from "./components/SetMetadata";
import {UploadFileButton} from "./components/UploadFileButton";
import {UploadFromGitHubStoreState} from "./store/uploadFromGitHubStore";

/**
 * An interface for the props of the UploadFromGitHub component.
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

export const UploadFromGitHub = (props: Props) => {

    const {storeKey, uploadType} = props
    
    return (

       <React.Fragment>
            <GitHubLogin storeKey={storeKey}/>
            <LoggedInUser storeKey={storeKey}/>
            <SelectRepository storeKey={storeKey} uploadType={uploadType}/>
            <RepositoryInfo storeKey={storeKey}/>
            <SelectBranch storeKey={storeKey} uploadType={uploadType}/>
            <SelectCommit storeKey={storeKey} uploadType={uploadType}/>
            <CommitInformation storeKey={storeKey}/>
            <SelectFileButton storeKey={storeKey} uploadType={uploadType}/>
            <SelectedFileDetails storeKey={storeKey} uploadType={uploadType}/>
            <SetMetadata storeKey={storeKey} uploadType={uploadType}/>
            <UploadFileButton storeKey={storeKey} uploadType={uploadType}/>
        </React.Fragment>
    )
};

UploadFromGitHub.propTypes = {

    storeKey: PropTypes.string.isRequired,
    uploadType: PropTypes.oneOf(["model", "schema"]).isRequired
};