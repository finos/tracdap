/**
 * This scene allows the user upload a schema to TRAC from a GitHub repository.
 *
 * @module UploadASchemaScene
 * @category Scene
 */

import PropTypes from "prop-types";
import React from "react";
import {SceneTitle} from "../../components/SceneTitle";
import {UploadFromGitHub} from "../../components/UploadFromGitHub";

/**
 * An interface for the props of the UploadASchemaScene index component.
 */
export interface Props {

    /**
     * The title for the scene, this is set in config_menu.tsx.
     */
    title: string
}

const UploadASchema = (props: Props) => {

    console.log("Rendering UploadASchemaScene")

    const {title} = props;

    return (
        <React.Fragment>
            <SceneTitle text={title}/>
            <UploadFromGitHub storeKey={"uploadASchema"} uploadType={"schema"}/>
        </React.Fragment>
    )
};

UploadASchema.propTypes = {

    title: PropTypes.string.isRequired
};

export default UploadASchema