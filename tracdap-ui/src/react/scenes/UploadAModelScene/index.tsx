/**
 * This scene allows the user upload a model to TRAC from a GitHub repository.
 *
 * @module UploadAModelScene
 * @category Scene
 */

import PropTypes from "prop-types";
import React from "react";
import {SceneTitle} from "../../components/SceneTitle";
import {UploadFromGitHub} from "../../components/UploadFromGitHub";

/**
 * An interface for the props of the UploadAModelScene index component.
 */
export interface Props {

    /**
     * The title for the scene, this is set in config_menu.tsx.
     */
    title: string
}

const UploadAModel = (props: Props) => {

    console.log("Rendering UploadAModelScene")

    const {title} = props;

    return (
        <React.Fragment>
            <SceneTitle text={title}/>
            <UploadFromGitHub storeKey={"uploadAModel"} uploadType={"model"}/>
        </React.Fragment>
    )
};

UploadAModel.propTypes = {

    title: PropTypes.string.isRequired
};

export default UploadAModel