/**
 * This scene allows the user upload a flow to TRAC from a local file.
 *
 * @module UploadAFlowScene
 * @category Scene
 */

import PropTypes from "prop-types";
import React from "react";
import {SceneTitle} from "../../components/SceneTitle";
import {SetMetadata} from "./components/SetMetadata";
import SelectFile from "./components/SelectFile";
import SelectedFlow from "./components/SelectedFlow";
import {UploadFlow} from "./components/UploadFlow";

/**
 * An interface for the props of the UploadAModelScene index component.
 */
export interface Props {

    /**
     * The title for the scene.
     */
    title: string
}

const UploadAFlow = (props: Props) => {

    console.log("Rendering UploadAFlowScene")

    const {title} = props;

    return (

        <React.Fragment>
            <SceneTitle text={title}/>
            <SelectFile/>
            <SelectedFlow/>
            <SetMetadata/>
            <UploadFlow/>
        </React.Fragment>
    )
};

UploadAFlow.propTypes = {

    title: PropTypes.string.isRequired
};

export default UploadAFlow;
