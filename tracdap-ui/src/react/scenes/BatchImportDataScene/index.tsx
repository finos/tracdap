/**
 * This scene allows the user to select data that has been moved to the TRAC waiting area and ingest it into
 * TRAC.
 * @module BatchImportDataScene
 * @category Scene
 */

import {BatchLoadSummary} from "./components/BatchLoadSummary";
import {EnvironmentNotice} from "./components/EnvironmentNotice";
import PropTypes from "prop-types";
import React from "react";
import {RunBatchLoad} from "./components/RunBatchLoad";
import {SceneTitle} from "../../components/SceneTitle";
import {SelectBatchDatasets} from "./components/SelectBatchDatasets";
import {Alert} from "../../components/Alert";

/**
 * An interface for the props of the BatchImportDataScene index component.
 */
export interface Props {

    /**
     * The main title for the page, this is set in the {@link MenuConfig}.
     */
    title: string
}

export const BatchImportDataScene = (props: Props) => {

    console.log("Rendering BatchImportDataScene")

    const {title} = props;

    return (

        <React.Fragment>

            <Alert className={"mt-4 py-3"} showBullets={false} variant={"danger"}>
                This is for demo purposes only.
            </Alert>

            <SceneTitle text={title}/>

            <EnvironmentNotice/>

            <SelectBatchDatasets/>

            <BatchLoadSummary/>

            <RunBatchLoad/>

        </React.Fragment>
    )
};

BatchImportDataScene.propTypes = {

    title: PropTypes.string.isRequired
};

// This additional export is needed to be able to load this scene lazily in config_menu.tsx
export default BatchImportDataScene;