/**
 * A component that allows the user to set metadata for the selected dataset. This is justa wrapper
 * for a component in order to optimize the rendering of the scene.
 *
 * @module SetMetadata
 * @category Component
 */

import React from "react";
import {SetAttributes} from "../../../components/SetAttributes/SetAttributes";
import {useAppSelector} from "../../../../types/types_hooks";

export const SetMetadata = () => {

    // Get what we need from the store
    const {fileInfo} = useAppSelector(state => state["uploadADatasetStore"].import)

    return (
        <SetAttributes show={Boolean(fileInfo)}
                       storeKey={"uploadADataset"}
                       title={`Set dataset attributes`}
        >
            When loading a dataset it is tagged with metadata or attributes that help users understand
            what the item contains and helps the application handle it correctly. The editor below allows
            you to set this metadata. If you make a mistake or want to change the information about an item
            in TRAC then the Admin tool can be used to update the tags.
        </SetAttributes>
    )
};