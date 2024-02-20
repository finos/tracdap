/**
 * A component that allows the user to set metadata for the selected flow. This is justa wrapper
 * for a component in order to optimize the rendering of the scene.
 *
 * @module SetMetadata
 * @category UploadAFlowScene component
 */

import React from "react";
import {SetAttributes} from "../../../components/SetAttributes/SetAttributes";
import {useAppSelector} from "../../../../types/types_hooks";

export const SetMetadata = () => {

    const {foundInTrac, tag} = useAppSelector(state => state["uploadAFlowStore"].alreadyInTrac)
    const {status: importStatus} = useAppSelector(state => state["uploadAFlowStore"].import)

    {/*Show if we have ever got a file's info, we do this rather than succeeded to stop
    the UI jerking as components are hidden if the user changes file*/}
    const show = Boolean(importStatus !== "idle" && !(foundInTrac && tag))

    return (

        <SetAttributes show={show}
                       storeKey={"uploadAFlow"}
                       title={"Set flow attributes"}
        >
            When loading a flow it is tagged with metadata or attributes that help users understand
            what the item contains and helps the application handle it correctly. The editor below allows
            you to set this metadata. If you make a mistake or want to change the information about an item
            in TRAC then the Admin tool can be used to update the tags.
        </SetAttributes>
    )
};