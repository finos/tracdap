/**
 * This scene allows the user to set up and then review a job that runs a flow before submission.
 *
 * @module RunAFlowScene
 * @category Scene
 */

import React from "react";
import {ReviewJob} from "./ReviewJob";
import {SetUpJob} from "./SetUpJob";
import {useAppSelector} from "../../../types/types_hooks";

const RunAFlowScene = () => {

    console.log("Rendering RunAFlowScene")

    // Get what we need from the store
    const setUpOrReview = useAppSelector(state => state["runAFlowStore"].setUpOrReview)

    return (
        setUpOrReview === "setup" ? <SetUpJob/> : <ReviewJob/>
    )

};

export default RunAFlowScene;