/**
 * This scene allows the user to set up and then review a job that runs a model before submission.
 * @module RunAModelScene
 * @category Scene
 */

import React from "react";
import {ReviewJob} from "./ReviewJob";
import {SetUpJob} from "./SetUpJob";
import {useAppSelector} from "../../../types/types_hooks";

const RunAModelScene = () => {

    console.log("Rendering RunAModelScene")

    // Get what we need from the store
    const setUpOrReview = useAppSelector(state => state["runAModelStore"].setUpOrReview)

    return (
        setUpOrReview === "setup" ? <SetUpJob/> : <ReviewJob/>
    )

};

export default RunAModelScene;