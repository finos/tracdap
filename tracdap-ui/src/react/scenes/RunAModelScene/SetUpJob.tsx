/**
 * A scene page that allows the user to select a model that they want to run and select the,
 * input datasets and parameters to use. When the user has selected all the options they
 * can then move to view a summary report before executing the model.
 * @module SetUpJob
 * @category RunAModelScene page
 */

import {BusinessSegments} from "./components/BusinessSegments";
import {GotoReview} from "./components/GotoReview";
import {Loading} from "./components/Loading";
import React from "react";
import {SceneTitle} from "../../components/SceneTitle";
import {SelectModel} from "./components/SelectModel";
import {SelectInputs} from "./components/SelectInputs";
import {SelectParameters} from "./components/SelectParameters";
import {useAppSelector} from "../../../types/types_hooks";
import {ValidateInputs} from "./components/ValidateInputs";
import {ValidateInputSchemas} from "./components/ValidateInputSchemas";

export const SetUpJob = () => {

    console.log("Rendering SetUpJob")

    // Get what we need from the store
    const status = useAppSelector(state => state["runAModelStore"].model.status)

    return (

        <React.Fragment>

            <SceneTitle text={"Setup your model"}/>
            <BusinessSegments storeKey={"runAModel"}/>
            <SelectModel/>
            <Loading/>

            {status === "succeeded" &&
                <React.Fragment>

                    <SelectParameters/>
                    <SelectInputs/>
                    <ValidateInputs/>
                    <ValidateInputSchemas/>
                    <GotoReview/>

                </React.Fragment>
            }
        </React.Fragment>
    )
};