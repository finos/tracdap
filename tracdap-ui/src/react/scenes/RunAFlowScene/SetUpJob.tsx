/**
 * A scene page that allows the user to select a flow that they want to run and select the models,
 * input datasets and parameters to use. When the user has selected all the options they
 * can then move to view a summary report before executing the flow.
 * @module SetUpJob
 * @category RunAFlowScene page
 */

import {BusinessSegments} from "./components/BusinessSegments";
import {GotoReview} from "./components/GotoReview";
import {Loading} from "./components/Loading";
import {ModelChainBuilder} from "./components/ModelChainBuilder";
import React from "react";
import {SceneTitle} from "../../components/SceneTitle";
import {SelectFlow} from "./components/SelectFlow";
import {SelectInputs} from "./components/SelectInputs";
import {SelectParameters} from "./components/SelectParameters";
import {SelectPolicy} from "./components/SelectPolicy";
import {useAppSelector} from "../../../types/types_hooks";
import {ValidateDatasets} from "./components/ValidateDatasets";
import {ValidateInputSchemas} from "./components/ValidateInputSchemas";
import {ValidateInputs} from "./components/ValidateInputs";

export const SetUpJob = () => {

    // Get what we need from the store
    const status = useAppSelector(state => state["runAFlowStore"].flow.status)

    return (

        <React.Fragment>

            <SceneTitle text={"Setup your job"}/>
            <BusinessSegments storeKey={"runAFlow"}/>
            <SelectFlow/>
            <SelectPolicy/>
            <Loading/>

            {/*The components below disappear when a new flow is selected*/}
            <ModelChainBuilder className={"mb-2"}
                               show={status === "succeeded"}
            />

            {status === "succeeded" &&
                <React.Fragment>

                    <SelectParameters/>
                    <SelectInputs/>
                    <ValidateDatasets/>
                    <ValidateInputs/>
                    <ValidateInputSchemas/>
                    <GotoReview/>

                </React.Fragment>
            }
        </React.Fragment>
    )
};