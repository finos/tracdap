/**
 * A component that shows a loading icon for the {@link RunAModelScene}. This is slightly more complex that in normal
 * cases as there are multiple interdependent API calls. It shows in two cases, first when the list of models is being
 * loaded, in which case a rotating icon is shown and second when the model is being built, in which case a progress
 * bar is shown because there are several calls required.
 * @module Loading
 * @category RunAModelScene component
 */
import {Loading as LoadingIcon} from "../../../components/Loading";
import {ProgressBar} from "../../../components/ProgressBar";
import React from "react";
import {useAppSelector} from "../../../../types/types_hooks";

export const Loading = () => {

    // Get what we need from the stores
    const {models : {status: modelsStatus}, model: {status: modelStatus, numberOfApiCalls: {toDo, completed}}} = useAppSelector(state => state["runAModelStore"])

    return (

        <React.Fragment>

            {modelsStatus === "pending"  &&
                <LoadingIcon text={"Please wait"}/>
            }

            {modelStatus === "pending" &&
                <ProgressBar toDo={toDo} completed={completed} text={ "Please wait ... building model options"}/>
            }

        </React.Fragment>
    )
};