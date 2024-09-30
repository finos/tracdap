/**
 * A component that shows a loading icon for the {@link RunAFlowScene}. This is slightly more complex that in normal
 * cases as there are multiple interdependent API calls. It shows in two cases, first when the list of flows is being
 * loaded, in which case a rotating icon is shown and second when the flow is being built, in which case a progress
 * bar is shown because there are several calls required.
 * @module Loading
 * @category RunAFlowScene component
 */

import {Loading as LoadingIcon} from "../../../components/Loading";
import {ProgressBar} from "../../../components/ProgressBar";
import React from "react";
import {useAppSelector} from "../../../../types/types_hooks";

export const Loading = () => {

    // Get what we need from the stores
    const {flows : {status: flowsStatus}, flow: {status: flowStatus, numberOfApiCalls: {toDo, completed}}} = useAppSelector(state => state["runAFlowStore"])

    return (

        <React.Fragment>

            {flowsStatus === "pending"  &&
                <LoadingIcon text={"Please wait"}/>
            }

            {flowStatus === "pending" &&
                <ProgressBar completed={completed} toDo={toDo} text={ "Please wait ... building flow options"}/>
            }

        </React.Fragment>
    )
};