/**
 * This is a wrapper for the BusinessSegmentsMenu component, this is needed as an optimisation of the {@link RunAFlowScene}
 * rendering, by placing it in its own wrapper it does not cause the entire scene to rerender when the values it
 * uses from the store are changed.
 * @module BusinessSegments
 * @category RunAFlowScene component
 */

import {BusinessSegmentMenu} from "../../../components/BusinessSegments/BusinessSegmentMenu";
import {BusinessSegmentsStoreState} from "../../../components/BusinessSegments/businessSegmentsStore";
import {getFlows} from "../store/runAFlowStore";
import React from "react";
import {useAppSelector} from "../../../../types/types_hooks";
import {TextBlock} from "../../../components/TextBlock";

/**
 * An interface for the props of the BusinessSegments component.
 */
export interface Props {

    /**
     * The key in the BusinessSegmentsStore to get the state for this component.
     */
    storeKey: keyof BusinessSegmentsStoreState["uses"]
}

export const BusinessSegments = (props: Props) => {

    console.log("Rendering BusinessSegments")

    const {storeKey} = props;

    // Get what we need from the store in this case we need to tell the business segment component to go
    // disabled when an API call is being made that uses the values (otherwise the user can change them
    // and initiate another call
    const {job: rerunJob} = useAppSelector(state => state["runAFlowStore"].rerun)
    const policiesStatus = useAppSelector(state => state["runAFlowStore"].policies.status)
    const policyStatus = useAppSelector(state => state["runAFlowStore"].policy.status)
    const flowsStatus = useAppSelector(state => state["runAFlowStore"].flows.status)
    const flowStatus = useAppSelector(state => state["runAFlowStore"].flow.status)
    const {"trac-tenant": tenant} = useAppSelector(state => state["applicationStore"].cookies)

    return (

        <React.Fragment>
            <TextBlock>
                First use the menu below to select the type of flow that you want to run.
            </TextBlock>

            {/* The onChange logic below makes the component not do a search for flows when it mounts */}
            {/* when we are trying to rerun a job or when an existing API call is running. tenant needs */}
            {/* to be set or the component will loop indefinitely */}
            <BusinessSegmentMenu disabled={Boolean(rerunJob || flowStatus === "pending" || flowsStatus === "pending" || policyStatus === "pending" || policiesStatus === "pending")}
                                 onChange={!rerunJob && tenant !== undefined ? getFlows : undefined}
                                 storeKey={storeKey}
            />
        </React.Fragment>
    )
};