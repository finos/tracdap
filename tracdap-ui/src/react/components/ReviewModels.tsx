/**
 * A component that shows summary information about the selected models for a flow or a job.
 *
 * @module ReviewModels
 * @category Component
 */

import {HeaderTitle} from "./HeaderTitle";
import {ModelListTable} from "./ModelListTable";
import type {Overlay} from "./OverlayBuilder/overlayBuilderStore";
import React, {useMemo} from "react";
import type {SearchOption} from "../../types/types_general";
import {ShowHideDetails} from "./ShowHideDetails";
import type {SingleValue} from "react-select";
import {TextBlock} from "./TextBlock";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the props of the ReviewParameters component.
 */
export interface Props {

    /**
     * The overlays configured to be applied to the datasets in a flow or that were run as
     * part of a job. The key here is the dataset key.
     */
    overlays?: Record<string, Overlay>
    /**
     * The policy values that the models should have, these are value from a
     * select component.
     */
    policyValues?: Record<string, SingleValue<SearchOption>>
    /**
     * The metadata for the models selected.
     */
    models: Record<string, trac.metadata.ITag> | Record<string, SingleValue<SearchOption>>
    /**
     * Whether the section should be expanded by default.
     */
    showOnOpen: boolean
    /**
     * What scene is using this component, this affects the language.
     */
    storeKey: "runAFlow" | "jobViewerRunFlow"
}

export const ReviewModels = (props: Props) => {

    const {models, overlays, policyValues, showOnOpen, storeKey} = props

    /**
     * A memoized value for the number of overlays applied by model.
     */
    const overlayCount = useMemo(() => {

        let overlayCount: Record<string, number> = {}

        models && Object.entries(models).forEach(([key, model]) => {

            // The keys of the output datasets for the model
            const modelOutputKeys = model?.tag?.definition?.model?.outputs ? Object.keys(model.tag.definition.model.outputs) : []

            overlayCount[key] = 0
            modelOutputKeys.forEach(outputKey => {
                if (overlays && Object.keys(overlays).includes(outputKey)) {
                    overlayCount[key] = overlayCount[key] + overlays[outputKey].changeTab.length
                }
            })
        })

        return overlayCount

    }, [overlays, models])

    return (
        <React.Fragment>
            <HeaderTitle type={"h3"} text={"Model summary"}/>

            <TextBlock>
                {storeKey === "runAFlow" ? "View information about the models that were used for this job." : "View information about the models selected for this job."}
            </TextBlock>

            <ShowHideDetails classNameOuter={"mt-0 pb-4"}
                             classNameInner={"mt-4 mt-md-0 mb-0"}
                             linkText={"model details"}
                             showOnOpen={showOnOpen}
            >
                <ModelListTable models={models}
                                overlayCount={overlayCount}
                                policyValues={policyValues}
                />
            </ShowHideDetails>
        </React.Fragment>
    )
};