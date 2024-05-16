/**
 * A component that shows summary information about the selected input datasets for a flow or
 * input/output datasets for a job.
 *
 * @module ReviewDatasets
 * @category Component
 */

import {DatasetSelector} from "./DatasetSelector";
import {getOptionalInputsFromCategories, getRequiredInputsFromCategories} from "../utils/utils_flows";
import {HeaderTitle} from "./HeaderTitle";
import {JobDatasetListTable} from "./JobDatasetListTable";
import React, {useMemo} from "react";
import {ShowHideDetails} from "./ShowHideDetails";
import Tab from "react-bootstrap/Tab";
import Tabs from "react-bootstrap/Tabs";
import {TextBlock} from "./TextBlock";
import type {Overlay} from "./OverlayBuilder/overlayBuilderStore";
import type {SingleValue} from "react-select";
import type {SearchOption} from "../../types/types_general";
import {tracdap as trac} from "@finos/tracdap-web-api";
import type {UiAttributesProps} from "../../types/types_attributes_and_parameters";

/**
 * An interface for the props of the ReviewParameters component.
 */
export interface Props {

    /**
     * The metadata for the datasets selected.
     */
    datasets: Record<string, trac.metadata.ITag> | Record<string, SingleValue<SearchOption>>
    /**
     * The dataset definitions, these are passed from the {@link runAFlowStore} or
     * {@link runAModelStore}.
     */
    definitions?: Record<string, UiAttributesProps>
    /**
     * The overlays configured to be applied to the datasets in a flow or that were run as
     * part of a job. The key here is the dataset key.
     */
    overlays?: Record<string, Overlay>
    /**
     * The policy values that the datasets should have, these are value from a
     * select component, should only be used when reviewing input datasets.
     */
    policyValues?: Record<string, SingleValue<SearchOption>>
    /**
     * Whether the section should be expanded by default.
     */
    showOnOpen: boolean
    /**
     * What scene is using this component, this affects the language.
     */
    storeKey: "runAFlow" | "runAModel" | "jobViewerRunFlow" | "jobViewerRunModel"
    /**
     * Whether the type of datasets being reviewed are inputs or outputs from a job.
     */
    type: "inputs" | "outputs"
}

export const ReviewDatasets = (props: Props) => {

    const {datasets, definitions, overlays, policyValues, showOnOpen, storeKey, type} = props

    // Which inputs are optional and which required
    const optionalInputKeys = getOptionalInputsFromCategories(definitions ?? {})
    const requiredInputKeys = getRequiredInputsFromCategories(definitions ?? {})

    /**
     * A memoized value for the number of overlays applied by input dataset key.
     */
    const overlayCount = useMemo(() => {

        let overlayCount: Record<string, number> = {}

        Object.keys(datasets).forEach(key => {
            overlayCount[key] = overlays?.[key]?.changeTab?.length ?? 0
        })

        return overlayCount

    }, [overlays, datasets])

    const useTabs = type === "inputs" && requiredInputKeys.length > 0 && optionalInputKeys.length > 0

    /**
     * A function that sets the message to show, this depends on where this component is being used and
     * on what type of datasets.
     * @param storeKey - What scene is using this component, this affects the language.
     * @param type - Whether the type of datasets being reviewed are inputs or outputs from a job.
     */
    const setText = (storeKey: Props["storeKey"], type: Props["type"]) => {

        if (storeKey == "runAFlow" || storeKey === "runAModel") {
            return "View information about each of the input datasets selected for the job."
        } else if (type === "inputs") {
            return "View information about each of the input datasets that were used for this job."
        } else {
            return "View information about each of the output datasets."
        }
    }

    return (
        <React.Fragment>
            <HeaderTitle type={"h3"} text={`${type === "inputs" ? "Input" : "Output"} data summary`}/>

            <TextBlock>
                {setText(storeKey, type)}
            </TextBlock>

            <ShowHideDetails classNameOuter={"mt-0 pb-4"}
                             classNameInner={"mt-4 mt-md-0 mb-0"}
                             linkText={`${type === "inputs" ? "input" : "output"} dataset details`}
                             showOnOpen={showOnOpen}
            >

                {/*If we have required and optional inputs then break these down into two tabs, tabs are not used for outputs*/}
                {/*TODO we could have a tab per category*/}
                {useTabs &&
                    <Tabs className={"mt-4"}
                          defaultActiveKey={"required"}
                          id={`${type === "inputs" ? "input-datasets" : "output-datasets"}`}
                    >
                        <Tab eventKey={"required"} title={`Required (${requiredInputKeys.length})`}
                             className={"tab-content-bordered pt-4 pb-2"}>
                            <JobDatasetListTable inputsForCategory={requiredInputKeys}
                                                 datasets={datasets}
                                                 overlayCount={overlayCount}
                                                 policyValues={policyValues}
                            />
                        </Tab>

                        <Tab eventKey={"optional"} title={`Optional (${optionalInputKeys.length})`}
                             className={"tab-content-bordered pt-4 pb-2"}>
                            <JobDatasetListTable inputsForCategory={optionalInputKeys}
                                                 datasets={datasets}
                                                 overlayCount={overlayCount}
                                                 policyValues={policyValues}
                            />
                        </Tab>
                    </Tabs>
                }

                {!useTabs &&
                    <JobDatasetListTable datasets={datasets}
                                         overlayCount={overlayCount}
                                         policyValues={policyValues}
                    />
                }

                {(Object.keys(datasets).length > 0) &&
                    <React.Fragment>
                        <HeaderTitle outerClassName={`mt-5 mb-3`}
                                     text={`View the ${type === "inputs" ? "input datasets set by the user" : "output datasets created"} `}
                                     type={"h3"}
                        />

                        <TextBlock>
                            Use the drop down to see each of the datasets.
                        </TextBlock>

                        <DatasetSelector datasets={datasets}/>
                    </React.Fragment>
                }

            </ShowHideDetails>
        </React.Fragment>
    )
};