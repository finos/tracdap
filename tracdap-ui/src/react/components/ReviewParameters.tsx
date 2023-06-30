/**
 * A component that shows summary information about the parameter values. This is used by the
 * {@link ObjectSummaryScene}, {@link RunAFlowScene} and the {@link RunAModelScene}  scenes.
 *
 * @module ReviewParameters
 * @category Component
 */

import {createTag} from "../utils/utils_attributes_and_parameters";
import {HeaderTitle} from "./HeaderTitle";
import {isValue} from "../utils/utils_trac_type_chckers";
import type {Option, SelectPayload} from "../../types/types_general";
import {ParameterTable} from "./ParameterTable";
import React from "react";
import {ShowHideDetails} from "./ShowHideDetails";
import {TextBlock} from "./TextBlock";
import {tracdap as trac} from "@finos/tracdap-web-api";
import type {UiAttributesProps} from "../../types/types_attributes_and_parameters";

/**
 * An interface for the props of the ReviewParameters component.
 */
export interface Props {

    /**
     * The parameter definitions, these are passed from the {@link runAFlowStore} or
     * {@link runAModelStore}. It contains the basic type of the parameter as well as the label etc.
     */
    definitions?: Record<string, UiAttributesProps>
    /**
     * The policy values that the parameters should have, these are value from a
     * select component.
     */
    policyValues?: Record<string, SelectPayload<Option, boolean>["value"]>
    /**
     * What scene is using this component, this affects the messages.
     */
    storeKey: "runAFlow" | "runAModel" | "jobViewerRunFlow" | "jobViewerRunModel"
    /**
     * The parameter values, these are either passed from the {@link runAFlowStore} or
     * {@link runAModelStore} (where it contains the value set by the user from a
     * select component) or from a job metadata tag (where it contains the values as trac.IValues).
     */
    values: Record<string, SelectPayload<Option, boolean>["value"]> | Record<string, trac.metadata.IValue> | null
}

export const ReviewParameters = (props: Props) => {

    const {definitions, policyValues, storeKey, values} = props

    const parameters: Record<string, trac.metadata.IValue & { label?: string }> = {}

    // Convert the parameters in the UI into a TRAC value object, matching what we get back from the API for a job definition
    // Meaning we can have a single component for both user cases. We handle both use cases here which as the 'values' prop
    // has different types.
    values != null && Object.entries(values).forEach(([key, value]) => {

        const definition = definitions?.[key]

        // If we are passed a set of TRAC IValues then we can simply pass those through and get the
        // label from the definitions if it has been passed as a prop
        if (isValue(value)) {

            parameters[key] = value
            parameters[key].label = definition?.name ?? undefined

        } else {

            // If we 'values' prop is a set of select component values we need to convert to
            // a TRAC IValue
            const tag = createTag(key, definition?.basicType ?? trac.BasicType.BASIC_TYPE_NOT_SET, value)

            if (tag.value) {
                parameters[key] = tag.value
                parameters[key].label = definition?.name ?? undefined
            }
        }

    })

    /**
     * A function that sets the message to show, this depends on where this component is being used.
     * @param storeKey - What scene is using this component, this affects the language.
     */
    const setText = (storeKey: Props["storeKey"]) => {

        if (storeKey == "runAFlow" || storeKey === "runAModel") {
            return "The parameters have the following values set."
        } else if (storeKey == "jobViewerRunFlow") {
            return "The parameters used by the models had the following values."
        } else {
            return "The parameters used by the model had the following values."
        }
    }

    return (
        <React.Fragment>
            <HeaderTitle type={"h3"} text={"Parameter summary"}/>

            {Object.keys(parameters).length === 0 &&
                <TextBlock>
                    There are no parameters.
                </TextBlock>
            }

            {Object.keys(parameters).length > 0 &&
                <React.Fragment>
                    <TextBlock>
                        {setText(storeKey)}
                    </TextBlock>

                    <ShowHideDetails classNameOuter={"mt-0"} linkText={"parameter details"} showOnOpen={true}>
                        <ParameterTable params={parameters}
                                        policyValues={policyValues}
                        />
                    </ShowHideDetails>
                </React.Fragment>
            }
        </React.Fragment>
    )
};