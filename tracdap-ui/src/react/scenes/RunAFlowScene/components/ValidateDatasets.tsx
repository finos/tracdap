/**
 * A component that shows an alert if the flow definition for the model inputs and outputs do not match
 * the definitions in the models selected.
 * @module ValidateDatasets
 * @category RunAFlowScene component
 */

import {Alert} from "../../../components/Alert";
import {modelsDoNotMatchFlowModelInputsAndOutputs} from "../../../utils/utils_flows";
import React from "react";
import {useAppSelector} from "../../../../types/types_hooks";
import {ValidationOfModelsAndFlowData} from "../../../../types/types_general";

// The header message before the bullets
const headerMessage = "The job can not be run because the selected models do not have input and output datasets that match the those defined in the flow:"

/**
 * A function that generates the messages to show in the alert if there is an issue.
 * @param modelsMatchFlowModelInputsAndOutputs - The validation object from the store.
 */
function calculateMessages(modelsMatchFlowModelInputsAndOutputs: ValidationOfModelsAndFlowData): string[] {

    const messages: string[] = [headerMessage]

    Object.entries(modelsMatchFlowModelInputsAndOutputs).forEach(([modelKey, validation]) => {

        validation.flow.missingInputs.forEach(missing => {
            messages.push(`The flow defines model '${validation.modelName || modelKey}' ${validation.modelName ? '(' + modelKey + ')' : ""} as having an input called '${missing}' but this is missing in the selected model.`)
        })

        validation.flow.missingOutputs.forEach(missing => {
            messages.push(`The flow defines model '${validation.modelName || modelKey}' ${validation.modelName ? '(' + modelKey + ')' : ""} as having an output called '${missing}' but this is missing in the selected model.`)
        })

        validation.models.missingInputs.forEach(missing => {
            messages.push(`The model '${validation.modelName || modelKey}' ${validation.modelName ? '(' + modelKey + ')' : ""} has an input called '${missing}' but this is not defined in the flow.`)
        })

        validation.models.missingOutputs.forEach(missing => {
            messages.push(`The model '${validation.modelName || modelKey}' ${validation.modelName ? '(' + modelKey + ')' : ""} has an output called '${missing}' but this is not defined in the flow.`)
        })
    })

    return messages
}

export const ValidateDatasets = () => {

    // Get what we need from the store
    const {modelsMatchFlowModelInputsAndOutputs} = useAppSelector(state => state["runAFlowStore"].validation)

    // Should we show an alert
    const hasError = modelsDoNotMatchFlowModelInputsAndOutputs(modelsMatchFlowModelInputsAndOutputs)

    // Generate the messages to show
    const messages: string[] = hasError ? calculateMessages(modelsMatchFlowModelInputsAndOutputs) : []

    return (

        <React.Fragment>
            {hasError &&
                <Alert className={"mt-2 mb-4"}
                       listHasHeader={true}
                       variant={"danger"}
                >
                    {messages}
                </Alert>
            }
        </React.Fragment>
    )
};