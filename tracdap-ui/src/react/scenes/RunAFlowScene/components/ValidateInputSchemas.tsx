/**
 * A component that shows an alert if the selected input datasets have schemas that do not match those defined in the model,
 * this checks the name and type of the schemas.
 * @module ValidateInputSchemas
 * @category RunAFlowScene component
 */

import {Alert} from "../../../components/Alert";
import {convertBasicTypeToString} from "../../../utils/utils_trac_metadata";
import {inputSchemasDoNotMatchModelInputs} from "../../../utils/utils_flows";
import React from "react";
import {useAppSelector} from "../../../../types/types_hooks";
import {ValidationOfModelInputs} from "../../../../types/types_general";

// The header message before the bullets
const headerMessage = "The job can not be run because the selected inputs do not have schemas that match those defined in the models that use them:"

/**
 * A function that generates the messages to show in the alert if there is an issue.
 * @param flowInputSchemasNotMatchingModelInputs - The validation object from the store.
 */
function calculateMessages(flowInputSchemasNotMatchingModelInputs: ValidationOfModelInputs): string[] {

    const messages: string[] = [headerMessage]

    Object.entries(flowInputSchemasNotMatchingModelInputs).forEach(([inputKey, validation]) => {

        validation.missing.forEach(missingField => {
            messages.push(`The input dataset '${inputKey}' does not have a field called '${missingField}', this is required by model '${validation.modelKey}'.`)
        })

        validation.type.forEach(badTypeField => {
            messages.push(`The input dataset '${inputKey}' has the wrong type for field '${badTypeField.fieldName}', it should be a ${convertBasicTypeToString(badTypeField.want)}, but a ${convertBasicTypeToString(badTypeField.have)} was found, this is required by model '${validation.modelKey}'.`)
        })
    })

    return messages
}

export const ValidateInputSchemas = () => {

    // Get what we need from the store
    const {flowInputSchemasNotMatchingModelInputs} = useAppSelector(state => state["runAFlowStore"].validation)

    // Should we show an alert
    const hasError = inputSchemasDoNotMatchModelInputs(flowInputSchemasNotMatchingModelInputs)

    // Generate the messages to show
    const messages: string[] = hasError ? calculateMessages(flowInputSchemasNotMatchingModelInputs) : []

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