/**
 * A component that shows an alert if the parameters have a validation error.
 * @module ValidateParameters
 * @category RunAFlowScene component
 */

import {Alert} from "../../../components/Alert";
import {commasAndAnds} from "../../../utils/utils_general";
import React from "react";
import {useAppSelector} from "../../../../types/types_hooks";
import {parametersHaveMultipleBasicTypes} from "../../../utils/utils_flows";
import {convertBasicTypeToString} from "../../../utils/utils_trac_metadata";

// The header message before the bullets
const headerMessage = "The job can not be run because the selected models share parameters that have different types:"

export const ValidateParameters = () => {

    // Get what we need from the store
    const {parametersWithMultipleBasicTypes} = useAppSelector(state => state["runAFlowStore"].validation)

    const messages = [headerMessage].concat(parametersWithMultipleBasicTypes.map(error => `Parameter '${error.label || error.key}' ${error.label ? '(' + error.key + ')' : ""} is defined as ${commasAndAnds(error.types.map(type => convertBasicTypeToString(type, true)))}`))

    return (

        <React.Fragment>

            {parametersHaveMultipleBasicTypes(parametersWithMultipleBasicTypes) &&
                <Alert className={"mt-3 mb-4"}
                       listHasHeader={true}
                       variant={"danger"}
                >
                    {messages}
                </Alert>
            }
        </React.Fragment>
    )
};