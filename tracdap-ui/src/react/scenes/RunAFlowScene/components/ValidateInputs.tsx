/**
 * A component that shows an alert if there are non-optional inputs in the model chain that do not have any options.
 * @module ValidateInputs
 * @category RunAFlowScene component
 */

import {Alert} from "../../../components/Alert";
import React from "react";
import {useAppSelector} from "../../../../types/types_hooks";

// The header message before the bullets
const headerMessage = "The job can not be run because there are required inputs in the model chain that do not have any options."

export const ValidateInputs = () => {

    // Get what we need from the store
    const {nonOptionalInputOptionsExistForAll} = useAppSelector(state => state["runAFlowStore"].validation)

    return (

        <React.Fragment>

            {!nonOptionalInputOptionsExistForAll &&
                <Alert className={"mt-2 mb-2"}
                       listHasHeader={true}
                       variant={"danger"}
                >
                    {headerMessage}
                </Alert>
            }
        </React.Fragment>
    )
};