/**
 * A component that shows an alert if there are models in the model chain that do not have any options.
 * @module ValidateModels
 * @category RunAFlowScene component
 */

import {Alert} from "../../../components/Alert";
import React from "react";
import {useAppSelector} from "../../../../types/types_hooks";

// The header message before the bullets
const headerMessage = "The job can not be run because there are models in the model chain that do not have any options."

export const ValidateModels = () => {

    // Get what we need from the store
    const {modelOptionsExistForAll} = useAppSelector(state => state["runAFlowStore"].validation)

    return (

        <React.Fragment>

            {!modelOptionsExistForAll &&
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