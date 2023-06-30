/**
 * A component that allows the user to select the parameter values for the models in a flow.
 * @module SelectParameters
 * @category RunAFlowScene component
 */

import {HeaderTitle} from "../../../components/HeaderTitle";
import {ParameterMenu} from "../../../components/ParameterMenu/ParameterMenu";
import React from "react";
import {setParameter, setShowKeys} from "../store/runAFlowStore";
import {Toolbar} from "./Toolbar";
import {useAppSelector} from "../../../../types/types_hooks";
import {ValidateParameters} from "./ValidateParameters";

export const SelectParameters = () => {

    console.log("Rendering SelectParameters")

    // Get what we need from the store
    const {
        lastParameterChanged,
        parameterDefinitions,
        showKeysInsteadOfLabels,
        values
    } = useAppSelector(state => state["runAFlowStore"].parameters)

    const {jobParameters} = useAppSelector(state => state["runAFlowStore"].rerun)

    // The parameters need to be frozen if fetching a list of the models as they may be cleared or updated
    // if the current choice isn't in the new results
    const status = useAppSelector(state => state["runAFlowStore"].models.status)

    const validationChecked = useAppSelector(state => state["runAFlowStore"].validation.validationChecked)

    return (

        <React.Fragment>

            {Object.keys(parameterDefinitions).length > 0 &&

                <HeaderTitle type={"h3"} text={`Set parameters`}>
                    <Toolbar disabled={false}
                             name={"parameters"}
                             onShowKeys={setShowKeys}
                             showKeysInsteadOfLabels={showKeysInsteadOfLabels}
                    />
                </HeaderTitle>
            }

            <ValidateParameters/>

            <ParameterMenu isDisabled={Boolean(status === "pending")}
                           lastParameterChanged={lastParameterChanged}
                           label={"parameters"}
                           onChange={setParameter}
                           parameters={parameterDefinitions}
                           policyValues={jobParameters}
                           showKeysInsteadOfLabels={showKeysInsteadOfLabels}
                           validationChecked={validationChecked}
                           values={values}
            />
        </React.Fragment>
    )
};