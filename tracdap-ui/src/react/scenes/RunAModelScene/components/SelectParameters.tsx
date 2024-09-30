/**
 * A component that allows the user to select the parameter values for the model to run.
 * @module SelectParameters
 * @category RunAModelScene component
 */

import {HeaderTitle} from "../../../components/HeaderTitle";
import {ParameterMenu} from "../../../components/ParameterMenu/ParameterMenu";
import React from "react";
import {setParameter, setShowKeys} from "../store/runAModelStore";
import {Toolbar} from "./Toolbar";
import {useAppSelector} from "../../../../types/types_hooks";
// TODO
// import {ValidateParameters} from "./ValidateParameters";

export const SelectParameters = () => {

    console.log("Rendering SelectParameters")

    // Get what we need from the store
    const {
        lastParameterChanged,
        parameterDefinitions,
        showKeysInsteadOfLabels,
        values
    } = useAppSelector(state => state["runAModelStore"].parameters)

    const {jobParameters} = useAppSelector(state => state["runAModelStore"].rerun)

    const validationChecked = useAppSelector(state => state["runAModelStore"].validation.validationChecked)

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

            {/*<ValidateParameters/>*/}

            <ParameterMenu isDisabled={false}
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