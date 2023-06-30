/**
 * A component that allows the user to select the input dataset options for the model to run.
 * @module SelectInputs
 * @category RunAModelScene component
 */

import {addInputOption, getInputs, setInput, setListsOrTabs, setShowKeys, updateOptionLabels} from "../store/runAModelStore";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {ParameterMenu} from "../../../components/ParameterMenu/ParameterMenu";
import React, {useMemo} from "react";
import {setParametersByCategory} from "../../../utils/utils_attributes_and_parameters";
import {Toolbar} from "./Toolbar";
import {tracdap as trac} from "@finos/tracdap-web-api"
import {useAppSelector} from "../../../../types/types_hooks";

export const SelectInputs = () => {

    console.log("Rendering SelectInputs")

    // Get what we need from the store
    const {
        inputDefinitions,
        lastInputChanged,
        listsOrTabs,
        selectedInputOptions,
        status,
        showKeysInsteadOfLabels
    } = useAppSelector(state => state["runAModelStore"].inputs)

    const validationChecked = useAppSelector(state => state["runAModelStore"].validation.validationChecked)

    const {
        showCreatedDate,
        showObjectId,
        showUpdatedDate,
        showVersions
    } = useAppSelector(state => state["runAModelStore"].optionLabels.inputs)

    const {jobInputs} = useAppSelector(state => state["runAModelStore"].rerun)

    // An object that contains a list of which parameters are in which categories
    const parametersByCategory = useMemo(() => setParametersByCategory(inputDefinitions), [inputDefinitions])

    return (

        <React.Fragment>

            {Object.keys(inputDefinitions).length > 1 &&

                <HeaderTitle type={"h3"} text={`Select inputs`}>
                    <Toolbar disabled={Boolean(status === "pending")}
                             listsOrTabs={listsOrTabs}
                             name={"inputs"}
                             onRefresh={getInputs}
                             onChangeLabel={updateOptionLabels}
                             onShowKeys={setShowKeys}
                        // Only allow swapping to a tab view if there are more than one category
                             onShowTabs={Object.keys(parametersByCategory).length > 1 ? setListsOrTabs : undefined}
                             showObjectId={showObjectId}
                             showVersions={showVersions}
                             showCreatedDate={showCreatedDate}
                             showUpdatedDate={showUpdatedDate}
                             showKeysInsteadOfLabels={showKeysInsteadOfLabels}
                    />
                </HeaderTitle>
            }

            <ParameterMenu baseWidth={6}
                           className={"pt-3 mb-4"}
                           hideDisabledOptions={true}
                           isDisabled={Boolean(status === "pending")}
                           lastParameterChanged={lastInputChanged}
                           label={""}
                           listsOrTabs={listsOrTabs}
                           onChange={setInput}
                           policyValues={jobInputs}
                           parameters={inputDefinitions}
                           selectOptionObjectType={trac.ObjectType.DATA}
                           selectOptionOnCreateNewOption={addInputOption}
                           showKeysInsteadOfLabels={showKeysInsteadOfLabels}
                           validationChecked={validationChecked}
                           values={selectedInputOptions}
            />

        </React.Fragment>
    )
};