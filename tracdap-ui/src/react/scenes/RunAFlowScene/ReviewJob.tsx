/**
 * A scene page allows the user to select a flow that they want to run and select the
 * models and the datasets to use. When the user has selected all the options they
 * can then view a summary report before executing the model.
 *
 * @module ReviewJob
 * @category RunAFlowScene page
 */

import {Button} from "../../components/Button";
import {Icon} from "../../components/Icon";
import {objectsContainsValue} from "../../utils/utils_object";
import React from "react";
import {ReviewFlow} from "../../components/ReviewFlow";
import {ReviewDatasets} from "../../components/ReviewDatasets";
import {ReviewModels} from "../../components/ReviewModels";
import {ReviewParameters} from "../../components/ReviewParameters";
import {runJob, setUpJob} from "./store/runAFlowStore";
import {SceneTitle} from "../../components/SceneTitle";
import {SetAttributes} from "../../components/SetAttributes/SetAttributes";
import {setValidationChecked} from "../../components/SetAttributes/setAttributesStore";
import {showToast} from "../../utils/utils_general";
import {useAppDispatch, useAppSelector} from "../../../types/types_hooks";
import {Link} from "react-router-dom";

export const ReviewJob = () => {

    // Get what we need from the store
    const {isValid: isValidAttributes} = useAppSelector(state => state["setAttributesStore"].uses.runAFlow.attributes.validation)

    const {flowMetadata} = useAppSelector(state => state["runAFlowStore"].flow)

    const {jobInputs, jobModels, jobParameters} = useAppSelector(state => state["runAFlowStore"].rerun)

    const {
        values: parameterValues,
        parameterDefinitions
    } = useAppSelector(state => state["runAFlowStore"].parameters)

    const {inputDefinitions, selectedInputOptions} = useAppSelector(state => state["runAFlowStore"].inputs)

    const {selectedModelOptions} = useAppSelector(state => state["runAFlowStore"].models)

    const {change: overlaysByKey} = useAppSelector(state => state["overlayBuilderStore"].uses.runAFlow)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A function that checks whether the attributes meet their validation rules before initiating a flow execution in TRAC.
     */
    const handleRun = (): void => {

        if (objectsContainsValue(isValidAttributes, false)) {

            dispatch(setValidationChecked({storeKey: "runAFlow", value: true}))
            showToast("error", "There are problems with some of your attributes, please fix the issues shown and try again.", "not-valid")

        } else {

            dispatch(setValidationChecked({storeKey: "runAFlow", value: false}))
            dispatch(runJob())
        }
    }

    return (
        <React.Fragment>

            <SceneTitle text={"Review job"}>
                <Button ariaLabel={"Setup job"}
                        className={"min-width-px-150 float-end"}
                        isDispatched={true}
                        onClick={setUpJob}
                >
                    <Icon ariaLabel={"Setup job"}
                          className={"me-2"}
                          icon={"bi-chevron-left"}
                    />Set up
                </Button>
            </SceneTitle>

            <ReviewFlow storeKey={"runAFlow"}
                        tag={flowMetadata}
            />

            <ReviewParameters definitions={parameterDefinitions}
                              policyValues={jobParameters}
                              storeKey={"runAFlow"}
                              values={parameterValues}
            />

            <ReviewModels models={selectedModelOptions}
                          overlays={overlaysByKey}
                          policyValues={jobModels}
                          showOnOpen={true}
                          storeKey={"runAFlow"}
            />

            <ReviewDatasets datasets={selectedInputOptions}
                            definitions={inputDefinitions}
                            overlays={overlaysByKey}
                            policyValues={jobInputs}
                            showOnOpen={true}
                            storeKey={"runAFlow"}
                            type={"inputs"}
            />

            <SetAttributes show={true}
                           storeKey={"runAFlow"}
                           title={`Set job attributes`}
            >
                When running a job it is tagged with metadata or attributes that help users understand
                what the run was for. The editor below allows you to set this metadata. If you make a mistake or
                want to change the information about an item in TRAC then the <Link to={"/admin/update-tags"}>Tag Editor</Link>{" "}
                can be used to update the tags.
            </SetAttributes>

            <Button ariaLabel={"Run job"}
                    className={"min-width-px-150 float-end"}
                    isDispatched={false}
                    onClick={handleRun}
            >
                Run<Icon ariaLabel={false}
                         className={"ms-2"}
                         icon={"bi-caret-right"}
            />
            </Button>

        </React.Fragment>
    )
};