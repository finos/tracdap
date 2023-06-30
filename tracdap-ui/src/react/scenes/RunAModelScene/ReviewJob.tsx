/**
 * A scene page allows the user to select a model that they want to run and select
 * the datasets to use. When the user has selected all the options they
 * can then view a summary report before executing the model.
 * @module ReviewJob
 * @category RunAModelScene page
 */

import {Button} from "../../components/Button";
import {Icon} from "../../components/Icon";
import {Link} from "react-router-dom";
import {objectsContainsValue} from "../../utils/utils_object";
import React from "react";
import {ReviewParameters} from "../../components/ReviewParameters";
import {ReviewModel} from "../../components/ReviewModel";
import {ReviewDatasets} from "../../components/ReviewDatasets";
import {runJob, setUpJob} from "./store/runAModelStore";
import {SceneTitle} from "../../components/SceneTitle";
import {SetAttributes} from "../../components/SetAttributes/SetAttributes";
import {setValidationChecked} from "../../components/SetAttributes/setAttributesStore";
import {showToast} from "../../utils/utils_general";
import {useAppDispatch, useAppSelector} from "../../../types/types_hooks";

export const ReviewJob = () => {

    // Get what we need from the store
    const {isValid: isValidAttributes} = useAppSelector(state => state["setAttributesStore"].uses.runAModel.attributes.validation)

    const {modelMetadata} = useAppSelector(state => state["runAModelStore"].model)

    const {jobInputs, jobParameters} = useAppSelector(state => state["runAModelStore"].rerun)

    const {
        values: parameterValues,
        parameterDefinitions
    } = useAppSelector(state => state["runAModelStore"].parameters)

    const {inputDefinitions, selectedInputOptions} = useAppSelector(state => state["runAModelStore"].inputs)

    /**
     * A function that checks whether the attributes meet their validation rules before initiating a model execution in TRAC.
     */
    const handleRun = (): void => {

        if (objectsContainsValue(isValidAttributes, false)) {

            dispatch(setValidationChecked({storeKey: "runAModel", value: true}))
            showToast("error", "There are problems with some of your attributes, please fix the issues shown and try again.", "not-valid")

        } else {

            dispatch(setValidationChecked({storeKey: "runAModel", value: false}))
            dispatch(runJob())
        }
    }

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

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

            <ReviewModel tag={modelMetadata}/>

            <ReviewParameters definitions={parameterDefinitions}
                              policyValues={jobParameters}
                              storeKey={"runAModel"}
                              values={parameterValues}
            />

            <ReviewDatasets datasets={selectedInputOptions}
                            definitions={inputDefinitions}
                            policyValues={jobInputs}
                            showOnOpen={true}
                            storeKey={"runAModel"}
                            type={"inputs"}
            />

            <SetAttributes show={true}
                           storeKey={"runAModel"}
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