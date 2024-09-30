/**
 * A component that shows a button that moves from setting up the job to the review.
 * @module GotoReview
 * @category RunAModelScene component
 */

import {Button} from "../../../components/Button";
import {commasAndAnds, showToast} from "../../../utils/utils_general";
import {Icon} from "../../../components/Icon";
import {isDefined} from "../../../utils/utils_trac_type_chckers";
import {objectsContainsValue} from "../../../utils/utils_object";
import React from "react";
import {reviewJob, setValidationChecked} from "../store/runAModelStore";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

export const GotoReview = () => {

    // Get what we need from the store
    const canRun = useAppSelector(state => state["runAModelStore"].validation.canRun)
    const {isValid: isValidSettings} = useAppSelector(state => state["runAModelStore"].validation)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A function that checks whether the job selections meet their validation rules before initiating moving to the review page.
     */
    const handleReview = (): void => {

        const invalidParameters = objectsContainsValue(isValidSettings.parameters, false)
        const invalidInputs = objectsContainsValue(isValidSettings.inputs, false)

        if (invalidParameters || invalidInputs) {

            const messageString = commasAndAnds([invalidParameters ? "parameters" : undefined, invalidInputs ? "input datasets" : undefined].filter(isDefined))

            dispatch(setValidationChecked({value: true}))
            showToast("error", `There are problems with some of your ${messageString}, please fix the issues shown and try again.`, "not-valid")

        } else {

            dispatch(setValidationChecked({value: false}))
            dispatch(reviewJob())
        }
    }

    return (

        <Button ariaLabel={"Review job"}
                className={"min-width-px-150 float-end"}
                disabled={!canRun}
                isDispatched={false}
                onClick={handleReview}
        >
            Review<Icon ariaLabel={false}
                        className={"ms-2"}
                        icon={"bi-chevron-right"}
        />
        </Button>
    )
};