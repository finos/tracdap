/**
 * A component that allows the user to select a true or false value. It is based on the
 * react-switch package that gives more functionality than the native select component in forms.
 *
 * @module SelectToggle
 * @category Component
 */

import type {CheckValidityReturn, SelectToggleCheckValidityArgs, SelectTogglePayload, SelectToggleProps} from "../../types/types_general";
import Form from "react-bootstrap/Form";
import {getThemeColour} from "../utils/utils_general";
import {LabelLayout} from "./LabelLayout";
import PropTypes from "prop-types";
import React, {memo, useEffect, useRef} from "react";
import Switch from "react-switch";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../types/types_hooks";

/**
 * The default function for the checkValidity prop. This is defined outside the component
 *  in order to prevent re-renders.
 * *
 *  * @param payload - The information required to check if the value is valid.
 *  */
export const defaultCheckValidity = ({mustValidate, value}: SelectToggleCheckValidityArgs): CheckValidityReturn => {

    if (mustValidate && (value == null || !value)) {
        return {isValid: false, message: `Please confirm`}
    } else {
        return {isValid: true, message: ``}
    }
}

const SelectToggleInner = (props: SelectToggleProps) => {

    // There are some additional props that are passed directly to the LabelLayout component
    // and not destructured here
    const {
        checkValidity = defaultCheckValidity,
        className,
        id,
        index,
        isDisabled = false,
        isDispatched = true,
        onChange,
        mustValidate = false,
        name,
        showValidationMessage = false,
        size,
        storeKey,
        validationChecked = false,
        validateOnMount = true,
        value
    } = props;

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store
    const {"trac-theme": theme} = useAppSelector(state => state["applicationStore"].cookies)

    // Does the boolean meet the required value
    const isValidValue = checkValidity({
        basicType: trac.BOOLEAN,
        id,
        mustValidate,
        name,
        value
    })

    /**
     * A wrapper function that runs when the user changes the toggle, it runs the
     * function to update the value in the store.
     *
     * @param checked - The toggle value.
     */
    function handleToggleChange(checked: boolean | null): void {

        // Does the value meet the validation requirements
        const isValidValue = checkValidity({
            basicType: trac.BOOLEAN,
            id,
            mustValidate,
            name,
            value: checked
        })

        const payload: SelectTogglePayload = {
            basicType: trac.BOOLEAN,
            id,
            index,
            isValid: isValidValue.isValid,
            name,
            storeKey,
            value: checked
        }

        // This passes the value selected, the name of the select and its ID back as an object.
        // This is passed back to state via the redux dispatch/reducer pattern.
        isDispatched ? dispatch(onChange(payload)) : onChange(payload)
    }

    /**
     * Awesomeness. A hook that runs when the component mounts that sends the value and the validity status of the value to the store.
     *
     */
    const componentIsMounting = useRef(true)

    /**
     * This runs when this component loads. This is needed because of form validation. Before we allow some
     * form elements to be used we need to validate their value in the store. But if a form item is given a value
     * upon loading and the user clicks 'send' we haven't got any validation in the store about whether
     * to allow this value.
     */
    useEffect(() => {

        // Strictly only ever run this when the component mounts
        if (componentIsMounting.current) {
            if (validateOnMount) handleToggleChange(value)
            componentIsMounting.current = false
        }

        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [validateOnMount])

    /**
     * There are props that if the value of them changes can alter the validation status of the
     * value, here we have a hook that runs IF the component is updating (rather than mounting)
     * and any of the values that are affect the validation change.
     */
    useEffect(() => {

        if (!componentIsMounting) handleToggleChange(value)

        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [mustValidate])

    const finalClassName = `react-switch align-middle ${validationChecked && !isValidValue.isValid ? "is-invalid" : ""}`

    return (
        <LabelLayout {...props}>

            {/*A 22 height is equivalent to a small bootstrap button height, 31 a medium - we don't set a style to apply this
                    as the component has an internal method to calculate the right css */}
            <label className={className}>
                <Switch activeBoxShadow={`0 0 8px ${getThemeColour(theme, "--select-toggle-shadow")}`}
                        checked={value == null ? false : value}
                        className={finalClassName}
                        disabled={isDisabled}
                        height={size === "sm" ? 22 : 31}
                        offColor={getThemeColour(theme, "--select-toggle-off-color")}
                        onChange={handleToggleChange}
                        onColor={getThemeColour(theme, "--info")}
                />
            </label>

            <React.Fragment>
                {showValidationMessage &&
                    <Form.Control.Feedback type="invalid" className={"d-block"}>
                        {validationChecked && !isValidValue.isValid ? isValidValue.message : <br/>}
                    </Form.Control.Feedback>
                }
            </React.Fragment>

        </LabelLayout>
    )
};

SelectToggleInner.propTypes = {

    basicType: PropTypes.number,
    checkValidity: PropTypes.func,
    className: PropTypes.string,
    onChange: PropTypes.func.isRequired,
    helperText: PropTypes.string,
    id: PropTypes.oneOfType([PropTypes.number, PropTypes.string, PropTypes.object]),
    index: PropTypes.number,
    isDisabled: PropTypes.bool,
    isDispatched: PropTypes.bool,
    labelPosition: PropTypes.oneOf(["top", "left"]),
    labelText: PropTypes.string,
    mustValidate: PropTypes.bool,
    showValidationMessage: PropTypes.bool,
    size: PropTypes.string,
    tooltip: PropTypes.string,
    validationChecked: PropTypes.bool,
    value: PropTypes.bool
};

export const SelectToggle = memo(SelectToggleInner);