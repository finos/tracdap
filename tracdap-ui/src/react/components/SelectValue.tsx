/**
 * A component that wraps the React Bootstrap text or number input. A wrapper is needed to be able to
 * integrate the input with Redux so that it can dispatch an action when the value is changed. This
 * component can be used for inputs as well as text areas.
 *
 * @module SelectValue
 * @category Component
 */

import {aOrAn, commasAndOrs, showToast} from "../utils/utils_general";
import type {CheckValidityReturn, GetTagsFromValuePayload, SelectValueCheckValidityArgs, SelectValueProps} from "../../types/types_general";
import {convertObjectTypeToString} from "../utils/utils_trac_metadata";
import Form from "react-bootstrap/Form";
import {getObjectsFromUserDefinedObjectIds} from "../utils/utils_trac_api";
import {LabelLayout} from "./LabelLayout";
import {isDefined, isTracNumber} from "../utils/utils_trac_type_chckers";
import {isValidNumberAsStringWithCommas} from "../utils/utils_string";
import PropTypes from "prop-types";
import React, {memo, useCallback, useEffect, useRef, useState} from "react";
import {toast} from "react-toastify";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../types/types_hooks";

/**
 * The default function for the checkValidity prop. This is defined outside the component
 * in order to prevent re-renders.
 *
 * @param payload - The information required to check if the value is valid.
 */
export const defaultCheckValidity = (payload: SelectValueCheckValidityArgs): CheckValidityReturn => {

    const {
        maximumValue,
        minimumValue,
        mustValidate,
        basicType,
        value
    } = payload

    if (!mustValidate || (payload.value && minimumValue == null && maximumValue == null)) {

        return {isValid: true, message: ""}

    } else if (basicType === trac.STRING) {

        if (value != null && typeof value !== "string") {
            throw new Error(`SelectValue checkValidity function returned a non-string value for a string value`)
        } else if (!value && (minimumValue === null || minimumValue != null && minimumValue > 0)) {
            return {isValid: false, message: `Value must not be blank`}
        } else if (value && minimumValue != null && value.length < minimumValue) {
            return {isValid: false, message: `Value must be at least ${minimumValue} characters`}
        } else if (value && maximumValue != null && value.length > maximumValue) {
            return {isValid: false, message: `Value can not be more than ${maximumValue} characters`}
        } else {
            return {isValid: true, message: ""}
        }

    } else if (basicType === trac.INTEGER || basicType === trac.FLOAT || basicType === trac.DECIMAL) {

        if (value != null && typeof value !== "number") {
            throw new Error(`SelectValue checkValidity function returned a non-numeric value for number value`)
        } else if (!value) {
            return {isValid: false, message: `A value must be set`}
        } else if (minimumValue != null && maximumValue != null && (value < minimumValue || value > maximumValue)) {
            return {isValid: false, message: `A value between ${minimumValue} and ${maximumValue} is allowed`}
        } else if (minimumValue != null && value < minimumValue) {
            return {isValid: false, message: `Minimum value allowed is ${minimumValue}`}
        } else if (maximumValue != null && value > maximumValue) {
            return {isValid: false, message: `Maximum value allowed is ${maximumValue}`}
        } else {
            return {isValid: true, message: ""}
        }
    } else {
        return {isValid: true, message: ""}
    }
}

const SelectValueInner = (props: SelectValueProps) => {

    // There are some additional props that are passed directly to the LabelLayout component
    // and not destructured here
    const {
        basicType,
        className = "",
        checkValidity = defaultCheckValidity,
        getTagsFromValue,
        id,
        index,
        isDisabled = false,
        isDispatched = true,
        labelText,
        objectType,
        onChange,
        onEnterKeyPress,
        maximumValue,
        minimumValue,
        mustValidate = false,
        name,
        placeHolderText = "Please enter",
        readOnly = false,
        runningGetTagsFromValue,
        showValidationMessage = false,
        size,
        specialType,
        storeKey,
        validationChecked = false,
        value,
        validateOnMount = true
    } = props;

    console.log("Rendering SelectValue")

    // Whether an API call is being made
    const [loadingData, setLoadingData] = useState<boolean>(false)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store
    const {"trac-tenant": tenant} = useAppSelector(state => state["applicationStore"].cookies)
    const {searchAsOf} = useAppSelector(state => state["applicationStore"].tracApi)

    // Does the text box meet the required length/number range
    const isValidValue = checkValidity({
        basicType,
        id,
        maximumValue,
        minimumValue,
        mustValidate,
        name,
        value
    })

    // Class to show the invalid status, if labelText is set then the className prop is passed to the outside label div
    const finalClassName = validationChecked && !isValidValue.isValid ? `${!labelText ? className : ""} is-invalid` : `${!labelText ? className : ""}`

    /**
     * A function that runs when the user presses a button on the keyboard, it is listening for the enter key being
     * pressed and then if it is it checks the input for object ID or keys using the handleAddTracObject function.
     * This function is only used when the objectType prop is set for a string text area. This is to stop having this
     * function run for every key press for the general use case rather than just when you want to load up TRAC objects
     * somewhere.
     *
     * @param e - The key press event that triggered the function
     */
    const handleAddTracObjectOnKeyPress = async (e: React.KeyboardEvent<HTMLInputElement>): Promise<void> => {

        if (e.key === 'Enter' && typeof value === "string") {
            await handleAddTracObject(value)
        }
    }

    /**
     * A function that runs when the user presses a button on the keyboard, it is listening for the enter key being
     * pressed and then if it is it calls the onEnterKeyPress prop. It assumes that this function is not dispatched.
     * This function is used for example when logging in, and the enter key initiates a login event.
     *
     * @param e - The key press event that triggered the function
     */
    const handleOnEnterKeyPress = (e: React.KeyboardEvent<HTMLInputElement>): void => {

        if (e.key === 'Enter' && onEnterKeyPress) {
            onEnterKeyPress(value)
        }
    }

    /**
     * A function that runs when the user pastes a set of object IDs or keys into the text box and this needs to
     * be converted into an array of tags for each value. The objectType prop enforces what type of object can be
     * loaded as an option.
     *
     * @param inputValue - The user set object ID or key that they want to load.
     */
    const handleAddTracObject = useCallback(async (inputValue: string): Promise<void> => {

        // This is just for Typescript to be happy objectType is set although this function can only run if it is.
        // If the user has not set a tenant then the API call can not be made.
        if (!objectType || tenant === undefined) return

        // This is a string used in messaging e.g. "model" or "dataset"
        const objectTypeAsString = convertObjectTypeToString(objectType, true, true)

        // Used in messaging to make it good English
        const aOraN = aOrAn(objectTypeAsString)

        // Turn on the loading status
        setLoadingData(true)

        // If the runningGetTagsFromValue prop is set then this is a function that we call as handleAddTracObject
        // executes in the same way as setLoadingData above, this sets some boolean flag in the FindInTracStore that
        // shows a loading icon in some other parent component. Without this the user enters their search term and 
        // gets no real feedback that a search is running.
        if (runningGetTagsFromValue) dispatch(runningGetTagsFromValue({storeKey, running: true}))

        // Go and do the API calls
        const {
            notAnObjectIdOrKey,
            notCorrectObjectType,
            foundTags,
            notFoundTags,
        } = await getObjectsFromUserDefinedObjectIds({inputValue, objectType, oneOrMany: "many", searchAsOf, tenant})

        // Clear all the messages, so it's not confusing if the last attempt failed, but they did not clear the messages
        toast.dismiss()

        // If we found that the string was not a valid object ID or key we tell the user and do nothing
        if (notAnObjectIdOrKey.length > 0) {

            const text = `${notAnObjectIdOrKey.length} ${notAnObjectIdOrKey.length === 1 ? "entry was" : "entries were"} not a valid object ID or object key.`

            showToast("warning", text, "invalid-id-or-key")

            // Turn off the loading status
            setLoadingData(false)
            if (runningGetTagsFromValue) dispatch(runningGetTagsFromValue({storeKey, running: false}))

            // Do not continue if there is nothing to continue with
            if (foundTags.length === 0 && notFoundTags.length === 0 && notCorrectObjectType.length === 0) {

                if (getTagsFromValue && storeKey) {
                    // Assume that all create options are dispatched to a store
                    dispatch(getTagsFromValue(storeKey))
                }

                return
            }
        }

        // If we found that the string was an object key but not the same type that we are allowed to search for
        // then tell the user and do nothing
        if (notCorrectObjectType.length > 0) {

            const foundTypes = commasAndOrs(notCorrectObjectType.map(tagSelector => convertObjectTypeToString(tagSelector?.objectType || trac.ObjectType.OBJECT_TYPE_NOT_SET)))

            const text = `${notCorrectObjectType.length} ${notCorrectObjectType.length === 1 ? "entry had an object type" : "entries had object types"} of ${foundTypes}, but only  ${aOraN} ${objectTypeAsString} can be added to this list`

            showToast("warning", text, "invalid-object-types")

            // Turn off the loading status
            setLoadingData(false)
            if (runningGetTagsFromValue) dispatch(runningGetTagsFromValue({storeKey, running: false}))

            // Do not continue if there is nothing to continue with
            if (foundTags.length === 0 && notFoundTags.length === 0) {

                if (getTagsFromValue && storeKey) {
                    // Assume that all create options are dispatched to a store
                    dispatch(getTagsFromValue(storeKey))
                }

                return
            }
        }

        // Do we have any failed requests that we for a specific version - earlier versions might exist
        if (notFoundTags.length > 0) {

            const message = `${notFoundTags.length} ${notFoundTags.length === 1 ? "entry had a" : "entries had "} valid object key but there does not appear to be ${aOraN} ${objectTypeAsString} with that key.`
            const text = {message, details: notFoundTags.map(tag => tag.objectId).filter(isDefined)}

            showToast("warning", text, "create-option-error")

            // Turn off the loading status
            setLoadingData(false)
            if (runningGetTagsFromValue) dispatch(runningGetTagsFromValue({storeKey, running: false}))

            if (foundTags.length === 0) {

                if (getTagsFromValue && storeKey) {
                    // Assume that all create options are dispatched to a store
                    dispatch(getTagsFromValue(storeKey))
                }

                return
            }
        }

        // Return the payload
        const payload: GetTagsFromValuePayload = {
            basicType,
            id,
            index,
            name,
            storeKey,
            inputValue,
            tags: foundTags
        }

        // This passes the new option, the name of the select and its ID back as an object.
        // This is passed back to the store via the redux dispatch/reducer pattern.
        if (getTagsFromValue) {
            // Assume that all create options are dispatched to a store
            dispatch(getTagsFromValue(payload))

        } else if (!getTagsFromValue) {

            showToast("error", "No getTagsFromValue function set", "rejected/handleAddTracObject")
        }

        // Turn off the loading status
        setLoadingData(false)

        if (runningGetTagsFromValue) dispatch(runningGetTagsFromValue({storeKey, running: false}))

    }, [objectType, runningGetTagsFromValue, dispatch, storeKey, tenant, searchAsOf, basicType, id, index, name, getTagsFromValue])

    /**
     * A wrapper function that runs when the user changes the input, it runs the
     * function to update the value in the store if using dispatch or the value is
     * passed using a regular function if not.
     *
     * @param e - The event that triggered the function call.
     */
    function handleValueChange(e: React.ChangeEvent<HTMLInputElement> | { target: { value: string | number | null } }): void {

        // Guarantee that the right type of variable is returned as e.target.value returns string. Note that this means
        // any empty strings are retained as "".
        let value

        if (isTracNumber(basicType) && (e.target.value === null || e.target.value === "" || (typeof e.target.value === "string" && !isValidNumberAsStringWithCommas(e.target.value)))) {
            value = null
        } else if (e.target.value === "" || e.target.value === null) {
            // This just satisfies Typescript type checking
            value = null
        } else if (basicType === trac.INTEGER) {
            value = typeof e.target.value === "number" ? Math.round(e.target.value) : Math.round(parseFloat(e.target.value))
        } else if (basicType === trac.FLOAT || basicType === trac.DECIMAL) {
            value = typeof e.target.value === "number" ? e.target.value : parseFloat(e.target.value)
        } else {
            // We never allow strings to have | as that is used by the application to join values together
            // for example when storing multiple values in one string
            value = e.target.value.toString().replace(new RegExp(/\|+/g, 'g'), "")
        }

        // Does the value meet the validation requirements
        const isValidValue = checkValidity({
            basicType,
            id,
            maximumValue,
            minimumValue,
            mustValidate,
            name,
            value
        })

        const payload = {
            basicType,
            name,
            id,
            index,
            isValid: isValidValue.isValid,
            storeKey,
            value
        }

        // This passes the value selected, the name of the select and its ID back as an object.
        // This is passed back to state via the redux dispatch/reducer pattern.
        isDispatched ? dispatch(onChange(payload)) : onChange(payload)
    }

    /**
     * Awesomeness. A hook that runs when the component mounts that sends the value and the validity
     * status of the value to the store.
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
            if (validateOnMount) handleValueChange({target: {value}})
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

        if (!componentIsMounting && mustValidate) handleValueChange({target: {value}})

        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [mustValidate, minimumValue, maximumValue])

    return (
        <LabelLayout {...props}>

            {/* This is quite a horrible thing to do but is necessary due to Typescript. The Typescript rules for the*/}
            {/* Bootstrap Form.Control component say that the rows can only be used when the type is 'textarea'. Even*/}
            {/* if you set it to undefined there is an error. The only way around this other than ignoring the error*/}
            {/* is to set up the component to render on an if else.*/}

            {specialType === "TEXTAREA" ?
                // In the actual application we allow null string values but the HTML forms do not - so at the last moment we convert value = null to ""
                <Form.Control aria-disabled={isDisabled}
                              as={"textarea"}
                              autoComplete={"off"}
                              className={finalClassName}
                              disabled={isDisabled || loadingData}
                              id={id?.toString()}
                              onChange={handleValueChange}
                              onKeyPress={objectType && basicType === trac.STRING ? handleAddTracObjectOnKeyPress : undefined}
                              placeholder={placeHolderText}
                              readOnly={readOnly}
                              rows={props.rows}
                              size={size}
                              value={value === null ? "" : value}
                />
                :
                <Form.Control aria-disabled={isDisabled}
                              as={"input"}
                              autoComplete={"off"}
                              className={finalClassName}
                              disabled={isDisabled || loadingData}
                              id={id?.toString()}
                              onChange={handleValueChange}
                              onKeyPress={onEnterKeyPress ? handleOnEnterKeyPress : undefined}
                              placeholder={placeHolderText}
                              readOnly={readOnly}
                              size={size}
                              type={specialType === "PASSWORD" ? "password" : isTracNumber(basicType) ? "number" : undefined}
                              value={value === null ? "" : value}
                />
            }

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

SelectValueInner.propTypes = {

    basicType: PropTypes.oneOf([trac.INTEGER, trac.FLOAT, trac.DECIMAL, trac.STRING]).isRequired,
    className: PropTypes.string,
    checkValidity: PropTypes.func,
    getTagsFromValue: PropTypes.func,
    helperText: PropTypes.string,
    id: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
    index: PropTypes.number,
    isDisabled: PropTypes.bool,
    isDispatched: PropTypes.bool,
    labelText: PropTypes.string,
    labelPosition: PropTypes.oneOf(["top", "left"]),
    objectType: PropTypes.number,
    onChange: PropTypes.func.isRequired,
    onEnterKeyPress: PropTypes.func,
    placeHolderText: PropTypes.string,
    maximumValue: PropTypes.number,
    minimumValue: PropTypes.number,
    mustValidate: PropTypes.bool,
    name: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
    readOnly: PropTypes.bool,
    rows: PropTypes.number,
    runningGetTagsFromValue: PropTypes.func,
    showValidationMessage: PropTypes.bool,
    size: PropTypes.oneOf(["sm", "lg"]),
    specialType: PropTypes.oneOf(["PASSWORD", "TEXTAREA"]),
    storeKey: PropTypes.string,
    validationChecked: PropTypes.bool,
    value: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
    validateOnMount: PropTypes.bool,
};

export const SelectValue = memo(SelectValueInner);