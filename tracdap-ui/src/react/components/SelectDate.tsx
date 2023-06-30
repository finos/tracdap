/**
 * A component that allows the user to select a date. It is based on the
 * 'react-datepicker' package.
 *
 * @module SelectDate
 * @category Component
 */

import addDays from "date-fns/addDays";
import type {CheckValidityReturn, SelectDateCheckValidityArgs, SelectDatePayload, SelectDateProps} from "../../types/types_general";
import {convertDateObjectToFormatCode, convertIsoDateStringToFormatCode} from "../utils/utils_formats";
import {convertDateObjectToIsoString, setDatetimePosition} from "../utils/utils_general";
import {DateFormats} from "../../config/config_general";
import DatePicker, {registerLocale} from "react-datepicker";
import enGb from 'date-fns/locale/en-GB';
import Form from "react-bootstrap/Form";
import getDay from "date-fns/getDay";
import {isDateObject} from "../utils/utils_trac_type_chckers"
import {LabelLayout} from "./LabelLayout";
import parseISO from "date-fns/parseISO";
import PropTypes from "prop-types"
import React, {memo, useEffect, useRef} from "react";
import "react-datepicker/dist/react-datepicker.min.css";
import startOfWeek from "date-fns/startOfWeek";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch} from "../../types/types_hooks";

// This enables you to set the cadence for the widget, such as what day is considered the first of the month
registerLocale('en-gb', enGb);

/**
 * The default function for the checkValidity prop. This is defined outside the component
 * in order to prevent re-renders.
 *
 * @param payload - The information required to check if the value is valid.
 */
export const defaultCheckValidity = (payload: SelectDateCheckValidityArgs): CheckValidityReturn => {

    const {
        basicType,
        maximumValue,
        minimumValue,
        mustValidate,
        value
    } = payload

    const type = basicType === trac.DATE ? "date" : "datetime"

    if (!mustValidate || (value && minimumValue == null && maximumValue == null)) {
        return {isValid: true, message: ""}
    } else if (!value && minimumValue == null && maximumValue == null) {
        return {isValid: false, message: `Please select a ${type}`}
    } else if (value == null && minimumValue != null && maximumValue != null) {
        return {isValid: false, message: `A ${type} must be set between ${minimumValue} and ${maximumValue}`}
    } else if (value == null && minimumValue == null && maximumValue != null) {
        return {isValid: false, message: `A ${type} must be set, the maximum allowed is ${maximumValue}`}
    } else if (value == null && minimumValue != null && maximumValue == null) {
        return {isValid: false, message: `A ${type} must be set, the minimum allowed is ${minimumValue}`}
    } else if (value != null && minimumValue != null && maximumValue != null && (parseISO(value) < parseISO(minimumValue) || parseISO(value) > parseISO(maximumValue))) {
        return {isValid: false, message: `A ${type} between ${minimumValue} and ${maximumValue} is allowed`}
    } else if (value != null && minimumValue != null && parseISO(value) < parseISO(minimumValue)) {
        return {isValid: false, message: `Minimum ${type} allowed is ${minimumValue}`}
    } else if (value != null && maximumValue != null && parseISO(value) > parseISO(maximumValue)) {
        return {isValid: false, message: `Maximum ${type} allowed is ${maximumValue}`}
    } else {
        return {isValid: true, message: ""}
    }
}

/**
 * A function that sets a group of dates in the opened date picker to be highlighted. This is used when the
 * format is set to "week", in this case we show a month of dates but as the user changes date we highlight
 * the week. Note that the first day of the week is set by the registerLocale.
 *
 * @param formatCode - The format for the date e.g. DAY.
 * @param value - The Javascript Date object for the date selected.
 */
const setDateHighlightRange = (value: Date | null | undefined, formatCode: SelectDateProps["formatCode"]): { [className: string]: Date[] }[] | undefined => {

    if (formatCode === null || formatCode !== "WEEK" || value == null) return undefined

    const weekStartsOn = enGb.options?.weekStartsOn

    // weekStartsOn can be undefined in the enGb definition
    if (weekStartsOn == null) return undefined

    const startOfWeekTemp = startOfWeek(value, {weekStartsOn: enGb.options?.weekStartsOn})

    // If the value is not set the date picker will be set to today's date,
    // so we need to set the range around that
    const dayOfWeek = getDay(startOfWeekTemp)

    // A week is seven days, we offset by which is the first day of the week.
    return [{
        "react-datepicker__day--highlighted-custom": [
            addDays(startOfWeekTemp, weekStartsOn + 0 - dayOfWeek),
            addDays(startOfWeekTemp, weekStartsOn + 1 - dayOfWeek),
            addDays(startOfWeekTemp, weekStartsOn + 2 - dayOfWeek),
            addDays(startOfWeekTemp, weekStartsOn + 3 - dayOfWeek),
            addDays(startOfWeekTemp, weekStartsOn + 4 - dayOfWeek),
            addDays(startOfWeekTemp, weekStartsOn + 5 - dayOfWeek),
            addDays(startOfWeekTemp, weekStartsOn + 6 - dayOfWeek),
        ]
    }]
}

const SelectDateInner = (props: SelectDateProps) => {

    const {
        basicType,
        checkValidity = defaultCheckValidity,
        className,
        formatCode,
        id,
        index,
        isClearable = false,
        isDisabled = false,
        isDispatched = true,
        maximumValue,
        minimumValue,
        mustValidate = false,
        name,
        onChange,
        placeHolderText = "Please select",
        position,
        showValidationMessage = false,
        size,
        storeKey,
        useMenuPortalTarget = false,
        validateOnMount = true,
        validationChecked = false,
        // A default is needed as if value is undefined the component will be rendered as an
        // uncontrolled component which can have unintended consequences
        value = null
    } = props;

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Does the string date meet the required value
    const isValidValue = checkValidity({
        basicType,
        id,
        maximumValue,
        minimumValue,
        mustValidate,
        name,
        value
    })

    // Make the input look like a bootstrap input
    const finalClassName = `form-control ${size ? "form-control-" + size : ""} ${className ? className : ""} ${validationChecked && !isValidValue.isValid && "is-invalid"}`

    /**
     * A wrapper function that runs when the user changes the date, it runs the
     * function to update the value in the store if using dispatch or the value is
     * passed using a regular function if not.
     *
     * @param datetime - The datetime value as a string or Javascript date.
     */
    function handleDatetimeChange(datetime: Date | string | null): void {

        // Guarantee that the right type of variable is returned - on first render the value will be string
        // when the useEffect hook runs, only when changed by the user in the widget will a Date object be returned
        const tempValue = datetime == null || isDateObject(datetime) ? datetime : parseISO(datetime)

        // setDatetimePosition is needed in case the date is being used to set filter ranges, and we want to
        // position a date at the start or end of a window.
        const value = tempValue === null ? tempValue : convertDateObjectToIsoString(setDatetimePosition(tempValue, formatCode, position), basicType === trac.BasicType.DATE ? "dateIso" : "datetimeIso")

        // Does the date meet the required value
        const isValidValue = checkValidity({
            basicType,
            id,
            maximumValue,
            minimumValue,
            mustValidate,
            name,
            value
        })

        const finalFormatCode = formatCode != null ? formatCode : basicType === trac.DATETIME ? "DATETIME" : "DAY"

        const payload: SelectDatePayload = {
            basicType,
            // A formatted version of the value
            formatted: !datetime ? null : typeof datetime === "string" ? convertIsoDateStringToFormatCode(datetime, finalFormatCode) : convertDateObjectToFormatCode(datetime, finalFormatCode),
            id,
            index,
            isValid: isValidValue.isValid,
            name,
            storeKey,
            value,
        }

        // This passes the value selected, the name of the select and its ID back as an object.
        // This is passed back to state via the redux dispatch/reducer pattern.
        isDispatched ? dispatch(onChange(payload)) : onChange(payload)
    }

    /**
     * Awesomeness. A hook that runs when the component mounts that sends the value and the validity
     * status of the value to the store.
     */
    const componentIsMounting = useRef(true)

    /**
     * This runs when this component loads. This is needed because of form validation. Before we allow some
     * form elements to be used we need to validate their value in the store. But if a form item is given
     * a value upon loading and the user clicks 'send' we haven't got any validation in the store about whether
     * to allow this value.
     */
    useEffect(() => {

        // Strictly only ever run this when the component mounts
        if (componentIsMounting.current) {
            if (validateOnMount) handleDatetimeChange(value)
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

        if (!componentIsMounting && mustValidate) handleDatetimeChange(value)

        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [mustValidate, minimumValue, maximumValue])

    // setDatetimePosition is needed in case the date is being used to set filter ranges
    const dateAtPosition = value ? setDatetimePosition(parseISO(value), formatCode, position) : undefined

    return (
        <LabelLayout {...props}>

            <DatePicker calendarClassName={formatCode === "WEEK" ? "datepicker-select-week-custom" : undefined}
                        className={finalClassName}
                // This sets the default format based on the type of variable
                        dateFormat={formatCode != null ? DateFormats[formatCode] : basicType === trac.DATETIME ? DateFormats.DATETIME : DateFormats.DAY}
                        locale={"en-gb"}
                        fixedHeight={true}
                        highlightDates={setDateHighlightRange(dateAtPosition, formatCode)}
                        isClearable={isClearable}
                        maxDate={maximumValue ? setDatetimePosition(parseISO(maximumValue), formatCode, position) : undefined}
                        minDate={minimumValue ? setDatetimePosition(parseISO(minimumValue), formatCode, position) : undefined}
                        onChange={handleDatetimeChange}
                        placeholderText={placeHolderText}
                // This mounts the menu into a special div with the ID 'datepicker-root-portal'. This
                // is used when otherwise the menu would be clipped because it is mounted in a table for
                // example. This div is created in by the plugin itself if it is not in the dom.
                        portalId={useMenuPortalTarget ? "datepicker-root-portal" : undefined}
                        readOnly={isDisabled}
                        selected={dateAtPosition}
                        showMonthYearPicker={formatCode ? Boolean(["MONTH", "HALF_YEAR"].includes(formatCode)) : undefined}
                        showQuarterYearPicker={Boolean(formatCode === "QUARTER")}
                        showTimeInput={Boolean(formatCode != null && ["TIME", "DATETIME", "FILENAME"].includes(formatCode))}
                        showTimeSelectOnly={Boolean(formatCode === "TIME")}
                        showWeekNumbers={formatCode ? Boolean(formatCode === "WEEK") : undefined}
                        showYearPicker={Boolean(formatCode === "YEAR")}
                        timeFormat={DateFormats.TIME}
                        timeInputLabel={"Time:"}
                        wrapperClassName={"d-block"}
            />

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

SelectDateInner.propTypes = {

    basicType: PropTypes.oneOf([trac.DATE, trac.DATETIME]).isRequired,
    checkValidity: PropTypes.func,
    formatCode: PropTypes.string,
    helperText: PropTypes.string,
    id: PropTypes.oneOfType([PropTypes.number, PropTypes.string, PropTypes.object]),
    isClearable: PropTypes.bool,
    isDisabled: PropTypes.bool,
    isDispatched: PropTypes.bool,
    labelPosition: PropTypes.oneOf(["top", "left"]),
    labelText: PropTypes.string,
    labelWidth: PropTypes.oneOf(["auto", 1, 2, 3, 4]),
    minimumValue: PropTypes.string,
    maximumValue: PropTypes.string,
    mustValidate: PropTypes.bool,
    name: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
    onChange: PropTypes.func.isRequired,
    placeHolderText: PropTypes.string,
    showValidationMessage: PropTypes.bool,
    tooltip: PropTypes.string,
    useMenuPortalTarget: PropTypes.bool,
    validateOnMount: PropTypes.bool,
    validationChecked: PropTypes.bool,
    value: PropTypes.string
};

export const SelectDate = memo(SelectDateInner);