/**
 * A group of utilities for processing formats.
 * @category Utils
 * @module FormatUtils
 */

import {DateFormat, DatetimeFormat, NumberFormatAsArray} from "../../types/types_general";
import {isDateFormat, isDateObject, isTracDateOrDatetime, isTracNumber} from "./utils_trac_type_chckers";
import {isValidIsoDateString, isValidIsoDatetimeString, isValidNumberAsString} from "./utils_string";
import {DateFormats, General} from "../../config/config_general";
import format from "date-fns/format";
import parseISO from "date-fns/parseISO";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * A function that takes a formatCode property from the schema of a variable and checks whether it
 * is a valid integer, decimal or float code. These are of the form ",|.|2|£|m" or "|.|2||%|100",
 * when split these become the 000 separator, the decimal character, the decimal places and any pre or
 * post unit values. There is a final optional part which is a multiplier that is the number to multiply
 * the number by to make the units apply, for example if a number is 0.01 and this is 1% a format of
 * "|.|0||%|100" would apply.
 *
 * This function allows the decimal places and the multiplier to be empty and still be a valid format
 * code, the {@link applyNumberFormat} handles missing values when applying the format.
 *
 * @param formatCodeAsString - The variable format code to test.
 */
export const isValidNumberFormatCode = (formatCodeAsString: string | null | undefined): formatCodeAsString is string => {

    // Format codes need to have either 4 or 5 pipes
    if (formatCodeAsString == null || ![4, 5].includes((formatCodeAsString.match(/\|/g) || []).length)) {

        return false

    } else {

        const splitFormatCode = formatCodeAsString.split("|")
        return (splitFormatCode[2] === "" || isValidNumberAsString(splitFormatCode[2])) && (splitFormatCode.length === 5 || (splitFormatCode.length === 6 && (splitFormatCode[5] === "" || isValidNumberAsString(splitFormatCode[5]))))
    }
}

/**
 * A function that takes sets the default number format as a string when no formatCode is set or the provided
 * code is not valid. The default values are set in the client-config.json.
 *
 * @param basicType - The TRAC basic type of the variable that we want to get the default number format for.
 * @returns A string version of a number format from the default values.
 */
export const assignDefaultNumberFormat = (basicType: null | undefined | trac.BasicType.FLOAT | trac.BasicType.DECIMAL | trac.BasicType.INTEGER = undefined): string => {

    if (basicType == null) {

        return General.defaultFormats.float

    } else if (basicType === trac.INTEGER) {

        return General.defaultFormats.integer

    } else if (basicType === trac.FLOAT) {

        return General.defaultFormats.float

    } else {

        return General.defaultFormats.decimal
    }
}

/**
 * A function that takes the formatCode property from the schema of a numeric variable and returns the array version of it.
 * If the format code is not valid then the default format is used instead.
 *
 * @param formatCodeAsString - The variable format code to convert.
 * @param basicType - The type of number variable, this is used to set the format if the formatCode is not valid.
 * @returns The numeric format as an array.
 */
export const convertNumberFormatCodeToArray = (formatCodeAsString: string | null | undefined, basicType?: trac.BasicType.DECIMAL | trac.BasicType.FLOAT | trac.BasicType.INTEGER): NumberFormatAsArray => {

    if (formatCodeAsString == null || !isValidNumberFormatCode(formatCodeAsString)) {

        return convertNumberFormatCodeToArray(assignDefaultNumberFormat(basicType), basicType)
    }

    let formatCodeAsArray = formatCodeAsString.split('|').map((item, i) => (i === 2) ? (item === "" ? null : parseInt(item)) : i === 5 ? (item === "" ? 1 : parseFloat(item)) : item)

    // If a format doesn't have a multiplier set then add one as 1 (no change)
    if (formatCodeAsArray.length === 5) {

        formatCodeAsArray.push(1)
    }

    return formatCodeAsArray as NumberFormatAsArray
}

/**
 * A function that converts a Javascript datetime object into formatted date or datetime. The returned
 * value is in the format specified in the application config. This uses the dateFns (date functions)
 * package which is excellent.
 *
 * @param value - The javascript date object to convert.
 * @param formatCodeAsString - The date format to return e.g. datetime, day, month, quarter, year.
 * @returns The formatted date string.
 */
export const convertDateObjectToFormatCode = (value: Date, formatCodeAsString: DateFormat | DatetimeFormat = "DAY"): string => {

    let newDate

    // dateFns does not have a native half year format, so we need to have a bespoke function for that based off first
    // converting it to a quarterly date. We don't use the default format in the config for this as there is no guarantee
    // that that will not be changed.
    if (formatCodeAsString === "HALF_YEAR") {

        // Apply a Quarterly format e.g. Q1 2022
        newDate = format(value, "QQQ yyyy")
        // Convert to Half year format
        newDate = convertQuarterFormatToHalfYear(newDate)

    } else {

        newDate = format(value, DateFormats[formatCodeAsString])
    }

    return newDate
}

/**
 * A function that converts a ISO date or datetime string into a new date or datetime format. This uses
 * the dateFns (date functions) package which is excellent. This function does not check that the value is a valid
 * ISO datetime, the isValidIsoDatetimeString function can be used for that check.
 *
 * @remarks
 * Note that this function does not check if the myString argument is a valid ISO datetime string.
 *
 * @param myString - The ISO date string to convert e.g. 2012-10-26.
 * @param formatCodeAsString - The date format to return e.g. datetime, day, month, quarter, year
 * @returns The formatted date string.
 */
export const convertIsoDateStringToFormatCode = (myString: string, formatCodeAsString: DateFormat | DatetimeFormat = "DAY"): string => {

    let newDate = parseISO(myString)

    return convertDateObjectToFormatCode(newDate, formatCodeAsString)
}

/**
 * A function that converts a string for a quarterly date into the equivalent half year date. This is needed as the
 * dateFns package we use does not support a native half year format.
 *
 * @param myString - The quarterly date string to convert.
 * @returns The half-year formatted date string.
 *
 * @example
 * console.log(convertQuarterFormatToHalfYear("Q2 2023")) // "H1 2023"
 */
export const convertQuarterFormatToHalfYear = (myString: string): string => (

    myString.replace(/(Q1|Q2)(\s\d{4})/g, "H1$2").replace(/(Q3|Q4)(\s\d{4})/g, "H2$2")
)

/**
 * A function that takes a boolean and converts it into a formatted string version. This is needed because in
 * many places a false boolean value will be read by Javascript as not to do something, so it won't show a
 * false boolean in a table for example.
 *
 * @param value - The value to format
 */
export const applyBooleanFormat = (value: undefined | null | boolean): string | null => (

    value ? "True" : value === false ? "False" : null
)

/**
 * A function that takes a formatCode string from the schema of a variable and a value for that variable and
 * returns the formatted version of it.
 *
 * @param value - The number to convert.
 * @param formatCodeAsString - The formatCode.
 * @param includeUnits - Whether to include the units in the formatted string. You typically do not want
 * the units in chart axis labels where there is a chart label with them in.
 * @returns The formatted numeric value.
 */
export const applyNumberFormat = (value: number, formatCodeAsString?: string | null, includeUnits: boolean = true): string => {

    // Note that convertNumberFormatCodeToArray adds in the multiplier as 1 if formatCodeAsString does not have one
    const formatCodeAsArray = convertNumberFormatCodeToArray(formatCodeAsString)

    // Apply the multiplier, so if the number is 0.01 and this should be 1% the multiplier should be 100
    value = value * formatCodeAsArray[5]

    // finalValue is now a string representation of the rounded number
    // If decimal places is not set then the string is not rounded
    let finalValue = formatCodeAsArray[2] === null ? value.toString() : value.toFixed(Math.max(0, formatCodeAsArray[2]))

    // See https://stackoverflow.com/questions/2901102/how-to-print-a-number-with-commas-as-thousands-separators-in-javascript
    let parts = finalValue.split(".");
    parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, formatCodeAsArray[0]);

    // The first character of the string
    let minusOrPlus = parts[0].charAt(0)

    // We need to put the pluses and minuses before the formats
    if (["+", "-"].includes(minusOrPlus)) {
        // Remove the leading +/-
        parts[0] = parts[0].substring(1)
    } else {
        minusOrPlus = ""
    }

    if (includeUnits) {
        return minusOrPlus + formatCodeAsArray[3] + parts.join(formatCodeAsArray[1]) + formatCodeAsArray[4];
    } else {
        return minusOrPlus + parts.join(formatCodeAsArray[1])
    }
}

/**
 * A function that applies a format to a variable for showing in the interface. For example this is used in tables
 * to make the data look nice. No formatting is applied to string and boolean types while date/datetime values and
 * numbers have different handling.
 *
 * @param basicType - The fieldType from the TRAC schema for the variable.
 * @param formatCodeAsString - The variable formatCode from the TRAC schema.
 * @param value - The value to format.
 * @returns The formatted value.
 */
export const setAnyFormat = (basicType: trac.BasicType, formatCodeAsString: null | string | DatetimeFormat | DateFormat, value: Date | null | boolean | string | number): null | string => {

    if (value == null) return null

    if (isTracDateOrDatetime(basicType) && typeof value === "string" && formatCodeAsString && isDateFormat(formatCodeAsString)) {

        if (basicType === trac.DATE && isValidIsoDateString(value)) {

            return convertIsoDateStringToFormatCode(value, formatCodeAsString)

        } else if (basicType === trac.DATETIME && isValidIsoDatetimeString(value)) {

            return convertIsoDateStringToFormatCode(value, formatCodeAsString)

        } else {

            // This should not happen but could, a string that is an invalid ISO date string  goes through here
            return value
        }

    } else if (isTracDateOrDatetime(basicType) && isDateObject(value) && formatCodeAsString && isDateFormat(formatCodeAsString)) {

            return convertDateObjectToFormatCode(value, formatCodeAsString)

    } else if (isTracNumber(basicType) && typeof value === "number" && formatCodeAsString) {

        return applyNumberFormat(value, formatCodeAsString)

    } else if (basicType === trac.BOOLEAN && typeof value === "boolean") {

        return applyBooleanFormat(value)

    } else {

        return value.toString()
    }
}