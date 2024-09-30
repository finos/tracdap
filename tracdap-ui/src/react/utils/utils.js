import {Types} from "../../config/config_trac_classifications";
import {General as Themes} from "../../config/config_themes";
import {DateFormats} from "../../config/config_general";
import format from "date-fns/format"
import formatISO from "date-fns/formatISO"
import React from "react";

/**
 * A function that converts a date or datetime object into a string date or datetime format. This uses
 * the dateFns (date functions) package which is excellent. The values of dateReturnType must map to
 * the keys used in the DateFormats config.
 *
 * @param value - The javascript date object to convert.
 * @param dateReturnType - The date format to return e.g. datetime, day, month, quarter, year.
 * @returns -
 */
export const formatDateObject = (value, dateReturnType = "day") => {

    if (dateReturnType === "dateIso") {

        return formatISO(value, {representation: 'date'})

    } else if (dateReturnType === "datetimeIso") {

        return formatISO(value)

    } else {

        let newDate = format(value, DateFormats[dateReturnType.toLowerCase()])

        // dateFns does not have a native half year format so we create one from the quarterly format
        if (dateReturnType.toLowerCase() === "half_year") {
            newDate = convertQuarterFormatToHalfYear(newDate)
        }

        return newDate
    }
}

export const convertQuarterFormatToHalfYear = (d) => {

    return d.replace(/(Q1|Q2)(\s\d{4})/g, "H1$2").replace(/(Q3|Q4)(\s\d{4})/g, "H2$2")
}

/**
 * A function that converts a delimited string into an array. It also removes any blank values due to
 * consecutive delimiters.
 *
 * @param myString (string) The string to turn into an array.
 * @param delimiter (string) The delimiter to break the string up by.
 * @returns {string[]|*}
 */
export const convertStringToArray = (myString, delimiter = "|") => {

    if (typeof myString === "string") {

        return myString.split(delimiter).filter(variable => variable !== "")

    } else if (Array.isArray(myString)) {

        return myString

    } else {

        return []
    }
}

/**
 * A function  that sorts and array of objects by a property of each object.
 * @param myArray - The array to sort.
 * @param propertyToSortBy - The property of each object to sort by.
 */
export const sortBy = (myArray, propertyToSortBy) => {

    // We need to slice to copy the array because myArray could have read only elements ie. it could come from a store
    return myArray.slice().sort((a, b) => (a[propertyToSortBy] > b[propertyToSortBy] ? 1 : -1))
}

/**
 * A function that takes a formatCode property from the schema of a variable and checks whether it
 * is a valid integer or float format code. These are of the form ",|.|2|£|m" and detail the 000s
 * separator, the decimal character, the decimal places and any pre or post unit values.
 *
 * @param formatCode (array|string) The variable format code to test.
 * @returns - Whether the format code is valid.
 */
export const isValidNumberFormat = (formatCode) => (

    ![null, undefined].includes(formatCode) && ((Array.isArray(formatCode) && formatCode.length === 5) || (typeof formatCode === "string" && (formatCode.match(/\|/g) || []).length === 4))
)

/**
 * A function that takes a formatCode property from the schema of a variable and returns the array version of it.
 *
 * @param formatCode - The variable format code to convert.
 */
export const convertNumberFormatToArray = (formatCode) => (

    isValidNumberFormat(formatCode) ? (Array.isArray(formatCode) ? formatCode : formatCode.split('|')) : null
)

export const convertUtcSecondsToDateObject = (utcSeconds) => {

    return new Date(Date.UTC(1970, 0, 1, 0, 0, 0, utcSeconds))
}


/**
 * A function that takes a formatCode property from the schema of a variable and a value for that variable and
 * returns the formatted version of it.
 *
 * @param value (number) The number to convert.
 * @param formatCode (array|string) The variable formatCode.
 * @returns -  The formatted value.
 */
export const applyNumberFormat = (value, formatCode, includeUnits = true) => {

    const formatCodeAsArray = convertNumberFormatToArray(formatCode)

    console.log(value)
    console.log(formatCode)

    // If the format code is invalid return the value unformatted.
    if (!formatCodeAsArray) {

        console.log(`LOG :: Invalid format ${formatCode} detected.`)
        return value.toString()
    }

    let finalValue = value

    // If the number of decimal places is a valid integer number
    if (!Number.isNaN(parseInt(formatCodeAsArray[2])) && typeof finalValue === "number") {

        // finalValue is now a string representation of the rounded number
        finalValue = finalValue.toFixed(Math.max(0, parseInt(formatCodeAsArray[2])))

    } else {

        finalValue = finalValue.toString()
    }

    // See https://stackoverflow.com/questions/2901102/how-to-print-a-number-with-commas-as-thousands-separators-in-javascript
    let parts = finalValue.split(".");
    parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, formatCodeAsArray[0].toString());

    // TODO need to add the units after any - sign
    if (includeUnits) {
        return formatCodeAsArray[3].toString() + parts.join(formatCodeAsArray[1].toString()) + formatCodeAsArray[4].toString();
    } else {
        return parts.join(formatCodeAsArray[1].toString())
    }
}

export const convertSchemaToOptions = (schema, labelIncludesName = false, useGroups = false, onlyCategorical = false) => {

    if (!useGroups) {

        return sortBy(schema.filter(variable => !onlyCategorical || (variable.hasOwnProperty("categorical") && variable.categorical === true)).map(variable => (

            {
                value: variable.fieldName,
                label: `${setLabel(variable)}${labelIncludesName ? " (" + variable.fieldName + ")" : ""}`,
                schema: {...variable}
            }

        )), "label")

    } else {

        // Create a property for each type of data - we are going to filter the schema into these lists
        let lists = {}
        Types.tracBasicTypes.forEach(dataType => {
            lists[dataType.value] = []
        })

        // Put the variables into the right properties
        schema.forEach(variable => {
            lists[variable.fieldType].push(variable)
        })

        // Treat each property as its own mini schema
        Object.keys(lists).forEach(key => {

            lists[key] = convertSchemaToOptions(lists[key], labelIncludesName, false, onlyCategorical)
        })

        // Convert to the structure to be read as options with headers by the SelectOption component
        let options = []
        Types.tracBasicTypes.forEach(dataType => {
            options.push({label: dataType.label, options: lists[dataType.value]})
        })

        return options
    }
}

/**
 * A function that runs when the user wants to download a file from the application. It sets the file name
 * for the download using metadata from the object and the user.
 * @param header - The header property of the TRAC metadata of the object being downloaded.
 * @param fileType - The fle type to use as the extension of the file e.g. csv. Can be blank if the
 * plugin performing the export automatically adds it's own file extension.
 * @param addDate - Whether to add a the datetime of the download to the filename.
 * @param userId - The user ID of the person downloading the file.
 * @returns - The filename.
 */
export const setDownloadName = (header, fileType, addDate, userId) => {

    // This sets defaults if the properties are not set
    const {objectType = "none", objectId = "none", objectVersion = "none", tagVersion = "none"} = header

    // Set a date to add
    const date = addDate ? `_${formatDateObject(new Date(), "filename")}` : ""

    // The check on filetype is needed because some plugins like HighCharts add their own extension so the . is not needed
    // TODO lower case was used here but we don't have the string equivalent
    return `${objectType}_${objectId}_objectVersion_${objectVersion}_tagVersion_${tagVersion}_${userId}${date}${fileType && fileType.length > 0 ? "." : ""}${fileType}`
}

export const getThemeColour = (name) => {

    return Themes.lightTheme[name]
}

/**
 * A function that parses an array of objects, such as a dataset, and extracts rows that
 * have a unique combinations of variables set by the variablesToCheck list.
 * @param data - The dataset to extract the unique rows from.
 * @param variablesToCheck - The list of properties in the row to extract the unique combinations of.
 * @returns -
 */
export const getUniqueRowsOfArrayOfObjects = (data, variablesToCheck) => {

    let uniqueRows = [];

    data.filter(row => {

        let i = uniqueRows.findIndex(x => {

            let completeMatch = true

            variablesToCheck.forEach(variable => {

                if (x[variable] !== row[variable]) {
                    completeMatch = false
                }
            })

            return completeMatch
        });

        if (i <= -1) {

            let newRow = {}
            variablesToCheck.forEach(variable => {

                newRow[variable] = row[variable]
            })

            uniqueRows.push(newRow);
        }

        return null;

    });

    return uniqueRows
}

export const getUniqueVariableTypes = (schema) => [...new Set(schema.map(item => item.fieldType))]

export const schemaIsAllNumbers = (schema) => {

    const newSchema = Array.isArray(schema) ? schema : [schema]
    return newSchema.length > 0 && newSchema.every(item => ["INTEGER", "FLOAT", "DECIMAL"].includes(item.fieldType))
}

export const schemaIsAllDatesOrDatetimes = (schema) => {

    const newSchema = Array.isArray(schema) ? schema : [schema]
    return newSchema.length > 0 && newSchema.every(item => ["DATE", "DATETIME"].includes(item.fieldType))
}

export const schemaIsAllIntegers = (schema) => {

    const newSchema = Array.isArray(schema) ? schema : [schema]
    return newSchema.length > 0 && newSchema.every(item => ["INTEGER"].includes(item.fieldType))
}

export const schemaIsMissingNumberFormatCode = (schema) => {

    const newSchema = Array.isArray(schema) ? schema : [schema]
    return newSchema.length > 0 && newSchema.some(item => !isValidNumberFormat(item.formatCode))
}

export const getAllDatesOrDatetimeVariables = (schema) => {

    const newSchema = Array.isArray(schema) ? schema : [schema]
    return newSchema.length > 0 && newSchema.filter(item => ["DATE", "DATETIME"].includes(item.fieldType))
}

export const getAllDateAndDatetimeVariables = (schema) => {

    const newSchema = Array.isArray(schema) ? schema : [schema]
    return newSchema.filter(item => ["DATE", "DATETIME"].includes(item.fieldType))
}


export const getAllNumberVariables = (schema) => {

    const newSchema = Array.isArray(schema) ? schema : [schema]
    return newSchema.filter(item => ["INTEGER", "FLOAT", "DECIMAL"].includes(item.fieldType))
}


export const getUniqueFormatCodes = (schema) => (

    [...new Set(schema.map(variable => variable.formatCode))].map(format => convertNumberFormatToArray(format)).filter(format => ![null, undefined, ""].includes(format))
)


export const getUniqueDatetimeFormatCodes = (schema) => (

    [...new Set(getAllDatesOrDatetimeVariables(schema).map(variable => variable.formatCode))].filter(format => ![null, undefined, ""].includes(format))
)

export const getUniqueNumberFormatCodes = (schema) => (

    [...new Set(getAllNumberVariables(schema).map(variable => variable.formatCode))].filter(format => ![null, undefined, ""].includes(format)).map(format => convertNumberFormatToArray(format))
)



export const hexToRgb = (hex) => {
    let result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
    return result ? {
        r: parseInt(result[1], 16),
        g: parseInt(result[2], 16),
        b: parseInt(result[3], 16)
    } : null;
}