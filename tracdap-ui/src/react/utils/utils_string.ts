/**
 * A group of utilities for processing stings.
 * @category Utils
 * @module StringUtils
 */

import {BasicTypesString, DataRow, DataValues, ObjectTypesString, PartTypesString} from "../../types/types_general";
import {Dictionary} from "../../config/config_dictionary";
import isValid from "date-fns/isValid";
import parseISO from "date-fns/parseISO";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {isTracBasicTypeString, isTracObjectTypeString} from "./utils_trac_type_chckers";
import {parse} from "papaparse";

/**
 * A function that capitalises a string.
 * @param myString -  The string to capitalise.
 * @returns A capitalised string.
 *
 * @example
 * const text = capitaliseString('example')
 * console.log(text) // 'Example'
 */
export const capitaliseString = (myString: string): string => myString.charAt(0).toUpperCase() + myString.slice(1)

/**
 * A function that takes a key (e.g. a property key in a TRAC metadata object) and converts it to
 * a human-readable piece of text. It can cope with camel case, _ and dot notations and also
 * splits numbers out as items. Regex can be checked here https://regex101.com/.
 *
 * The function makes an additional check to see if a word in the key is in a dictionary of
 * acronyms that should be converted to capitals.

 * @param key - The key to convert.
 * @param capitalise - Whether to capitalise the first word found in the key.
 * @returns A humanreadable version of the key.
 *
 * @example
 * const text = convertKeyToText('somePropertyKey', true)
 * console.log(text) // 'Some property text'
 */
export const convertKeyToText = (key: string, capitalise: boolean = true): string => {

    // Split into words where there is an _ or an Aa (capital and none capital), allow : to be retained
    // Use this to check any changes https://regex101.com/
    let newString = key.match(/:|[A-Z]$|[A-Z]{2,}|[a-z]+|[A-Z][a-z]+|[0-9]+|^[A-Z]+/g)

    // Right so now we have the key split out as an array. Some elements well be
    // acronyms, so we have a list of these in the config that we can check against
    return newString == null ? key : newString.map((part, i) => {

        let newPart = part.toLowerCase()

        newPart = Dictionary.acronyms.includes(newPart) ? newPart.toUpperCase() : newPart

        if (i === 0 && capitalise) {
            // The toString is there to cover the case when the first element is a number
            newPart = capitaliseString(newPart.toString())
        }
        return newPart

    }).join(' ')
}

/**
 * A function that performs a regex test on a string to see if it is a number. This is for when the string may have
 * comma separators or want to differentiate whether the commas are set correctly. This can also cope with when the
 * string has a number in scientific notation.
 *
 * An example of this being used is when parsing a csv and trying to guess the schema of the dataset.
 *
 * @see https://stackoverflow.com/questions/16148034/regex-for-number-with-decimals-and-thousand-separator
 *
 * @remarks
 * The regex corresponds to:
 * 1. Either - or + which is optional.
 * 2a. Then at least one digit or zero to 2 digits followed by any number of repetitions of a comma and 3 digits. (123,999)
 * 2b. or just an optional 0 (0.1 and .1)
 * 2c. or one non-zero digit followed by  any number digits. (123999)
 * 4. Then an optional decimal and then some optional digits. i.e. a number!
 *
 * @param myString - The string to check.
 */
export const isValidNumberAsStringWithCommas = (myString: string): boolean => {

    // The second check is to see if the string is in scientific notation
    return myString.length > 0 && (/^(((\+?|-?)\d+)|((\+?|-?)([1-9]\d{0,2}(,\d{3})*|0?|[1-9]\d*)(\.\d+)?))$/.test(myString) || !isNaN(Number(myString)))
}

/**
 * A function that returns whether a string is a valid number. This is for when you know that the string does not have
 * comma separators.
 *
 * @param myString - The string to check.
 */
export const isValidNumberAsString = (myString: string): boolean => myString.length !== 0 && !isNaN(Number(myString))

/**
 * A function that removes non-numeric characters from a string.
 *
 * @remarks
 * This function allows dots/periods and minus signs to remain in the string
 * as these can be valid in a number. However, other regions may use commas
 * for the decimal point, in which case if the cleaned string is converted to
 * a number it will give the wrong result.
 *
 * @remarks
 * This function doe not guarantee that the returned string is a valid number
 * e.g. "-1.v3.5" will get cleaned to "-1.3.5".
 *
 * @param myString - The string to remove non-numeric characters from.
 *
 * @example
 * console.log(removeNonNumericCharacters("")) // ''
 * console.log(removeNonNumericCharacters("xyx566")) // '566'
 * console.log(removeNonNumericCharacters("5xyx!,46")) // '546'
 * console.log(removeNonNumericCharacters("-xyx!,4.6")) // '-4.6'
 */
export function removeNonNumericCharacters(myString: string): string {

    return myString.replace(/[^0-9.-]/g, '');
}

/**
 * A function that removes non-numeric characters from a string and converts this to an
 * integer. Note that the parseFloat function returns NaN if the string is not a valid
 * number, this (oddly) is still considered a number type.
 *
 * @remarks
 * This does not correctly convert scientific notation strings.
 *
 * @param myString - The string to convert to an integer.
 * @returns An integer parsed from the given string or NaN.
 *
 * @example
 * console.log(convertStringToInteger("")) // NaN
 * console.log(convertStringToInteger("xyx566")) // 566
 * console.log(convertStringToInteger("5xyx!,46")) // 546
 * console.log(convertStringToInteger("-xyx!,4.6")) // -5
 *
 * @example
 * Why do we not use parseInt?
 * ```ts
 * console.log(parseInt("1.5")) // 1
 * ```
 */
export function convertStringToInteger(myString: string): number {

    return Math.round(parseFloat(removeNonNumericCharacters(myString)))
}

/**
 * A function that checks whether a string is a valid date format. When users load data they
 * may use a range of valid formatted dates that need to be converted into an ISO date format
 * before being loaded into TRAC. This is particularly important when loading from Excel
 * files as Excel tries to convert anything that looks like a date and therefore loses the
 * original formatting.
 *
 * @remarks
 * This function does not check that the number of days in the month is right, just that the
 * correct numeric format is  being used for a valid date.
 *
 * @param myString - The string to check.
 * @returns Both the identified format and the date/datetime format that this implies.
 *
 * @example
 * console.log(getDateFormatFromString("31/11/2022")) // {inFormat: 'dd/MM/yyyy', type: 'DATE'}
 * console.log(getDateFormatFromString("30/11/2022")) // {inFormat: 'dd/MM/yyyy', type: 'DATE'}
 * console.log(getDateFormatFromString("2022/12/01")) // {inFormat: 'yyyy/MM/dd', type: 'DATE'}
 * console.log(getDateFormatFromString("2022-12-01")) // {inFormat: 'yyyy-MM-dd', type: 'DATE'}
 * console.log(getDateFormatFromString("31 November 2022")) // {inFormat: 'dd MMMM yyyy', type: 'DATE'}
 * console.log(getDateFormatFromString("30 November 2022")) // {inFormat: 'dd MMMM yyyy', type: 'DATE'}
 * console.log(getDateFormatFromString("30 Feb 2022")) // {inFormat: 'dd MMM yyyy', type: 'DATE'}
 * console.log(getDateFormatFromString("1 Feb 2022")) // {inFormat: null, type: 'STRING'}
 * console.log(getDateFormatFromString("01 Feb 2022")) // {inFormat: 'dd MMM yyyy', type: 'DATE'}
 * console.log(getDateFormatFromString("Q1 2022")) // {inFormat: 'QQQ yyyy', type: 'DATE'}
 * console.log(getDateFormatFromString("2010-06-15T11:01:59+02:00")) // {inFormat: 'isoDatetime', type: 'DATETIME'}
 * console.log(getDateFormatFromString("2010-06-15T11:01:59Z")) // {inFormat: 'isoDatetime', type: 'DATETIME'}
 * console.log(getDateFormatFromString("1997-07-16T19:20:30.123+01:00")) // {inFormat: 'isoDatetime', type: 'DATETIME'}
 * console.log(getDateFormatFromString("2010-06-15T11:01:59.123Z")) // {inFormat: 'isoDatetime', type: 'DATETIME'}
 * console.log(getDateFormatFromString("")) // {inFormat: null, type: 'STRING'}
 */
export const getDateFormatFromString = (myString: string): { inFormat: "dd/MM/yyyy" | "yyyy/MM/dd" | "yyyy-MM-dd" | "dd MMMM yyyy" | "dd MMM yyyy" | "QQQ yyyy" | "isoDatetime" | null, type: "DATE" | "DATETIME" | "STRING" } => {

    if (/^(0[1-9]|[12][0-9]|3[01])\/(0[1-9]|1[012])\/\d{4}$/.test(myString)) {

        // Check if the string is 01/10/2021 from Excel (short date)
        return {inFormat: "dd/MM/yyyy", type: "DATE"}

    } else if (/^\d{4}\/(0[1-9]|1[012])\/(0[1-9]|[12][0-9]|3[01])$/.test(myString)) {

        // Check if the string is 2021/10/01
        return {inFormat: "yyyy/MM/dd", type: "DATE"}

    } else if (/^\d{4}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$/.test(myString)) {

        // Check if the string is the ISO standard
        return {inFormat: "yyyy-MM-dd", type: "DATE"}

    } else if (/^(0[1-9]|[12][0-9]|3[01])\s(January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}$/.test(myString)) {

        // Check if the string is 01 October 2021 from Excel (long date)
        return {inFormat: "dd MMMM yyyy", type: "DATE"}

    } else if (/^(0[1-9]|[12][0-9]|3[01])\s(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s\d{4}$/.test(myString)) {

        // Check if the string is 01 Oct 2021
        return {inFormat: "dd MMM yyyy", type: "DATE"}

    } else if (/^Q[1-4]\s\d{4}$/.test(myString)) {

        // Check if the string is Q4 2021
        return {inFormat: "QQQ yyyy", type: "DATE"}

    } else if (/^\d{4}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])T[0-2]\d:[0-5]\d:[0-5]\d\.\d+([+-][0-2]\d:[0-5]\d|Z)$/.test(myString)) {

        // With milliseconds
        //https://stackoverflow.com/questions/3143070/javascript-regex-iso-datetime
        return {inFormat: "isoDatetime", type: "DATETIME"}

    } else if (/^\d{4}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])T[0-2]\d:[0-5]\d:[0-5]\d([+-][0-2]\d:[0-5]\d|Z)$/.test(myString)) {

        // Without milliseconds
        //https://stackoverflow.com/questions/3143070/javascript-regex-iso-datetime
        return {inFormat: "isoDatetime", type: "DATETIME"}

    } else {

        return {inFormat: null, type: "STRING"}
    }
}

/**
 * A function that converts a string corresponding to a TRAC basic type to its enum equivalent.
 * @param type - The basic type to convert.
 *
 * @example
 * console.log(convertStringToBasicType("STRING")) // 4
 */
export const convertStringToBasicType = (type: BasicTypesString): trac.BasicType => trac.BasicType[type]

/**
 * A function that converts a string corresponding to a TRAC object type to its enum equivalent.
 * @param type - The object type to convert.
 *
 * @example
 * console.log(convertStringToObjectType("DATA")) // 1
 */
export const convertStringToObjectType = (type: ObjectTypesString): trac.ObjectType => trac.ObjectType[type]

/**
 * A function that converts a string corresponding to a TRAC part type to its enum equivalent.
 * @param type - The part type to convert.
 *
 * @example
 * console.log(convertStringToPartType("PART_BY_VALUE")) // 2
 */
export const convertStringToPartType = (type: PartTypesString): trac.PartType => trac.PartType[type]

/**
 * A function that checks if a string is a valid ISO date. The check that the string is of a length 10
 * is due to an issue with parseISO. For example the number 10 is a valid ISO date as it can be interpreted
 * as 1910.
 *
 * @see See this issue https://github.com/date-fns/date-fns/issues/1748
 *
 * @param dateString - The string to check.
 */
export const isValidIsoDateString = (dateString: string): boolean => (

    dateString.length === 10 && isValid(parseISO(dateString))
)

/**
 * A function that checks if a string is a valid ISO datetime. The check that the string is of a length 10
 * is due to an issue with parseISO. For example the number 10 is a valid ISO date as it can be interpreted
 * as 1910.
 *
 * @see See this issue https://github.com/date-fns/date-fns/issues/1748
 *
 * @param dateTimeString - The string to check.
 */
export const isValidIsoDatetimeString = (dateTimeString: string): boolean => (

    dateTimeString.length >= 10 && isValid(parseISO(dateTimeString))
)

/**
 * A function that parses a string with a regex and either just returns an array of the matches when the 'groups'
 * argument is not set or an array per group of the matches in the group.
 *
 * @see https://stackoverflow.com/questions/432493/how-do-you-access-the-matched-groups-in-a-javascript-regular-expression
 *
 * @param myString - The string to find the matches in.
 * @param regexPattern - e.g. /d="(.*)"/g
 * @param groups - The number of groups in regexPattern this is the number of items from the string that you
 * are trying to get back, these have () around them normally to signify a group.
 *
 * @returns An array of matches from the regex, if there is more than one group in the regex then each element will be
 * an array of strings, one for each of the groups.
 *
 * @remarks This function has multiple functional signatures, each defining a function overload where the return type depends on the
 * value of the 'groups' argument. The final signature is the generalised form with the first two the overloads that say that the
 * type of the returned array varies depending on whether 'groups' is set.
 *
 * @see https://stackoverflow.com/questions/66134236/typescript-function-return-type-depending-on-number-or-type-of-arguments
 *
 * @example
 * let term = "VARIABLE_1"
 * // Check if it is a single word \w+ is regex for a word
 * let variables = getAllRegexMatches(term, /^(\w+)$/g)
 * console.log(variables) // ['VARIABLE_1']
 *
 * @example
 * let term = "sum(VARIABLE_1) as SUM_VARIABLE_1"
 * // Check if it is a SQL rename
 * let variables = getAllRegexMatches(term, /^(\w+)\s*\(\s*(\w+)\s*\)\s+as\s+(\w+)$/g, 3)
 * console.log(variables) // [['sum', 'VARIABLE_1', 'SUM_VARIABLE_1']]
 *
 * @example
 * let term = "sum(VARIABLE_1) as SUM_VARIABLE_1, count(VARIABLE_2) as COUNT_VARIABLE_2"
 * // Check if it is a SQL function and rename for multiple variables
 * let variables = getAllRegexMatches(term, /(\w+)\s*\(\s*(\w+)\s*\)\s+as\s+(\w+)/g, 3)
 * console.log(variables) // [['sum', 'VARIABLE_1', 'SUM_VARIABLE_1'], ['count', 'VARIABLE_2', 'COUNT_VARIABLE_2']]
 */
export function getAllRegexMatches(myString: string, regexPattern: RegExp, groups: number): (string[])[];
export function getAllRegexMatches(myString: string, regexPattern: RegExp): string[];
export function getAllRegexMatches(myString: string, regexPattern: RegExp, groups?: number): (string | string[])[] {

    let results = [];

    let match
    const regex = new RegExp(regexPattern);

    while (null !== (match = regex.exec(myString))) {
        results.push(groups === undefined ? match[1] : match.slice(1, groups + 1))
    }

    return results
}

/**
 * A function that converts a string to a boolean equivalent
 * @param myString - The string to convert, nulls converted to null.
 * @returns The equivalent boolean for the given string or null is it if not a valid boolean.
 *
 * @example
 * console.log(convertStringValueToBoolean(null)) // null
 *
 * @example
 * console.log(convertStringValueToBoolean('true')) // true
 *
 * @example
 * console.log(convertStringValueToBoolean('FalSe')) // false
 */
export const convertStringValueToBoolean = (myString: string | null): boolean | null => {

    return myString === null || !["TRUE", "FALSE"].includes(myString.toUpperCase()) ? null : myString.toUpperCase() === "TRUE"
}

/**
 * A function that removes line breaks from a string. This is used for example when sending an SQL query to be executed,
 * but we need to remove any linebreaks in it that have been added to make it  more readable on screen.
 *
 * @remarks
 * \r matches a carriage return (ASCII 13)
 * \n matches a line-feed (newline) character (ASCII 10)
 *
 * @param myString - The string to remove linebreaks from.
 * @returns The string without line breaks.
 */
export const removeLineBreaks = (myString: string): string => myString.replace(/\r?\n|\n?\r/g, " ")

/**
 * A function that adds a '/' to a string if it does not end with one, this is used when forming URLS from multiple
 * strings, and you need to make sure the path created is valid with a trailing slash.
 *
 * @param myString - The string to check.
 * @returns The updated string.
 */
export const checkUrlEndsRight = (myString: string): string => {

    return myString.slice(myString.length - 1) === "/" ? myString : myString + "/"
}

/**
 * A function that checks a string to see if it is a valid object key (a shorthand for object type, ID and version) and
 * returns false if it is invalid and true if it is. Object keys are of the form "MODEL-4dfc8132-153c-43eb-95b3-bd4b64e04122-v1".
 *
 * @param objectKey - The string to check.
 * @returns Whether the string is a valid object key.
 *
 * @example
 * const myString = "FLOW-597b589b-96fa-4e56-8066-d01fedcfcad1-v4"
 * console.log(isObjectKey(myString)) // true
 *
 * @example
 * const myString = "3327ff48-6311-4223-aa24-411f02656284" // Although a valid object ID
 * console.log(isObjectKey(myString)) // false
 */
export const isObjectKey = (objectKey: any): boolean => {

    if (typeof objectKey !== "string") return false

    // The leading string before the first '-'. In a valid objectKey this is a TRAC object type
    const objectKeyAsArray = objectKey.split("-")

    // Is the first element of the string to check a recognised object type (e.g. "DATA") and is the rest an objectId with a version added
    return isTracObjectTypeString(objectKeyAsArray[0].toUpperCase()) && /^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}-(v[0-9]+|latest)$/.test(objectKeyAsArray.slice(1).join("-"))
}

/**
 * A function that returns whether a string is a valid object ID from TRAC. Object ID
 * values are UUIDs of the form "97f1a213-dc88-4cc8-9643-1e9f21018e27".
 *
 * @param objectId - The string to check.
 * @returns Whether the string is a valid object ID.
 *
 * @example
 * const myString = "3327ff48-6311-4223-aa24-411f02656284"
 * console.log(isObjectId(myString)) // true
 *
 * @example
 * const myString = "FLOW-597b589b-96fa-4e56-8066-d01fedcfcad1-v4" // Although a valid object Key
 * console.log(isObjectId(myString)) // false
 */
export const isObjectId = (objectId: any): boolean => {

    return typeof objectId === "string" && /^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$/.test(objectId)
}

/**
 * A function that takes a string which contains a schema csv file and converts it into a schema array. This is
 * needed when downloading a model's csv schema from a Git repository which is returned as a string.
 *
 * @param myString - The string version of the csv schema file.
 * @param maximumNumberOfErrors - The maximum number of errors to show the user.
 */
export const convertStringToSchema = (myString: string, maximumNumberOfErrors: number = 20): { fields: trac.metadata.IFieldSchema[], errorMessages: string[] } => {

    // Convert the csv as a string to a JSON
    const data = parse<DataRow>(myString, {
        delimiter: ",",
        header: true,
        skipEmptyLines: true,
        worker: false,
    });

    // A mapping between the CSV variable names and the TRAC schema properties
    const schemaNameLookup: Record<string, keyof trac.metadata.IFieldSchema> = {
        "FIELD_NAME": "fieldName",
        "LABEL": "label",
        "FIELD_TYPE": "fieldType",
        "FIELD_ORDER": "fieldOrder",
        "FORMAT_CODE": "formatCode",
        "CATEGORICAL": "categorical",
        "BUSINESS_KEY": "businessKey",
        "NOT_NULL": "notNull"
    }

    // The CSV parse options mean that the values come back as strings, so we convert here. Given that
    // the CSV can have extra spaces in its GitHub content this is better than asking the parser
    // to try and convert as we'd have to check it anyway for the spaces.
    let fieldOrders: number[] = []

    // Process each row in the array from the converted string
    let csvData: trac.metadata.IFieldSchema[] = data.data.map(row => {

        // We are going to create a revised row to put in the schema definition that has any issues fixed
        let newRow: DataRow = {}

        // For each column or property in the csv
        Object.entries(row).forEach(([key, value]) => {

            // Get the equivalent property name for the column e.g. 'FIELD_NAME' -> 'fieldName'
            const newKey = schemaNameLookup[key.trim().toUpperCase()]

            // If the CSV has " in it and a space then these can come back as " \"MONTH\"" so we need to remove the
            // extra quotes
            let newValue: DataValues = value === null ? null : value.toString().trim().replace(/"/g, "")
            newValue = newValue === "" ? null : newValue

            // Convert the boolean strings
            if (["categorical", "businessKey", "notNull"].includes(newKey)) {
                newValue = newValue?.toUpperCase() === "TRUE" ? true : newValue?.toUpperCase() === "FALSE" ? false : null
            }

            // Get the field order as an integer
            if (["fieldOrder"].includes(newKey) && typeof newValue === "string") {

                newValue = convertStringToInteger(newValue)
                if (isNaN(newValue)) {
                    newValue = null
                }

            } else if (["fieldOrder"].includes(newKey)) {
                // Handle the case were the value is not a string
                newValue = null
            }

            // Convert the string version of the field type to its enum 'STRING' -> 4
            if (["fieldType"].includes(newKey) && typeof newValue === "string") {

                const newValueUpper = newValue.toUpperCase()

                newValue = isTracBasicTypeString(newValueUpper) ? convertStringToBasicType(newValueUpper) : null

            } else if (["fieldType"].includes(newKey)) {

                newValue = null
            }

            // Label can just be the string or null
            if (newKey) {
                newRow[newKey] = newValue
            }

            if (!newRow.hasOwnProperty("categorical") || newRow.categorical == null) {
                newRow.categorical = false
            }

            if (!newRow.hasOwnProperty("businessKey") || newRow.businessKey == null) {
                newRow.businessKey = false
            }

            if (!newRow.hasOwnProperty("notNull") || newRow.notNull == null) {
                newRow.notNull = false
            }

            // This is a trick for later on resorting the dataset into the same order as defined by the field order
            if (newKey === "fieldOrder" && typeof newValue === "number") {
                fieldOrders.push(newValue)
            }
        })

        return newRow
    })

    fieldOrders = fieldOrders.sort()

    // Here we are filling in blank field orders with their position of in the list when not defined
    let numberOfFieldOrderRemaps: number = 0
    csvData = csvData.map(row => {

        const index = fieldOrders.findIndex(fieldOrder => fieldOrder === row.fieldOrder)

        if (row.fieldOrder != null && index > -1) {

            row.fieldOrder = index

        } else {

            row.fieldOrder = (fieldOrders.length) + numberOfFieldOrderRemaps
            numberOfFieldOrderRemaps = numberOfFieldOrderRemaps + 1
        }

        return row
    })

    // We create a set of error messages to show the user if there is an issue found
    // The returned schema array should not be used if there is an error message
    const errorMessages: string[] = []

    // It is possible that the user could accidentally pass a very large
    // dataset rather than a schema csv to this function, so we try and
    // protect against processing that if there are large numbers of errors.
    for (const [i, row] of csvData.entries()) {

        if (errorMessages.length === maximumNumberOfErrors) break;

        const keys = Object.keys(row)

        if (errorMessages.length < maximumNumberOfErrors) {

            if (!keys.includes("fieldName")) {
                errorMessages.push(`Row ${i + 1} does not contain a field name`)
            } else if (row.fieldName == null) {
                errorMessages.push(`Row ${i + 1} has an empty field name`)
            }
        }

        if (errorMessages.length < maximumNumberOfErrors) {
            if (!keys.includes("fieldType")) {
                errorMessages.push(`Row ${i + 1} does not contain a field type`)
            } else if (row.fieldType == null) {
                errorMessages.push(`Row ${i + 1} has an empty field type`)
            }
        }
    }

    if (errorMessages.length === maximumNumberOfErrors) {
        errorMessages.push("...")
    }

    return {fields: csvData, errorMessages}
}