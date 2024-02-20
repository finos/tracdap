/**
 * A group of utilities for parsing JSON data and guessing the schema, these are tested in tests/utils/utils_schema_guessing.test.ts.
 * These functions are most closely related to the {@link FileImportModal} component but are exported for general use.
 * TODO add tests
 * @category Utils
 * @module SchemaGuessingUtils
 */

import {commasAndAnds, convertDataBetweenTracTypes, convertDateObjectToIsoString, isoDatetimeNoZone, removeIsoZone, standardiseStringArray} from "./utils_general";
import {convertBasicTypeToString} from "./utils_trac_metadata";
import {convertKeyToText, convertStringToBasicType, getDateFormatFromString, isValidNumberAsStringWithCommas} from "./utils_string";
import {DataRow, DataValues, GuessedVariableTypes, GuessVariableType, ImportedFileSchema} from "../../types/types_general";
import dateFnsParse from "date-fns/parse";
import formatISO from "date-fns/formatISO";
import {General} from "../../config/config_general";
import {getAdditionalVariablesInData} from "./utils_schema";
import {hasOwnProperty, isDateObject, isDefined, isTracBoolean, isTracDateOrDatetime, isTracNumber, isTracString} from "./utils_trac_type_chckers";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * A function that takes a variable and guesses what type it may be. For example, even though it is a string
 * it could be "1.2" and therefore be a float, or "2023-01-24" could be a date. This is used when loading data
 * from a csv or Excel file where the loaded data is all passed back as strings.
 *
 * @param variable - The variable to guess the type of.
 */
export const guessVariableType = (variable: DataValues | Date): GuessVariableType => {

    // inFormat here refers to how the variable is stored, NATIVE means that it is the native Javascript type so
    // a number is stored as 1.2 instead of "1.2"

    // First we test the non-string options
    if (variable === null || variable === undefined) {

        return {inFormat: "NATIVE", type: "NULL"}
    }

    // Deal with the case where we get a date object
    if (isDateObject(variable)) {

        if (variable.getMinutes() === 0 && variable.getSeconds() === 0) {
            return {inFormat: "NATIVE", type: "DATE"}
        } else {
            return {inFormat: "NATIVE", type: "DATETIME"}
        }
    }

    if (variable === true || variable === false || (typeof variable === "string" && (variable.toLowerCase() === "true" || variable.toLowerCase() === "false"))) {

        return {inFormat: "NATIVE", type: "BOOLEAN"}
    }

    if (typeof variable === 'number') {

        if (Number.isInteger(variable)) {
            return {inFormat: "NATIVE", type: "INTEGER"}
        } else {
            return {inFormat: "NATIVE", type: "FLOAT/DECIMAL"}
        }

    } else {

        // Now check through the type hierarchy since we know it's a string
        if (variable === "") {

            return {inFormat: "NATIVE", type: "NULL"}

        } else if (["TRUE, FALSE"].includes(variable.toUpperCase())) {

            return {inFormat: "STRING", type: "BOOLEAN"}

        } else if (isValidNumberAsStringWithCommas(variable)) {

            // If the variable is a string and is in scientific notation then parseInt will fail to
            // provide the right number whereas parseFloat will. This means that any value that is
            // in scientific notation will always be seen as a float/decimal.
            if (parseFloat(variable) === parseInt(variable)) return {inFormat: "STRING", type: "INTEGER"};
            else return {inFormat: "STRING", type: "FLOAT/DECIMAL"};

        } else if (getDateFormatFromString(variable).inFormat) {

            return getDateFormatFromString(variable);

        } else {

            return {inFormat: "STRING", type: "STRING"}
        }
    }
}
/**
 * A function that takes the guessed variable type and format for a value in a column in a dataset and adds it to the results
 * collated across all the values for that column. It de-duplicates the options and handles floats and decimals which are
 * grouped together.
 *
 * @param key - The name of the variable in the dataset.
 * @param type - The type and format guessed for the variable's value. This comes from the {@link guessVariableType} function.
 * @param variableTypes - The collated types and formats for the variable so far.
 */
export const addGuessedTypeToOptions = (key: string, type: GuessVariableType, variableTypes: GuessedVariableTypes): GuessedVariableTypes => {

    // Add the found variable type to the list of types found in the column, we have to deal with the FLOAT/DECIMAL types differently
    if (type.type !== "FLOAT/DECIMAL" && !variableTypes[key].types.found.includes(type.type)) {

        variableTypes[key].types.found.push(type.type)

    } else if (type.type === "FLOAT/DECIMAL" && !variableTypes[key].types.found.includes("FLOAT") && !variableTypes[key].types.found.includes("DECIMAL")) {

        variableTypes[key].types.found = variableTypes[key].types.found.concat(["FLOAT", "DECIMAL"])
    }

    // Add the found 'inFormat' to the list of types found in the column, do not pust null values into the list
    if (type.inFormat && !Array.isArray(type.type) && !variableTypes[key].inFormats.found.includes(type.inFormat)) {
        variableTypes[key].inFormats.found.push(type.inFormat)
    }

    return variableTypes
}

/**
 * A function that takes all the type and format options identified for each variable in a dataset and adds in a recommendation
 * for each. So if a column was found to consist only of integers and nulls then we would recommend integer as the type, but
 * string and floats would also be valid options.
 *
 * @param variableTypes - The collated types and formats for every variable across the whole dataset (or whatever was processed).
 */
export const makeFinalTypeRecommendation = (variableTypes: GuessedVariableTypes): GuessedVariableTypes => {

    Object.keys(variableTypes).forEach(key => {

        // We remove nulls as we don't care about these - we can't use them to set the fieldTpe
        // But we only get rid of them for columns where another data type has been identified
        // as then if we have a single data type of NULL we know that the whole column is null.
        const foundTypes = variableTypes[key].types.found.filter(type => variableTypes[key].types.found.length === 1 || (variableTypes[key].types.found.length > 1 && "NULL" !== type))

        // Update what we have in the guessed types
        variableTypes[key].types.found = foundTypes

        // If after the filtering of NULL values we have a single found type of type NULL then recommend all types, we
        // don't have any information about what the right type should be

        // Note that the order that items are pushed to recommended in the guessVariableTypes function matters
        // as the option at the start is used as the default and best option.
        if (foundTypes.length === 1 && foundTypes[0] === "NULL") {

            // String is added first to make it the default option selected for the schema
            variableTypes[key].types.recommended.push(trac.STRING) // <- this will be the type selected for the schema
            variableTypes[key].types.recommended.push(trac.INTEGER)
            variableTypes[key].types.recommended.push(trac.FLOAT)
            variableTypes[key].types.recommended.push(trac.DECIMAL)
            variableTypes[key].types.recommended.push(trac.DATE)
            variableTypes[key].types.recommended.push(trac.DATETIME)
            variableTypes[key].types.recommended.push(trac.BOOLEAN)
        }

        // If a numeric column has integers, let it be set as a float or decimal if needed
        else if (foundTypes.length === 1 && foundTypes[0] === "INTEGER") {

            variableTypes[key].types.recommended.push(trac.INTEGER) // <- this will be the type selected for the schema
            variableTypes[key].types.recommended.push(trac.FLOAT)
            variableTypes[key].types.recommended.push(trac.DECIMAL)
        }

            // If a numeric column has both floats/decimals and integers we assume that it is a float, the check on length 3 is to make sure
        // that no booleans, strings, dates etc. were found in the column
        else if (foundTypes.length === 3 && (foundTypes.includes("FLOAT") && foundTypes.includes("DECIMAL")) && foundTypes.includes("INTEGER")) {

            variableTypes[key].types.recommended.push(trac.FLOAT) // <- this will be the type selected for the schema
            variableTypes[key].types.recommended.push(trac.DECIMAL)
        }

        // If a numeric column has both floats/decimals then recommend float
        else if (foundTypes.length === 2 && foundTypes.includes("FLOAT") && foundTypes.includes("DECIMAL")) {

            variableTypes[key].types.recommended.push(trac.FLOAT) // <- this will be the type selected for the schema
            variableTypes[key].types.recommended.push(trac.DECIMAL)
        } else if (foundTypes.length === 1 && foundTypes.includes("DATE")) {

            variableTypes[key].types.recommended.push(trac.DATE)
        } else if (foundTypes.length === 1 && foundTypes.includes("DATETIME")) {

            variableTypes[key].types.recommended.push(trac.DATETIME)
        }

        // If a date/datetime columns don't let them be interchangeable, treat them as date times.
        else if (foundTypes.length === 2 && foundTypes.includes("DATE") && foundTypes.includes("DATETIME")) {

            variableTypes[key].types.recommended.push(trac.DATETIME)
        }

            // It's easy if we only find one type of variable - that's our recommendation. However, if we have dates with multiple
        // formats we can't handle that.
        else if (foundTypes.length === 1 && foundTypes[0] !== "NULL" && !(foundTypes[0] === "DATE" && variableTypes[key].inFormats.found.length > 1)) {

            // This is a bit odd, but it avoids a Typescript error when using map to convert
            variableTypes[key].types.recommended = [convertStringToBasicType(foundTypes[0])]
        }

        // Always add string as a recommendation, dates with multiple date formats found will be treated as strings
        if (!variableTypes[key].types.recommended.includes(trac.STRING)) variableTypes[key].types.recommended.push(trac.STRING)
    })

    return variableTypes
}

/**
 * A function that takes collated types and formats for every variable across the whole dataset (or whatever was processed) and
 * creates a set of messages to show the user about what was found and any issues that the user needs to be aware of.
 *
 * @param variableTypes - The collated types and formats for every variable across the whole dataset (or whatever was processed).
 * @param fileExtension - The file extension of the original file, this is used in messaging.
 */
export const guessedVariableTypeUserMessages = (variableTypes: GuessedVariableTypes, fileExtension: string): { error: string[], warning: string[] } => {

    // Process the results of guessing the variable types into messages to the user in case there were issues found. Issues
    // do not stop the loading of the file.
    let guessedVariableTypeMessages: { error: string[], warning: string[] } = {
        error: [],
        warning: []
    }

    Object.keys(variableTypes).forEach(fieldName => {

        // A little destructuring to make the code shorter to read
        const {types: {found, recommended}, inFormats} = variableTypes[fieldName]
        const recommendationString = convertBasicTypeToString(recommended[0])

        if (found.length === 0) {

            // Don't think that this can actually happen but just in case
            guessedVariableTypeMessages.warning.push(`No valid data types could be identified for column '${fieldName}' so ${recommendationString} was assumed`)

        } else if (found.length === 1 && found[0] === "NULL") {

            guessedVariableTypeMessages.warning.push(`The '${fieldName}' column was empty so no type could be identified, so ${recommendationString} was assumed`)

        } else if (found.length > 1 && ((found.length === 2 && found.includes("FLOAT") && found.includes("DECIMAL")) || (found.length === 3 && found.includes("INTEGER") && found.includes("FLOAT") && found.includes("DECIMAL")))) {

            // This is fine - adding a specific clause earlier in the chain helps with the logic below
            // This is when the only thing is found is numbers

        } else if (found.length === 2 && found.includes("DATE") && found.includes("DATETIME") && inFormats.found.length === 2 && inFormats.found.includes("NATIVE")) {

            // This is fine - adding a specific clause earlier in the chain helps with the logic below
            // This is when there are only dates, but they are a mix of dates and date times

        } else if ((found.includes("DATE") || found.includes("DATETIME")) && ((inFormats.found.length > 1 && !inFormats.found.includes("NATIVE")) || (inFormats.found.length > 2 && inFormats.found.includes("NATIVE")))) {

            guessedVariableTypeMessages.error.push(`Multiple date formats were found in column '${fieldName}', date columns must be in a single format. The identified formats were ${commasAndAnds(inFormats.found.filter(inFormat => inFormat !== "NATIVE"))}.`)

        } else if (found.length > 1) {

            // If we get to here we know that there is a general issue, and we had to map to string by default
            guessedVariableTypeMessages.warning.push(`Multiple data types were found in column '${fieldName}', ${recommendationString} was assumed`)
        }
    })

    // Add the header to the list of messages
    if (guessedVariableTypeMessages.warning.length > 0) guessedVariableTypeMessages.warning.unshift(`The following warnings were found importing the ${fileExtension} file:`)
    if (guessedVariableTypeMessages.error.length > 0) guessedVariableTypeMessages.error.unshift(`The following errors were found importing the ${fileExtension} file:`)

    return guessedVariableTypeMessages
}

export const calculateImportFieldsLookup = (schema: ImportedFileSchema[] | trac.metadata.IFieldSchema[], fieldNamesInData: string[]): Record<string, { jsonName: undefined | string, upperCase: undefined | string }> => {

    // This is lookup between the fields in the schema and which variable we think that they match to the in the dataset,
    // when a schema is provided rather than guessed at it is possible that the schema and the data are different in case.
    // This lookup provides that matching lookup that allows us to still use the data.
    const fieldsLookup: Record<string, { jsonName: undefined | string, upperCase: undefined | string }> = {}

    schema.forEach(field => {

        if (field.fieldName) {
            const jsonName = fieldNamesInData.find(fieldName => fieldName.toUpperCase() === field.fieldName?.toUpperCase())
            fieldsLookup[field.fieldName] = {jsonName, upperCase: jsonName?.toUpperCase()}
        }
    })

    return fieldsLookup
}

/**
 * A function that validates a schema for a dataset that has been loaded from a csv or Excel file against what is expected. The
 * expected schema is the 'allowedSchema' argument whereas the potential schema options is in 'guessedVariableTypes'. This
 * function checks that all the required fields are available, and they are of the right type.
 *
 * @param allowedSchema - The schema that is expected for the dataset.
 * @param variableTypes - The collated types and formats for every variable across the whole dataset (or whatever was processed).
 * @param fieldNames - The array of field names found in the dataset.
 */
export const validateAllowedSchema = (allowedSchema: ImportedFileSchema[], variableTypes: GuessedVariableTypes, fieldNames: string[]): { importedSchema: ImportedFileSchema[], additionalVariables: string[], differentCaseVariables: string[], missingVariables: string[], differentTypeVariables: [string, trac.BasicType][] } => {

    // This is lookup between the fields in the schema and which variable we think that they match to the in the dataset,
    // when a schema is provided rather than guessed at it is possible that the schema and the data are different in case.
    // This lookup provides that matching lookup that allows us to still use the data.
    const fieldsLookup = calculateImportFieldsLookup(allowedSchema, fieldNames)

    const matchingJsonNamesAsUppercase = Object.values(fieldsLookup).map(lookup => lookup.upperCase).filter(isDefined)

    // Remove any fields in the imported csv/xlsx workbook that are not in the allowed schema
    const importedSchema: ImportedFileSchema[] = allowedSchema.filter(field =>

        field.fieldName != null && matchingJsonNamesAsUppercase.includes(field.fieldName.toUpperCase())).map(field => {

            if (field.fieldName == null) return field

            // A little destructuring to make the code shorter to read
            // 'variableTypes' has already been remapped to have the names in the schema
            const {inFormats} = variableTypes[field.fieldName]

            let newField: ImportedFileSchema = {...field}

            // So for dates we will know what the string format is that we need to use to parse the date string
            // This can contain NATIVE and the format to use, so we have to ensure that we store the format
            if (isTracDateOrDatetime(newField.fieldType) && inFormats.found.length === 1) {

                // There will either not be a conversion as all the dates are Javascript date objects (NATIVE or
                // all the dates were in s single format
                newField.inFormat = inFormats.found[0]

            } else if (isTracDateOrDatetime(newField.fieldType) && inFormats.found.length > 1) {

                // There will be a conversion but only some dates will need conversion
                newField.inFormat = inFormats.found.find(item => item !== "NATIVE")
            }

            // If the field name for the schema is not the same as the name in the data we need to add in the ability to map
            // it across. e.g. If the allowed schema has a field name 'A' but the dataset contains 'a', we need to say that when
            // uploading the data we should use 'a' to access the right data.
            if (field.fieldName !== fieldsLookup[field.fieldName].jsonName) {
                newField.jsonName = fieldsLookup[field.fieldName].jsonName
            }

            return newField
        }
    )

    // So do we have all the variables needed in the allowed schema, if not then we message the user
    const missingVariables = Object.entries(fieldsLookup).filter(([, lookup]) => lookup.jsonName === undefined).map(([key]) => key)

    // So what extra variables do we have
    const additionalVariables = getAdditionalVariablesInData(allowedSchema, fieldNames)

    // Fields that are of the wrong case
    const differentCaseVariables = Object.entries(fieldsLookup).filter(([key, lookup]) => lookup.jsonName !== key).map(([, lookup]) => lookup.jsonName).filter(isDefined)

    // Calculate which variables in the loaded dataset do not have matching types in the allowed schema,
    // then convert to a tuple
    const differentTypeVariables: ([string, trac.BasicType])[] = []

    importedSchema.forEach(variable => {

        if (variable.fieldName != null && variable.fieldType != null && !variableTypes[variable.fieldName].types.recommended.includes(variable.fieldType)) {
            differentTypeVariables.push([variable.fieldName, variable.fieldType])
        }
    })

    return {importedSchema, missingVariables, additionalVariables, differentCaseVariables, differentTypeVariables}
}

/**
 * A function that creates a guess at a schema for a dataset that has been loaded from a csv or Excel file. We do this when there
 * is no schema defined that the dataset must adhere to.
 *
 * @param variableTypes - The collated types and formats for every variable across the whole dataset (or whatever was processed).
 * @param fieldNames - The array of field names found in the dataset.
 */
export const generateGuessedSchema = (variableTypes: GuessedVariableTypes, fieldNames: string[]): trac.metadata.IFieldSchema[] => {

    return fieldNames.map((field, i) => {

        let schema: ImportedFileSchema = {
            fieldName: field,
            fieldOrder: i,
            categorical: false,
            businessKey: false,
            label: convertKeyToText(standardiseStringArray(field)),
            fieldType: variableTypes[field].types.recommended[0]
        }

        if (schema.fieldName != null && hasOwnProperty(variableTypes, schema.fieldName)) {

            // A little destructuring to make the code shorter to read
            const {types: {recommended}, inFormats} = variableTypes[schema.fieldName]

            // If we only found integers and not floats and decimals then format as an integer. We have to check
            // found because we automatically add float and integer into the recommended types in this case

            // Note that the order that items are pushed to recommended in the guessVariableTypes function matters
            // as the option at the start is used as the default and best option.
            if (recommended[0] === trac.INTEGER) {

                schema.formatCode = General.defaultFormats.integer

            } else if (recommended[0] === trac.FLOAT) {

                schema.formatCode = General.defaultFormats.float

            } else if (recommended[0] === trac.DECIMAL) {

                schema.formatCode = General.defaultFormats.decimal

            } else if (recommended[0] === trac.DATE) {

                schema.formatCode = General.defaultFormats.date

            } else if (recommended[0] === trac.DATETIME) {

                schema.formatCode = General.defaultFormats.datetime
            }

            // So for dates we will know what the string format is that we need to use to parse the date string
            // This can contain NATIVE and the format to use, so we have to ensure that we store the format
            if (isTracDateOrDatetime(schema.fieldType) && inFormats.found.length === 1) {

                // There will either not be a conversion as all the dates are Javascript date objects (NATIVE or
                // all the dates were in s single format
                schema.inFormat = variableTypes[schema.fieldName].inFormats.found[0]

            } else if (isTracDateOrDatetime(schema.fieldType) && inFormats.found.length > 1) {

                // There will be a conversion but only some dates will need conversion
                schema.inFormat = variableTypes[schema.fieldName].inFormats.found.find(item => item !== "NATIVE")
            }
        }

        return schema
    })
}

/**
 * A function that goes through each row and each column up to a maximum number of rows and guesses what type of variables
 * are in the columns. It creates arrays of the found types of data in each column as well as and format information found,
 * depending on how complex the column data types are a recommended type and format is set.
 *
 * @param data - The dataset to assess.
 * @param fieldNames - The names of the fieldNames in the dataset.
 * @param rowLimit - The maximum number of rows to parse.
 * @returns An object containing the guessed types as a string and the format that were found across all columns and the
 * sample rows.
 */
export const guessVariableTypes = (data: DataRow[] | Record<string, DataValues | Date>[], fieldNames: string[], rowLimit: number = 10000): GuessedVariableTypes => {

    // Set up the variable that we are going to send back
    let variableTypes: GuessedVariableTypes = {}

    // We want to get the output object by the uppercase version of the key in the
    // data so that we don't have to pass both the original (potentially mixed case)
    // and the uppercase version around the application downstream of loading data.
    // Downstream we want to have a single key that we know will work.
    fieldNames.forEach(key => variableTypes[key] = {
        types: {found: [], recommended: []},
        inFormats: {found: []}
    })

    // If the data is empty we still need to allow the user to upload this dataset. So we send back
    // a guessed schema that consists of all the types.
    if (data.length === 0) {

        fieldNames.forEach(key => {
            variableTypes[key].types.found = ["NULL"]
        })
    }

    // Now parse the data looking for what data types we find
    for (let i = 0; i < Math.min(rowLimit, data.length); i++) {

        // We assume all rows have all keys
        fieldNames.forEach(fieldName => {

            const type = guessVariableType(data[i][fieldName])

            variableTypes = addGuessedTypeToOptions(fieldName, type, variableTypes)
        })
    }

    return makeFinalTypeRecommendation(variableTypes)
}

export const convertRowToGuessedSchema = (row: DataRow, schema: ImportedFileSchema[]) => {

    let newRow: DataRow = {}

    schema.forEach(field => {

        const {fieldName, inFormat, jsonName} = field

        if (fieldName == undefined) return

        // jsonName is present when the name of the properties in the JSON differ to the required schema
        // only by case, so here we have to pick out the variable from the json using the name it has
        // but add it back into newRow using the name in the schema.
        let value = row[jsonName ?? fieldName]

        if (value === null || value === "") {

            newRow[fieldName] = null

        } else {

            if (typeof value === "string") {

                if (isTracNumber(field.fieldType) || isTracBoolean(field.fieldType)) {

                    newRow[fieldName] = convertDataBetweenTracTypes(trac.STRING, field.fieldType, value)

                } else if (field.fieldType === trac.DATE && inFormat) {

                    // Convert the variable out of its format
                    const dateValue = dateFnsParse(value, inFormat, new Date(1900, 0, 1))
                    newRow[fieldName] = convertDateObjectToIsoString(dateValue, "dateIso")

                } else if (field.fieldType === trac.DATETIME && inFormat) {

                    // When loading datetime values in columns of data they must be in UTC format
                    let dateValue

                    // Convert the variable out of its format, strings with a format instead of being isoDatetime
                    // strings are handled differently
                    if (inFormat === "isoDatetime") {

                        // Convert the variable out of its format into a UTC isoDatetime string
                        dateValue = convertDateObjectToIsoString(new Date(value), "datetimeIso")

                    } else {

                        dateValue = dateFnsParse(value, inFormat, new Date(1900, 0, 1))
                        dateValue = convertDateObjectToIsoString(dateValue, "datetimeIso")
                    }

                    // Remove the 'Z' off the isoDatetime string as that can not be sent in the encoding
                    newRow[fieldName] = dateValue.substring(0, dateValue.length - 1)

                } else {

                    newRow[fieldName] = value
                }

            } else if (isDateObject(value)) {

                if (field.fieldType === trac.DATE) {

                    newRow[fieldName] = convertDateObjectToIsoString(value, "dateIso")

                } else if (field.fieldType === trac.DATETIME || isTracString(field.fieldType)) {

                    const dateValue = convertDateObjectToIsoString(value, "datetimeIso")

                    // Remove the 'Z' off the isoDatetime string as that can not be sent in the encoding
                    newRow[fieldName] = dateValue.substring(0, dateValue.length - 1)
                }

            } else if (isTracString(field.fieldType)) {

                newRow[fieldName] = value.toString()

            } else if (isDateObject(value)) {

                // This is a catch all that no Date objects are left in the resultant dataset, then
                // Typescript will assert that the dataset type does indeed not include Dates
                newRow[fieldName] = null

            } else {

                // Already in the correct format.
                newRow[fieldName] = value
            }
        }
    })

    return newRow
}

export function normalizeRowToSchema(row: DataRow, schema: ImportedFileSchema[], quoteDates: boolean = false): DataRow {

    const normalizedRow: DataRow = {}

    for (let i = 0; i < schema.length; i++) {

        const field = schema[i];

        // When the user is using a predefined schema then the name for the variable might be different to what
        // is in the csv loaded, for example the case could be different. If this is the case here then the 'jsonName'
        // value will be the name of the variable to send over the wire.
        const jsonName = field.jsonName ?? field.fieldName;

        if (jsonName === undefined || jsonName === null)
            throw new Error("Invalid data during conversion");

        const rawValue = row[jsonName];
        normalizedRow[jsonName] = normalizeValueToSchema(rawValue, field, quoteDates);
    }

    return normalizedRow;
}


// Normalization is based on the logic in convertRowToGuessedSchema and convertDataBetweenTracTypes
// It should do the same conversion for every combination of fieldType and typeof value
// These conversions run in the inner loop for data processing, they are performance-critical
export function normalizeValueToSchema(value: DataValues | Date | undefined, field: ImportedFileSchema,  quoteDates: boolean): any {

    if (value == null) {
        return null
    }

    if (value === "") {
        if (isTracString(field.fieldType))
            return ""
        else
            return null
    }

    switch (field.fieldType) {

        case trac.BOOLEAN:

            if (value === true || value === false)
                return value;

            if (typeof value === "string") {
                const strValue = value.toLowerCase();
                if (strValue === "true")
                    return true;
                if (strValue === "false")
                    return false;
                return null;
            }

            break;

        case trac.INTEGER:

            if (Number.isInteger(value))
                return value;

            // parseInt will always round down, Math.round will round to nearest
            if (typeof value === "string") {
                const floatValue = Number.parseFloat(value);
                return Number.isNaN(floatValue) ? null : Math.round(floatValue);
            }

            break;

        case trac.FLOAT:

            if (typeof value === "number")
                return value;

            if (typeof value === "string") {
                const floatValue = Number.parseFloat(value);
                return Number.isNaN(floatValue) ? null : floatValue;
            }

            break;

        case trac.DECIMAL:

            if (typeof value === "number")
                return value;

            // Note: For large decimal values, loss of precision or overflow can occur

            if (typeof value === "string") {
                const floatValue = Number.parseFloat(value);
                return Number.isNaN(floatValue) ? null : floatValue;
            }

            break;

        case trac.STRING:

            if (typeof value === "string")
                return value;

            else
                return value.toString();

        case trac.DATE:

            if (value instanceof Date) {
                return quoteDates ? formatISO(value, {representation: 'date'}) : value
            }

            if (typeof value === "string" && field.inFormat) {

                // Save time on parse / quote if the data is already in ISO format
                if (quoteDates && field.inFormat === "yyyy-MM-dd")
                    return value;

                // These functions are expensive, only call them if really needed
                const dateValue = dateFnsParse(value, field.inFormat, new Date(1900, 0, 1))
                return quoteDates ? formatISO(dateValue, {representation: 'date'}) : dateValue;
            }

            break;

        case trac.DATETIME:

            if (value instanceof Date)
                return quoteDates ? isoDatetimeNoZone(value) : value;

            if (typeof value === "string" && field.inFormat) {

                // Save time on parse / quote if the data is already in ISO format
                if (quoteDates && field.inFormat === "isoDatetime") {
                    return removeIsoZone(value);
                }

                // These functions are expensive, only call them if really needed
                const datetimeValue = field.inFormat === "isoDatetime"
                    ? new Date(value)
                    : dateFnsParse(value, field.inFormat, new Date(1900, 0, 1))
                return quoteDates ? isoDatetimeNoZone(datetimeValue) : datetimeValue;
            }

            break;

        default:
            break;
    }

    // Fall-through default
    // If we haven't been able to match and normalize the type, pass it through as is
    // To be strict we could throw an error, that might result in more breaks rather than less...

    return value;
}