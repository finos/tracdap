import {hasOwnProperty, isDateFormat, isDefined, isGroupOption, isTracDateOrDatetime, isTracNumber} from "./utils_trac_type_chckers";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {DataRow, DateFormat, DatetimeFormat, Group, GuessedVariableTypes, ImportedFileSchema, NumberFormatAsArray, Option, SchemaDetails, ValidationOfModelInputs} from "../../types/types_general";
import {Types} from "../../config/config_trac_classifications";
import {General} from "../../config/config_general";
import {convertKeyToText} from "./utils_string";
import {arraysOfObjectsEqualByKey, arraysOfPrimitiveValuesEqual, sortArrayBy} from "./utils_arrays";
import {convertBasicTypeToString, setLabel} from "./utils_trac_metadata";
import {convertNumberFormatCodeToArray, isValidNumberFormatCode} from "./utils_formats";
import {guessVariableTypes} from "./utils_schema_guessing";

/**
 * A function that takes a TRAC schema and creates an empty row, used when adding new rows to datasets. When called the
 * type of the returned object is passed so that typescript understands what the type of the object returned is, for example
 * const emptyRow = createEmptyRow<UiEditableRow>(schema.table.fields).
 * @param schema - The TRAC schema for the dataset.
 */
export const createEmptyRow = <T extends Record<string, any>>(schema: trac.metadata.IFieldSchema[]): T => {

    let emptyRow: Record<string, null> = {}

    schema.forEach(field => {

        if (typeof field.fieldName === "string") {
            emptyRow[field.fieldName] = null
        }
    })

    return emptyRow as T
}

/**
 * A function that checks whether two schemas are the same without the order of the variables mattering (note the order
 * property matters but not the order of the items in the array). This check can be performed across every property
 * of the schemas (e.g. label, order, businessKey) or just those that are non-editable. In the latter case if two
 * schemas are identical in their non-editable properties but not their editable ones then that means that the user
 * could edit them to be completely identical.
 * @param a1 - The first schema to compare.
 * @param a2 - The second schema to compare.
 * @param checkOnlyNonEditable - Whether to only include the non-editable fields in the schema. If this is false
 * then every property will be compared including the formats for example. When off only the properties that can not
 * be edited will be compared, the meaning being that the schemas could be edited to match if this check returns true.
 */
export function areSchemasEqual(a1: trac.metadata.IFieldSchema[], a2: trac.metadata.IFieldSchema[], checkOnlyNonEditable: boolean = true): boolean {

    // Check the lengths and the names of the variables, this is the quickest check
    const fileNamesSame = arraysOfPrimitiveValuesEqual(a1.map(variable => variable.fieldName), a2.map(variable => variable.fieldName))

    if (!fileNamesSame) return false

    let areSchemasTheSame: boolean

    const nonEditableFields = ["fieldType", "fieldName", "fieldOrder", "businessKey", "categorical"]

    if (checkOnlyNonEditable) {

        // Keep only the non-editable fields in the schema and check against these
        const reducedSchema1: trac.metadata.IFieldSchema[] = a1.map(item => {

            // By default, when objects come over the wire protobuf removes the default values to make the
            // amount of information transmitted smaller. So false categorical, and 0 fieldOrder values will not be in
            // the  schema. To compensate we fill these missing values here to make the comparison fair. For example
            // when comparing a schema from TRAC to a schema that we have build locally in the app.
            let newItem: trac.metadata.IFieldSchema = fillSchemaDefaultValues(item)

            Object.keys(item).forEach(key => {
                if (!nonEditableFields.includes(key) && hasOwnProperty(newItem, key)) delete newItem[key]
            })

            return newItem
        })

        // Keep only the non-editable fields in the schema and check against these
        const reducedSchema2: trac.metadata.IFieldSchema[] = a2.map(item => {

            let newItem: trac.metadata.IFieldSchema = fillSchemaDefaultValues(item)

            Object.keys(item).forEach(key => {
                if (!nonEditableFields.includes(key) && hasOwnProperty(newItem, key)) delete newItem[key]
            })

            return newItem
        })

        areSchemasTheSame = arraysOfObjectsEqualByKey(reducedSchema1, reducedSchema2, "fieldName")

    } else {

        areSchemasTheSame = arraysOfObjectsEqualByKey(a1, a2, "fieldName")
    }

    return areSchemasTheSame
}

/**
 * A function that adds the  default values of a schema field if  they are missing.
 *
 * By default, when objects come over the wire protobuf removes the default values to make the
 * amount of information transmitted smaller. So false categorical, and 0 fieldOrder values will not be in
 * the  schema. To compensate we fill these missing values here to make the comparison fair. For example
 * when comparing a schema from TRAC to a schema that we have build locally in the app.
 *
 * @param item - The schema field item.
 */
export const fillSchemaDefaultValues = (item: trac.metadata.IFieldSchema): trac.metadata.IFieldSchema => {

    const fieldSchemaShim = {fieldOrder: 0, categorical: false, businessKey: false, notNull: false}

    return {...fieldSchemaShim, ...item}
}

export function areSchemasEqualButOneHasExtra(currentSchema: trac.metadata.IFieldSchema[], newSchema: trac.metadata.IFieldSchema[], checkOnlyNonEditable: boolean = true): boolean {

    // We can only add fields in an update never take away
    if (currentSchema.length > newSchema.length) return false

    // Get a list of the fields that are in both schemas
    const fieldsInBoth = currentSchema.map(variable => variable.fieldName).filter(fieldName => newSchema.map(variable => variable.fieldName).includes(fieldName))

    // If the current schema has fields not in both then we can not update using the new
    if (currentSchema.length !== fieldsInBoth.length) return false

    // Keep only the fields in both and check if they are the same, the newSchema can have more fields
    return areSchemasEqual(currentSchema.filter(variable => fieldsInBoth.includes(variable.fieldName)), newSchema.filter(variable => fieldsInBoth.includes(variable.fieldName)))
}

/**
 * A function that checks two schemas to see if the field names and field types match, this is used when checking the
 * inputs selected for a flow or model to run and compare these to the model definitions for the input. It returns an
 * object that contains information about the differences rather than just as boolean result. The first schema a1, is
 * allowed to have more fields than the second, this will not trigger an error.
 */
export function differencesInSchemas(a1: trac.metadata.IFieldSchema[], a2: trac.metadata.IFieldSchema[]): { missing: string[], type: { fieldName: string, want: trac.BasicType, have: trac.BasicType }[] } {

    // Get an array of field names that are in a1 and not in a2, note that TRAC is case-insensitive when loading data, so we need to make
    // case-insensitive comparisons
    const missingVariables = a2.map(variable => variable.fieldName?.toUpperCase()).filter(fieldName => !a1.map(variable => variable.fieldName?.toUpperCase()).includes(fieldName)).filter(isDefined)
    const differentTypes: Omit<ValidationOfModelInputs[keyof ValidationOfModelInputs], "modelKey">["type"] = []

    a2.forEach(a2Variable => {

        // Get the equivalent in a1, note that TRAC is case-insensitive when loading data, so we need to make
        // case-insensitive comparisons
        const a1Variable = a1.find(variable => variable.fieldName?.toUpperCase() === a2Variable.fieldName?.toUpperCase())

        // Are the field types set and if they are but are different, then add it as an error
        if (a1Variable?.fieldName != null && a2Variable.fieldType !== a1Variable?.fieldType && a2Variable.fieldType != null && a1Variable?.fieldType != null) {

            // TRAC allows lossless conversion of field types, so we have one last check to make
            // a1 here MUST be the selectedInputSchema and a2 MUST be the schemaInModelDefinition
            if (!(a1Variable.fieldType == trac.INTEGER && isTracNumber(a2Variable.fieldType))) {
                differentTypes.push({fieldName: a1Variable.fieldName.toUpperCase(), have: a1Variable?.fieldType, want: a2Variable.fieldType})
            }
        }
    })

    return {missing: missingVariables, type: differentTypes}
}

/**
 * A function that converts a dataset schema into a set of options for the variables in the schema.
 *
 * @remarks
 * For information on the function signatures see https://stackoverflow.com/questions/52817922/typescript-return-type-depending-on-parameter
 * The aim is to make the type of the returned value depend on the type of the 'useGroups' argument.
 *
 * @param schema {trac.metadata.IFieldSchema[]} The dataset schema.
 * @param labelIncludesName - Whether
 * @param useGroups - Whether to put the options into groups
 * @param onlyCategorical - Whether to keep only variables that are flagged as categorical or can
 * be classed as categorical.
 */
export function convertSchemaToOptions<T extends boolean>(schema: trac.metadata.IFieldSchema[], useGroups: T, labelIncludesName?: boolean, onlyCategorical?: boolean): T extends true ? Group<string, SchemaDetails>[] : Option<string, SchemaDetails>[]
export function convertSchemaToOptions(schema: trac.metadata.IFieldSchema[], useGroups: boolean, labelIncludesName?: boolean, onlyCategorical?: boolean): Group<string, SchemaDetails>[] | Option<string, SchemaDetails>[] {

    // If useGroups is false we just return a simple array of options
    if (!useGroups) {

        let options: Option<string, SchemaDetails>[] = []

        let newSchema = onlyCategorical === true ? getCategoricalVariables(schema) : schema

        // The forEach rather than map allows us to ensure that the value is not null, the TRAC Typescript says it can be
        newSchema.forEach(variable => {

            if (variable.fieldName != null) {
                options.push({
                    value: variable.fieldName,
                    label: setLabel(variable, labelIncludesName || false),
                    details: {schema: {...variable}}
                })
            }
        })

        return sortArrayBy(options, "label")

    } else {

        // Create a property for each type of data - we are going to filter the schema into these lists
        let lists: Record<string, trac.metadata.IFieldSchema[]> = {}

        Types.tracBasicTypes.forEach(basicType => {
            lists[basicType.value] = []
        })

        // Put the variables into the right properties
        schema.forEach(variable => {
            if (variable.fieldType != null) lists[convertBasicTypeToString(variable.fieldType, false)].push(variable)
        })

        // Convert to the structure to be read as options with headers by the SelectOption component
        let options: Group<string, SchemaDetails>[] = []

        Types.tracBasicTypes.forEach(basicType => {

            let optionsForType = convertSchemaToOptions(lists[basicType.value], false, labelIncludesName, onlyCategorical)

            // The check on isGroupOption is for Typescript to be happy that we are not storing a group inside a group
            if (!isGroupOption(optionsForType)) {
                options.push({label: basicType.label, options: optionsForType})
            }
        })

        return options
    }
}

/**
 * A function that takes a dataset and guesses the schema for it, first by parsing the data and coming up with
 * a range of basic types that match the data and then by using this to define a schema with the recommended
 * field type.
 *
 * @param data {TableRow[]} The data to get the schema for.
 * @param setHumanReadableLabels - Whether to set labels as a humanreadable version of the field name.
 */
export const guessSchema = (data: DataRow[], setHumanReadableLabels: boolean = false): { schema: trac.metadata.IFieldSchema[], guessedVariableTypes: GuessedVariableTypes } => {

    // If there are no rows we can't identify the schema
    if (data.length === 0) return {schema: [], guessedVariableTypes: {}}

    // Guess the variable types, this gives us an object with options for the variable types found
    const guessedVariableTypes = guessVariableTypes(data, Object.keys(data[0]))

    const schema = Object.keys(data[0]).map((field, i) => {

        // There is always a recommended type
        const recommendedFieldType = guessedVariableTypes[field].types.recommended[0]

        // Make an initial stab at the required information
        let item: trac.metadata.IFieldSchema = {
            fieldName: field,
            fieldType: recommendedFieldType,
            fieldOrder: i,
            categorical: false,
            businessKey: false,
            label: setHumanReadableLabels ? convertKeyToText(field) : field
        }

        // Set the format
        if (recommendedFieldType === trac.INTEGER) {

            item.formatCode = General.defaultFormats.integer

        } else if (recommendedFieldType === trac.FLOAT) {

            item.formatCode = General.defaultFormats.float

        } else if (recommendedFieldType === trac.DECIMAL) {

            item.formatCode = General.defaultFormats.decimal

        } else if (recommendedFieldType === trac.DATE) {

            // The guessVariableTypes makes a guess at the format to put DATE strings into
            item.formatCode = General.defaultFormats.date

        } else if (recommendedFieldType === trac.DATETIME) {

            item.formatCode = General.defaultFormats.datetime
        }

        return item
    })

    return {schema, guessedVariableTypes}
}

/**
 * A function that takes a TRAC schema and returns the label for a particular field, a fallback is used if either
 * the variable does not exist or the label is not defined.
 *
 * @param fields - The schema to get the label from.
 * @param fieldName - The name of the field to look for.
 * @param fallback - The fallback label if the fieldName is not found.
 * @returns The label for the variable.
 */
export function getFieldLabelFromSchema(fields: null | undefined | trac.metadata.IFieldSchema[], fieldName: string, fallback: string): Exclude<trac.metadata.IFieldSchema["label"], undefined | null> {

    const fieldSchema = fields?.find(field => field.fieldName === fieldName)
    return fieldSchema === undefined || fieldSchema.label == null ? fallback : fieldSchema.label
}

/**
 * A function that returns an array of all the unique fieldTypes in a dataset.
 * @param fields - The schema to get the fieldTypes from.
 */
export const getUniqueVariableTypes = (fields: trac.metadata.IFieldSchema[]): trac.BasicType[] => (
    [...new Set(fields.map(item => item.fieldType))].filter(isDefined)
)

/**
 * A function that returns an array of all the unique numeric format codes in a dataset.
 * @param fields - The schema to get the fieldTypes from.
 */
export const getUniqueNumberFormatCodes = (fields: trac.metadata.IFieldSchema[]): NumberFormatAsArray[] => (

    [...new Set(getNumberVariables(fields).map(variable => variable.formatCode))].map(format => typeof format === "string" && isValidNumberFormatCode(format) ? convertNumberFormatCodeToArray(format) : undefined).filter(isDefined)
)

/**
 * A function that returns an array of all the unique date or datetime formats codes in a dataset.
 * @param fields - The schema to get the fieldTypes from.
 */
export const getUniqueDatetimeFormatCodes = (fields: trac.metadata.IFieldSchema[]): (DateFormat | DatetimeFormat)[] => (

    [...new Set(getDateAndDatetimeVariables(fields).map(variable => variable.formatCode))].map(format => isDateFormat(format) ? format : undefined).filter(isDefined)
)

/**
 * A function that returns whether all the variables in a schema are numbers.
 * @param fields - The schema to check.
 */
export const fieldsAreAllNumbers = (fields: trac.metadata.IFieldSchema[]): boolean => (

    !fields.some(field => !isTracNumber(field.fieldType))
)

/**
 * A function that returns whether all the variables in a schema are integers.
 * @param fields - The schema to check.
 */
export const fieldsAreAllIntegers = (fields: trac.metadata.IFieldSchema[]): boolean => (

    !fields.some(field => field.fieldType !== trac.INTEGER)
)

/**
 * A function that returns whether all the variables in a schema are dates and datetime fields.
 * @param fields - The schema to check.
 */
export const fieldsAreDatesAndDatetime = (fields: trac.metadata.IFieldSchema[]): boolean => (

    !fields.some(field => !isTracDateOrDatetime(field.fieldType))
)

/**
 * A function that returns whether all the variables in a schema are date fields.
 * @param fields - The schema to check.
 */
export const fieldsAreAllDates = (fields: trac.metadata.IFieldSchema[]): boolean => (

    !fields.some(field => field.fieldType !== trac.DATE)
)

/**
 * A function that returns whether all the variables in a schema are datetime fields.
 * @param fields - The schema to check.
 */
export const fieldsAreAllDatetime = (fields: trac.metadata.IFieldSchema[]): boolean => (

    !fields.some(field => field.fieldType !== trac.DATETIME)
)

export const schemaHasInvalidNumberFormatCodes = (fields: trac.metadata.IFieldSchema[]): boolean => (

    fields.some(item => !isValidNumberFormatCode(item.formatCode))
)

/**
 * A function that returns all the variables in a schema that are categorical fields.
 * @param fields - The schema to filter.
 */
export const getCategoricalVariables = (fields: trac.metadata.IFieldSchema[]): trac.metadata.IFieldSchema[] => {

    // In TRAC only string fields can have categorical flags set, but it is reasonable to be able to group by other variables too,
    // for example booleans. Integers and dates can be either way, but we assume that they should be available to group by.
    const additionalCategoricalTypes = [trac.BOOLEAN, trac.DATE, trac.INTEGER]

    return fields.filter(field => field.hasOwnProperty("categorical") && field.categorical === true || (field.fieldType != null && additionalCategoricalTypes.includes(field.fieldType)))
}

/**
 * A function that returns all the variables in a schema that are dates or datetime fields.
 * @param fields - The schema to filter.
 */
export const getDateAndDatetimeVariables = (fields: trac.metadata.IFieldSchema[]): trac.metadata.IFieldSchema[] => (

    fields.filter(field => field.fieldType && isTracDateOrDatetime(field.fieldType))
)

/**
 * A function that returns all the variables in a schema that are number fields.
 * @param fields - The schema to filter.
 */
export const getNumberVariables = (fields: trac.metadata.IFieldSchema[]): trac.metadata.IFieldSchema[] => (

    fields.filter(field => field.fieldType && isTracNumber(field.fieldType))
)
/**
 * A function that goes through a schema an identifies what additional variables are defined in the in an array of names. This is case-insensitive.
 * This function is used for example when loading data, and you have a pre-defined schema, you may need to know what additional variables are
 * in the data that are not in the schema.
 *
 * @param schema - The schema to test against.
 * @param fieldNamesInData - The array of field names from the data.
 */
export const getAdditionalVariablesInData = (schema: ImportedFileSchema[] | trac.metadata.IFieldSchema[], fieldNamesInData: string[]): string[] => {

    // This is a lookup keyed by the schema variable names with the data name and it's uppercase version stored in an object
    const matchingJsonNamesAsUppercase: string[] = schema.map(field => {

        if (field.fieldName) {

            // See if we can find a field in the dataset that we think matches the schema, case-insensitive
            return fieldNamesInData.find(fieldName => fieldName.toUpperCase() === field.fieldName?.toUpperCase())?.toUpperCase()

        } else {
            return undefined
        }

    }).filter(isDefined)

    // So what extra variables do we have
    return fieldNamesInData.filter(fieldName => !matchingJsonNamesAsUppercase.includes(fieldName.toUpperCase()))
}