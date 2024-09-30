import {
    ButtonPayload,
    DeepWritable,
    Option,
    QueryButton,
    SchemaDetails,
    SelectOptionPayload,
    SelectValuePayload,
    StoreStatus,
    DataRow,
    FullTableState, ConfirmButtonPayload
} from "../../../types/types_general";
import cloneDeep from "lodash.clonedeep";
import {configuration} from "./config_query_builder";
import {createAsyncThunk, createSlice, nanoid, PayloadAction} from "@reduxjs/toolkit";
import {decideWhichSqlToUse} from "../../utils/utils_sql";
import differenceInSeconds from "date-fns/differenceInSeconds";
import {General} from "../../../config/config_general";
import {
    showToast
} from "../../utils/utils_general";
import {guessSchema} from "../../utils/utils_schema";
import {isDefined, isTracNumber} from "../../utils/utils_trac_type_chckers";
import {MultiValue, SingleValue} from "react-select";
import parseISO from "date-fns/parseISO";
import {RootState} from "../../../storeController";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {Types} from "../../../config/config_trac_classifications";
import {toast} from "react-toastify";
import {setWhereClauseValidationChecked, WhereClause} from "../WhereClauseBuilder/whereClauseBuilderStore";
import {getAllRegexMatches, removeLineBreaks} from "../../utils/utils_string";
import {makeArrayOfObjectsUniqueByProperty} from "../../utils/utils_arrays";

// A newline character in a string, used to layout query string in the browser
// https://stackoverflow.com/questions/1761051/difference-between-n-and-r
const newLine = "\r\n"

/**
 * A function that updates a schema for a variable created by the select query based on the function used to create it.
 * For example if a count function is used you know the resulting value will be an integer.
 * @param functionOption {QueryButton} The details about the function used.
 * @param schema {trac.metadata.IFieldSchema} The TRAC schema item to update.
 */
export const updateLabelAndFormatBasedOnFunction = (functionOption: QueryButton, schema: trac.metadata.IFieldSchema): trac.metadata.IFieldSchema => {

    let newSchema = {...schema}

    // strings and booleans don't have a format
    if (functionOption.outputFormat === "NONE") {

        delete newSchema.formatCode

    } else if (functionOption.outputFormat !== "INHERIT") {

        newSchema.formatCode = functionOption.outputFormat
    }

    if (functionOption.outputType !== "INHERIT") {

        newSchema.fieldType = functionOption.outputType
    }

    if (schema.label && functionOption.hasOwnProperty("outputLabelStart")) {

        newSchema.label = `${functionOption.outputLabelStart} ${newSchema.label}`
    }

    if (newSchema.label && functionOption.hasOwnProperty("outputLabelEnd")) {

        newSchema.label = `${newSchema.label} ${functionOption.outputLabelEnd}`
    }

    if (functionOption.outputCategorical !== "INHERIT") {

        newSchema.categorical = functionOption.outputCategorical
    }

    return newSchema
}

/**
 * A function that processes the select query based on what has been set by the user. This creates an array
 * for each term where the first part is the query and the second part is the name for the variable in the result.
 * It also calculates the schema for the new variable. The name for the variable is useful as we can use this
 * function to work out if a variable is already added to the query and if so warn the user.
 * @param added - The 'added' part of the selectTab property of the query.
 * @returns - An array of arrays, the inner array has the SQL term string, the variable for the term and the schema for the term variable.
 */
export const processSelectQuery = (added: (Query["selectTab"][])[]): ([string, string, trac.metadata.IFieldSchema])[] => {

    return added.map(selects => {

        // Calculate the SQL code for the select term
        let term = ""

        selects.forEach((select, i) => {

            if (select.operator) term = `${term}${i === 0 ? "" : " "}${select.operator.value} `
            if (select.aggregation) term = `${term}${select.aggregation.value}(`
            if (select.sqlFunction) term = `${term}${select.sqlFunction.value}(`
            term = `${term}${select.variable?.value}`
            if (select.sqlFunction) term = `${term})`
            if (select.aggregation) term = `${term})`
        })

        // If the select statement has more than one term then the user is using a group e.g. A * B  + C
        // so add a bracket
        if (selects.length > 1) {
            term = `(${term})`
        }

        // Calculate the variable name to assign the variable in the result
        let as: string = ""

        // If the user has created a term using operators use the nanoid version of the
        // variable name rather tty to work it out
        if (selects.length > 1) {

            as = `variable_${selects[0].id}`

        } else {

            if (selects[0].aggregation) as = `${as}${selects[0].aggregation.value}_`
            if (selects[0].sqlFunction) as = `${as}${selects[0].sqlFunction.value}_`
            as = `${as}${selects[0].variable?.value}`
        }

        // Calculate the schema for the result
        let schema: trac.metadata.IFieldSchema = {fieldType: null, fieldName: as}

        // If the user has created a term using operators then since these are
        // restricted to number variables the resulting type must be a float. Use the term
        // as the label
        if (selects.length > 1) {

            schema.fieldType = trac.FLOAT
            schema.label = term

        } else {

            const select = selects[0]

            // Inherit the variable type, format,  label and categorical from the input scenario
            schema.fieldType = select.variable?.details.schema.fieldType || trac.STRING

            if (select.variable?.details.schema.hasOwnProperty("label")) {
                schema.label = select.variable.details.schema.label
            } else {
                schema.label = select.variable?.value || "Unknown"
            }

            if (select.variable?.details.schema.hasOwnProperty("formatCode")) {
                schema.formatCode = select.variable.details.schema.formatCode
            }

            if (select.variable?.details.schema.hasOwnProperty("categorical")) {
                schema.categorical = select.variable.details.schema.categorical
            }

            // Now update the labels, types and format codes based on the functions applied
            // The order matters here - aggregations are applied after functions in the
            // same way as the term expression
            if (select.sqlFunction) {

                const {sqlFunction} = select

                schema = updateLabelAndFormatBasedOnFunction(sqlFunction, schema)
            }

            if (select.aggregation) {

                const {aggregation} = select

                schema = updateLabelAndFormatBasedOnFunction(aggregation, schema)
            }
        }

        return [term, as, schema]
    })
}

/**
 * A function that calculates the full SQL query as a string with \n as line breaks.
 * @param query - The query[objectKey] part of the QueryBuilderStore.
 * @param whereClause - The whereClause[objectKey] part of the QueryBuilderStore.
 * @returns - The full SQL query.
 */
export const calculateQueryString = (query: Query, whereClause: WhereClause): string => {

    // If the variable in the select has no functions applied to it then the term will be the same as the as "BRAND as BRAND".
    // In these cases we drop the as part.
    const selectArray = processSelectQuery(query.selectTab.added)

    // Get a list of all the variables in the groupBy and orderBy statements and get the variable names
    // Then see if any of these variable are in the query as they need to be added if not. If the query is a select *
    // then we do not need to add any additional terms as they will all be on it
    const additionalVariables = [...new Set([...query.groupByTab.variables, ...query.orderByTab.variables].map(variable => variable.value))]

    // Note that we prepend with any additional variables needed to make the groupBy and orderBy variables appear on the output
    // It's the select that is filtered so that all group and order by variables appear at the start
    const selectString = [...additionalVariables.map(variable => [variable, variable]), ...selectArray.filter(select => !additionalVariables.includes(select[1]))].map(select => select[0] === select[1] ? select[0] : `${select[0]} as ${select[1]}`).join(", ")

    const whereClauseString = whereClause.whereTab[0].stringVersion.length > 0 ? `${newLine}where ${whereClause.whereTab[0].stringVersion}` : ""

    const groupByString = `${query.groupByTab.variables.length > 0 ? `${newLine}group by ` : ""}${query.groupByTab.variables.map(variable => variable.value).join(", ")}`

    const isAllOrderingAscending = !Object.entries(query.orderByTab.ordering).some(([, variable]) => variable.value === "DESC")

    const orderByString = query.orderByTab.variables.length > 0 ? `${newLine}order by ` + query.orderByTab.variables.map(variable => `${variable.value}${!isAllOrderingAscending ? ` ${query.orderByTab.ordering[variable.value].value}` : ""}`).join(", ") : ""

    const limitString = `${newLine}limit ${query.limitTab.limit}`

    return `${query.selectTab.unique ? "DISTINCT " : ""}${(selectString === "" ? "*" : selectString) + `${newLine}from dataset ` + whereClauseString + groupByString + orderByString + limitString}`
}

/**
 * A function that calculates the schema for the output from an SQL query. This is deterministic in that since
 * we know the input data schema and the functions applied to it, we can work out the schema of the output schema.
 * @param query {Query} The full SQL query object.
 * @returns {trac.metadata.IFieldSchema[]} The TRAC schema for the query result.
 */
export const getSchemaOfNonUserEditedQuery = (query: Query): trac.metadata.IFieldSchema[] => {

    // Get the processed select query that includes the schemas for the individual terms
    let selectArray = processSelectQuery(query.selectTab.added)

    if (!query.inputData.schema) throw new Error("The data being queried does not have a schema available.")

    // If the select array has zero elements it is a select * and the output schema will be equal to the input one
    if (selectArray.length === 0) return query.inputData.schema

    // Get a list of all the variables in the groupBy and orderBy statements

    let additionalVariables = makeArrayOfObjectsUniqueByProperty([...query.groupByTab.variables, ...query.orderByTab.variables].map(variable => variable.details.schema), "fieldName")

    // Get the schemas for the group and order by variables, create a list of their names
    const additionalVariableNames = [...new Set([...additionalVariables.map(variable => variable.fieldName).filter(isDefined)])]

    // Remove the variables in the selectArray corresponding to the variables in the additional variables,
    // this is because we prepend the additional variables in the calculateQueryString function, and we are
    // trying to make the schema match the order that the variables appear in the returned table
    // The selectArray variable is an array of arrays, the inner array has three values, the last (index =2) is the schema for the variable being created by each term
    const selectArraySchemasWithoutAdditionalVariables = selectArray.map(select => select[2]).filter(select => select.fieldName && !additionalVariableNames.includes(select.fieldName))

    // Note we also reset the order to that in the select statement
    return [...additionalVariables, ...selectArraySchemasWithoutAdditionalVariables].map((variable, i) => ({...variable, fieldOrder: i}))
}

/**
 * A function that guesses the schema for the output from an SQL query, this is for when the user unlocks the query
 * builder interface and edits the SQL directly. When the user edits the SQL we lose track of the functions and
 * variables, so we have to guess what it is.
 *
 * The function does two things, parse the data to establish the data types, and then it tries to parse the select
 * statement to augment this with additional information.
 *
 * @param inputSchema - The schema for the input dataset that the query ran against.
 * @param outputData - The data from the SQL query.
 * @param queryThatRan - The SQL query that ran. This needs to have the 'select' at the start in order to work.
 * @param textButtons - The metadata for the text functions in the store.
 * @param datetimeButtons - The metadata for the date time functions in the store.
 * @param numberButtons - The metadata for the number functions in the store.
 * @param aggregationButtons - The metadata for the aggregation functions in the store.
 */
const guessSchemaOfUserEditedQuery = (inputSchema: null | trac.metadata.IFieldSchema[], outputData: DataRow[], queryThatRan: string, {
    textButtons,
    datetimeButtons,
    numberButtons,
    aggregationButtons
}: typeof configuration) => {

    // Some housekeeping for Typescript
    if (inputSchema == null) throw new Error("The selected dataset does not have a schema to use to guess the output data's schema")

    // Guess the schema
    const {schema: guessedSchema} = guessSchema(outputData)

    // Look at the select statement and try and see what variables it uses
    const foundVariables = inspectSqlSelect(queryThatRan, inputSchema)

    // The inspectSqlSelect function also tries to identify functions applied
    // to a variable in the user set select. We use some configuration
    // we use for the functions in the query builder interface to better guess the
    // schema

    // Get a list of all the functions
    const allFunctions = [...textButtons, ...datetimeButtons, ...numberButtons, ...aggregationButtons]

    // Now go through the input schema and for those variables that we found
    // in the inspectSqlSelect function update the variable schema to reflect what we know. So if we found
    // sum(BALANCE)  we can add 'Sum of' to the label and use the input variable format as the output etc.
    const newSchemaVariables = inputSchema.filter(
        // Only keep variables from the input schema that we found information about in inspectSqlSelect
        variable => variable.fieldName != null && foundVariables.map(found => found.was).includes(variable.fieldName)
    ).map(variable => {

        // For each variable in the guessed schema get the information we found about it the
        // inspectSqlSelect function results
        const details = foundVariables.find(found => found.was === variable.fieldName)

        // TODO can this be triggered and still be valid
        if (!details) throw new Error("An error occurred trying to guess the schema of the output data.")

        // Update the variable name to be the new variable name we found. e.g. if the user wrote
        // 'sum(BALANCE) as NEW_BALANCE' this takes the BALANCE variable schema from the input
        // schema and renames the fieldName to NEW_BALANCE.
        let newVariable: trac.metadata.IFieldSchema = {...variable, fieldName: details.is}

        // Use the variable name as a label if one is not present. This allows us to add additional
        // information about the function used to the label
        if (!newVariable.hasOwnProperty("label")) {
            newVariable.label = newVariable.fieldName
        }

        // If we found a function being applied
        if (details.function) {

            // Is it a function we have metadata for
            const functionOption = allFunctions.find(sqlFunction => sqlFunction.value === details.function)

            // If it is use the metadata to set the label, type and categorical
            if (functionOption) {
                newVariable = updateLabelAndFormatBasedOnFunction(functionOption, newVariable)
            }
        }

        return newVariable
    })

    // Now replace the new variable schemas into the guessed schema
    return guessedSchema.map(
        variable => newSchemaVariables.find(found => found.fieldName === variable.fieldName) || variable
    ).map(
        (variable, i) => ({...variable, fieldOrder: i})
    )
}

/**
 * A function that extracts the select statement from a user edited SQL query and then parses it using regex
 * to see if it can understand what variables it is selecting. Only simple selects (e.g. 'select BALANCE from'
 * or 'select BALANCE as NEW_BALANCE from' or 'select sum(BALANCE) as NEW_BALANCE from' are understood.
 * While this function could be extended to cover more use case such as identifying more functions it doesn't
 * because SQL can be complex adding more functionality will increase the likelihood of errors and bugs.
 *
 * The list of identified variables is used after this function to set a schema for the dataset created by the
 * SQL query where the identified variables can have their schema set as the same as that on the input dataset.
 * @param sqlQuery - The  SQL query edited by the user.
 * @param originalSchema - The schema of the original dataset that the query will run against.
 * @returns - The array of variables that have been found in the select part of the user edited SQL query.
 */
export const inspectSqlSelect = (sqlQuery: string, originalSchema: trac.metadata.IFieldSchema[]): { was: string, is: string, function: null | string }[] => {

    // Since it is primitive this does not edit the source
    sqlQuery = removeLineBreaks(sqlQuery)

    // This is what we are going to return, a list of the variables found in the select clause
    let foundVariables: { was: string, is: string, function: null | string }[] = []

    // Get the select statement
    let matches = getAllRegexMatches(sqlQuery, /select\s+(.+)\s+from/g)

    // If the edited query is simply a select * then we can return the original dataset schema variable names
    // as the found items - meaning that the original schema will be used as the output schema
    if (matches.length === 1 && matches[0] === "*" && originalSchema) {
        return originalSchema.map(variable => {

            return variable.fieldName == null ? undefined : {
                was: variable.fieldName,
                is: variable.fieldName,
                function: null
            }

        }).filter(isDefined)
    }

    // If we have found the select statement
    if (matches.length > 0) {

        // Get the terms which will be separated by a comma and check if we can easily see what variable it
        // references
        matches[0].split(",").forEach(term => {

            // Remove leading and trailing white space
            term = term.trim()

            console.log(`LOG:: Checking to see if the term '${term}' can be understood`)

            // Check if it is a simple variable select \w+ is regex for a word
            let variables1 = getAllRegexMatches(term, /^(\w+)$/g)
            // Check if it is a simple variable select with rename \w+ is regex for a word
            let variables2 = getAllRegexMatches(term, /^(\w+)\s+as\s+(\w+)$/g, 2)
            // Check if it is a variable select with a function and rename
            let variables3 = getAllRegexMatches(term, /^(\w+)\s*\(\s*(\w+)\s*\)\s+as\s+(\w+)$/g, 3)

            // If the regex found anything add it to the identified information
            if (variables1.length === 1) foundVariables.push({was: variables1[0], is: variables1[0], function: null})

            if (variables2.length === 1 && variables2[0].length === 2) foundVariables.push({
                was: variables2[0][0],
                is: variables2[0][1],
                function: null
            })

            if (variables3.length === 1 && variables3[0].length === 3) foundVariables.push({
                was: variables3[0][1],
                is: variables3[0][2],
                function: variables3[0][0]
            })
        })
    }

    return foundVariables
}

/**
 * A function that returns an object containing the default values of all the options. This is
 * used when the user selects a new dataset to load into the query builder. A new object is stored.
 */

export type Query = {
    inputData: {
        schema: null | trac.metadata.IFieldSchema[],
        metadata: null | trac.metadata.ITag
    },
    selectTab: {
        aggregation: null | QueryButton
        sqlFunction: null | QueryButton
        unique: null | QueryButton
        operator: null | QueryButton
        // A list of all TRAC basic types that the user wants to see listed in the select tab variable selector
        fieldTypes: trac.BasicType[]
        variable: SingleValue<Option<string, SchemaDetails>>
        isLastOfGroup: boolean
        // A random string generated by nanoid which is used in variable names in more complex calculations
        id: null | string
        // For each part of the select statement when added is pushed into an array which is pushed into added here. The reason an
        // array is added in is so that when using groups each part of the group is added into an array for the overall select statement
        // e.g. select A, B * C + D = added = [[A], [B, *C, +D]]
        added: (Query["selectTab"][])[]
        // On load the select is invalid as no variable is selected
        isValid: boolean
        validationChecked: boolean
        validation: { variable: boolean }
    },
    limitTab: {
        limit: number
    },
    groupByTab: {
        variables: DeepWritable<MultiValue<Option<string, SchemaDetails>>>
    },
    orderByTab: {
        variables: DeepWritable<MultiValue<Option<string, SchemaDetails>>>
        ordering: Record<string, Option<string>>
    },
    editor: {
        isTextLocked: boolean
        userQuery: string
        objectQuery: string
    },
    execution: {
        status: StoreStatus,
        startTime: null | string,
        endTime: null | string,
        duration: null | number
    },
    result: { data: null | DataRow[], schema: trac.metadata.IFieldSchema[], metadata: trac.metadata.ITag, queryThatRan: null | string, tableState: FullTableState }
}

// We have separate functions for setting up each of the properties of the query object, this gives us finer control over resetting it, for example if the
// user cocks up and wants to reset the interface we do not want to remove the information about their selected dataset.
function setupInitialQueryInputData(): Query["inputData"] {
    return {
        schema: null,
        metadata: null
    }
}

function setupInitialQueryForSelectTab(): Query["selectTab"] {
    return {
        aggregation: null,
        sqlFunction: null,
        unique: null,
        operator: null,
        fieldTypes: Types.tracBasicTypes.map(dataType => dataType.type),
        variable: null,
        isLastOfGroup: false,
        id: null,
        added: [],
        // On load the select is invalid as no variable is selected
        isValid: false,
        validationChecked: false,
        validation: {variable: false}
    }
}

function setupInitialQueryForLimitTab(): Query["limitTab"] {
    return {
        limit: General.numberOfDataDownloadRows
    }
}

function setupInitialQueryForGroupByTab(): Query["groupByTab"] {
    return {
        variables: []
    }
}

function setupInitialQueryForOrderByTab(): Query["orderByTab"] {
    return {
        variables: [],
        ordering: {}
    }
}

function setupInitialQueryForEditor(): Query["editor"] {
    return {
        isTextLocked: true,
        userQuery: "",
        objectQuery: `* ${newLine}from dataset ${newLine}limit ${General.numberOfDataDownloadRows}`
    }
}

function setupInitialQueryForExecution(): Query["execution"] {
    return {
        status: "idle",
        startTime: null,
        endTime: null,
        duration: null
    }
}

function setupInitialQueryForResult(): Query["result"] {
    return {
        data: null, schema: [], metadata: {}, queryThatRan: null, tableState: {}
    }
}

export function setupInitialQuery(): Query {

    return {
        inputData: setupInitialQueryInputData(),
        selectTab: setupInitialQueryForSelectTab(),
        limitTab: setupInitialQueryForLimitTab(),
        groupByTab: setupInitialQueryForGroupByTab(),
        orderByTab: setupInitialQueryForOrderByTab(),
        editor: setupInitialQueryForEditor(),
        execution: setupInitialQueryForExecution(),
        result: setupInitialQueryForResult()
    }
}

/**
 * A function that returns an object containing the default history object. This is
 * used when the user selects a new dataset to load into the query builder. A new object is stored.
 * @returns {{indexLoaded: number, positions: []}}
 */
type QueryHistory = { indexLoaded: number, positions: Query["selectTab"][] }

export const setupInitialHistory = (): QueryHistory => ({indexLoaded: 0, positions: []})

// Define a type for the slice state
export interface QueryBuilderStoreState {
    currentlyLoaded: {
        storeKey: null | keyof QueryBuilderStoreState["uses"], objectKey: null | string
    },
    uses: {
        dataAnalytics: {
            query: Record<string, Query>
            history: Record<string, QueryHistory>
        }
    }
}

// If you need to extend the application to use the tool in a new page or component then simply create a new storeKey object and
// load that property in the new scene. The existing reducers all the key (called storeKey) to set values.
const initialState: QueryBuilderStoreState = {

    currentlyLoaded: {
        storeKey: null, objectKey: null
    },
    // Copy this if you want to add a new use case
    uses: {
        dataAnalytics: {
            query: {},
            history: {}
        }
    }
}

/**
 * A reducer that runs when the user clicks to run their query, it checks that the whole select statement is valid and if it is it runs the query.
 */
export const checkAndRunQuery = createAsyncThunk<// Return type of the payload creator
    Promise<DataRow[] | void>,
    // First argument to the payload creator
    ButtonPayload,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('checkAndRunQuery', async ({id: tenant}, {dispatch, getState}) => {

    // Get the parameters we need from the store
    const {objectKey, storeKey} = getState().queryBuilderStore.currentlyLoaded

    if (storeKey != null && objectKey != null && typeof tenant === "string") {

        const {metadata} = getState().queryBuilderStore.uses[storeKey].query[objectKey].inputData

        if (metadata?.header == null) {
            showToast("warning", "The query can not be run because the selected dataset does not have a metadata header.", "runQuery/warning")
            return
        }

        // Get what we need from the store
        const query: undefined | Query = getState().queryBuilderStore.uses[storeKey].query[objectKey]

        const whereClause: undefined | WhereClause = getState().whereClauseBuilderStore.uses[storeKey].whereClause[objectKey]

        if (!query || !whereClause) {
            showToast("warning", "The query can not be run because the query is not defined.", "runQuery/warning")
            return
        }

        // Calculate the full query from the user interface UI as a string
        const objectQuery = calculateQueryString(query, whereClause)

        // Check the validation of the where clause, objectVersion refers to an object version of the where clause, there
        // is a string version of it also available
        const {objectVersion} = getState().whereClauseBuilderStore.uses[storeKey].whereClause[objectKey].whereTab[0]

        if (!objectVersion.isValid) {
            dispatch(setWhereClauseValidationChecked(true))
            showToast("warning", "The query can not be run because you have missing information in your where clause. Please add the information where indicated and try again.", "runQuery/warning")
            return
        }

        // We don't need to validate the select tab as that could be in user editing mode which we can't validate. Instead,
        // the select tab is validated when the user adds a select statement to the query.
        return await decideWhichSqlToUse(objectKey, metadata.header, tenant, "select " + removeLineBreaks(query.editor.isTextLocked ? objectQuery : query.editor.userQuery))
    }
})

export const QueryBuilderStore = createSlice({

    name: 'queryBuilderStore',
    initialState: initialState,
    reducers: {
        /**
         * A reducer that resets the store to its initial value.
         */
        resetStore: () => initialState,
        /**
         * A reducer that sets the object ID of the dataset selected by the user.
         */
        setCurrentlyLoadedQuery: (state, action: PayloadAction<{ storeKey: keyof QueryBuilderStoreState["uses"], objectKey: string }>) => {

            const {storeKey, objectKey} = action.payload

            state.currentlyLoaded.storeKey = storeKey
            state.currentlyLoaded.objectKey = objectKey
        },
        /**
         * A reducer that adds a section to allow a query to be created by the user for their selected dataset. By creating
         * a new entry keyed by the object ID of the dataset the user can move between datasets and recover their query.
         */
        createQueryEntry: (state, action: PayloadAction<{ storeKey: keyof QueryBuilderStoreState["uses"], objectKey: string, metadata: trac.metadata.ITag }>) => {

            // This checks whether there is a property in the store for the user selected dataset and
            // if not creates an entry to be able to build an SQL query for it
            const {storeKey, objectKey, metadata} = action.payload

            if (!state.uses[storeKey].query.hasOwnProperty(objectKey) && metadata.definition?.data?.schema?.table?.fields) {

                state.uses[storeKey].query[objectKey] = setupInitialQuery()
                state.uses[storeKey].history[objectKey] = setupInitialHistory()
                // Store the schema and metadata of the dataset
                state.uses[storeKey].query[objectKey].inputData.schema = metadata.definition.data.schema.table.fields
                state.uses[storeKey].query[objectKey].inputData.metadata = metadata

                // Add the current state of the interface on load, meaning we can always get back to the initial state
                QueryBuilderStore.caseReducers.saveHistory(state)
            }
        },
        /**
         * A reducer that toggles whether the query editor is readonly (and the user changes the query through
         * the menu) or is unlocked (and they can edit the query directly).
         */
        setEditorLocking: (state, _: PayloadAction<ConfirmButtonPayload>) => {

            const {objectKey, storeKey} = state.currentlyLoaded

            if (storeKey != null && objectKey != null) {

                const {positions, indexLoaded} = state.uses[storeKey].history[objectKey]
                const query = state.uses[storeKey].query[objectKey]
                query.editor.isTextLocked = !query.editor.isTextLocked

                // If the editor went from unlocked to locked reset to the last history point
                if (query.editor.isTextLocked) {

                    QueryBuilderStore.caseReducers.moveInHistory(state, {type: "setEditorLocking", payload: {id: positions.length - 1 - indexLoaded}})
                    query.editor.userQuery = ""

                } else {

                    // If the other way round then copy the object to the user string
                    query.editor.userQuery = query.editor.objectQuery
                }
            }
        },
        /**
         * A reducer that saves the SQL query, this is run when the user is manually editing the SQL in the Editor component.
         */
        setUserQuery: (state, action: PayloadAction<SelectValuePayload<string>>) => {

            // Get where we should be saving the payload
            const {objectKey, storeKey} = state.currentlyLoaded

            // If there is a value set then update the store
            if (storeKey != null && objectKey != null && action.payload.value != null) {
                const {editor} = state.uses[storeKey].query[objectKey]
                editor.userQuery = action.payload.value
            }
        },
        /**
         * A reducer that saves the current state of a query to a separate part of the store so that this history can be
         * used to undo any changes.
         */
        saveHistory: (state) => {

            // If user changes something in the query interface then this saves the values to a history
            const {objectKey, storeKey} = state.currentlyLoaded

            if (storeKey != null && objectKey != null) {

                const {indexLoaded, positions} = state.uses[storeKey].history[objectKey]
                const {selectTab} = state.uses[storeKey].query[objectKey]

                // If we have moved back down the history and are not positioned at the end of it.
                // Then the history needs to be cut where we are and then added to. It's like a fork being created.
                if (positions.length > indexLoaded + 1) {
                    state.uses[storeKey].history[objectKey].positions = positions.slice(0, indexLoaded + 1)
                }

                // If the history stored is at the max allowed then trim it down by 1
                if (positions.length >= configuration.maxSelectHistory) {
                    state.uses[storeKey].history[objectKey].positions.slice(1)
                }

                // Add the current state to the history
                state.uses[storeKey].history[objectKey].positions.push(selectTab)
                state.uses[storeKey].history[objectKey].indexLoaded = positions.length - 1
            }
        },
        /**
         * A reducer that updates the state of the query builder back to a particular point in its history. This
         * means we can add undo and redo buttons to the menu.
         */
        moveInHistory: (state, action: PayloadAction<ButtonPayload>) => {

            const {objectKey, storeKey} = state.currentlyLoaded

            // id is either -1 or 1
            const {id: move} = action.payload

            if (storeKey != null && objectKey != null && typeof move === "number") {

                const {indexLoaded, positions} = state.uses[storeKey].history[objectKey]

                // Add the current state to the history before we update it
                state.uses[storeKey].query[objectKey].selectTab = cloneDeep(positions[indexLoaded + move])
                state.uses[storeKey].history[objectKey].indexLoaded = indexLoaded + move
            }
        },
        /**
         * A reducer that resets the query to its original default state. Used when the user completely
         * cocks up.
         */
        resetQuery: (state) => {

            const {objectKey, storeKey} = state.currentlyLoaded

            if (storeKey != null && objectKey != null) {

                // Reset the query but leave the details of the selected dataset
                state.uses[storeKey].query[objectKey].selectTab = setupInitialQueryForSelectTab()
                state.uses[storeKey].query[objectKey].limitTab = setupInitialQueryForLimitTab()
                state.uses[storeKey].query[objectKey].groupByTab = setupInitialQueryForGroupByTab()
                state.uses[storeKey].query[objectKey].orderByTab = setupInitialQueryForOrderByTab()
                state.uses[storeKey].query[objectKey].editor = setupInitialQueryForEditor()
                state.uses[storeKey].query[objectKey].execution = setupInitialQueryForExecution()
                state.uses[storeKey].query[objectKey].result = setupInitialQueryForResult()

                // Add the new state of the interface to the history
                QueryBuilderStore.caseReducers.saveHistory(state)
            }
        },
        /**
         * A reducer that removes the currently selected variable for the select statement, this can be used where the user
         * makes a change to the select statement that means the current variable is no longer applicable.
         */
        resetSelectVariable: (state) => {

            const {objectKey, storeKey} = state.currentlyLoaded

            if (storeKey != null && objectKey != null) {

                const {selectTab} = state.uses[storeKey].query[objectKey]

                // Set the validation message to off
                selectTab.validationChecked = false
                // Remove the variable
                selectTab.variable = null
                selectTab.validation.variable = false
            }
        },
        /**
         * A reducer that sets which button is selected for the select statement. This covers aggregation, SQL functions and unique,
         * buttons, so we use the payload to work out which part of the store to update.
         */
        setFunctionButton: (state, action: PayloadAction<ButtonPayload>) => {

            // Store the value of any functions clicked on
            const {objectKey, storeKey} = state.currentlyLoaded
            const {id, name} = action.payload

            if (storeKey != null && objectKey != null && (id === "aggregation" || id === "unique" || id === "sqlFunction" || id === "unique") && typeof name === "string") {

                const {selectTab} = state.uses[storeKey].query[objectKey]
                const {textButtons, datetimeButtons, numberButtons, aggregationButtons, uniqueButtons} = configuration

                // Get a list of all the functions and get the details for the one selected
                const functionButton = [...textButtons, ...datetimeButtons, ...numberButtons, ...aggregationButtons, ...uniqueButtons].find(sqlFunction => sqlFunction.value === name)

                // Get the details about the selected option, if the same function is set already count it as a de-select
                selectTab[id] = !functionButton || selectTab[id]?.value === functionButton?.value ? null : functionButton

                // Add the new state of the interface to the history
                QueryBuilderStore.caseReducers.saveHistory(state)
            }
        },
        /**
         * A reducer that sets which button is selected for the select statement. This covers operator buttons, so
         * we use the payload to work out which part of the store to update.
         */
        setOperatorButton: (state, action: PayloadAction<ButtonPayload>) => {

            // Store the value of any functions clicked on
            const {objectKey, storeKey} = state.currentlyLoaded
            const {name} = action.payload

            if (storeKey != null && objectKey != null && typeof name === "string") {

                const {selectTab} = state.uses[storeKey].query[objectKey]

                // Get a list of all the functions and get the details for the one selected
                const operatorButton = configuration.operatorButtons.find(operator => operator.value === name)

                // Get the details about the selected option, if the same function is set already count it as a de-select
                selectTab["operator"] = !operatorButton || selectTab["operator"]?.value === operatorButton?.value ? null : operatorButton

                // If the user changes the operator and has a variable selected that is not a number,
                // then remove the selected variable as the operator will not work on it
                if (selectTab["operator"] && !isTracNumber(selectTab.variable?.details.schema.fieldType)) {
                    QueryBuilderStore.caseReducers.resetSelectVariable(state)
                }

                // Add the new state of the interface to the history
                QueryBuilderStore.caseReducers.saveHistory(state)
            }
        },
        /**
         * A reducer that sets the field types to filter the
         */
        setFieldTypeButton: (state, action: PayloadAction<ButtonPayload & { fieldTypes: trac.BasicType[] }>) => {

            // Store the value of any field type filters
            const {objectKey, storeKey} = state.currentlyLoaded
            const {fieldTypes} = action.payload

            if (storeKey != null && objectKey != null) {

                const {selectTab} = state.uses[storeKey].query[objectKey]

                // If we are updating the field type filters then this is just the value, if not then we have a toggle
                selectTab["fieldTypes"] = fieldTypes

                // fieldType with a fallback
                const fieldType = selectTab.variable?.details.schema.fieldType || trac.BasicType.BASIC_TYPE_NOT_SET

                // If the user changes the variable filter but has selected a variable that is not in the filtered list
                // then we remove the variable selected
                if (selectTab.variable && !fieldTypes.includes(fieldType)) {
                    QueryBuilderStore.caseReducers.resetSelectVariable(state)
                }

                // Add the new state of the interface to the history
                QueryBuilderStore.caseReducers.saveHistory(state)
            }
        },
        /**
         * A reducer that sets the variable to use in the select query. If the variable changes type then we need to also look at the function, operator
         * and aggregation buttons again to see if they are still valid.
         */
        setSelectVariable: (state, action: PayloadAction<DeepWritable<SelectOptionPayload<Option<string, SchemaDetails>, false>>>) => {

            // Store the variable selected in the query
            const {objectKey, storeKey} = state.currentlyLoaded
            const {value, isValid} = action.payload

            if (storeKey != null && objectKey != null) {

                const {selectTab} = state.uses[storeKey].query[objectKey]

                // Get all the button definitions from the config
                const {textButtons, datetimeButtons, numberButtons, aggregationButtons, operatorButtons} = configuration

                selectTab.variable = value
                selectTab.validation.variable = isValid

                // Set the validation message to off if they change something
                selectTab.validationChecked = false

                // Get a list of all the non-aggregation functions
                const allFunctions = [...textButtons, ...datetimeButtons, ...numberButtons]

                // If the user changes the variable to one where the selected function is not valid
                // the remove that function selection
                const sqlFunctionButton = allFunctions.find(button => button.value === selectTab.sqlFunction?.value)

                if (value && selectTab.sqlFunction?.value && sqlFunctionButton && !sqlFunctionButton.fieldTypes.includes(value?.details.schema.fieldType || trac.BasicType.BASIC_TYPE_NOT_SET)) {
                    selectTab.sqlFunction = null
                }

                // If the user changes the variable to one where the selected aggregation is not valid
                // the remove that aggregation selection
                const aggregationButton = aggregationButtons.find(button => button.value === selectTab.aggregation?.value)

                if (value && selectTab.aggregation?.value && aggregationButton && !aggregationButton.fieldTypes.includes(value?.details.schema.fieldType || trac.BasicType.BASIC_TYPE_NOT_SET)) {
                    selectTab.aggregation = null
                }

                // If the user changes the variable to one where the selected operator is not valid
                // the remove that operator selection
                const operatorButton = operatorButtons.find(button => button.value === selectTab.operator?.value)

                if (value && selectTab.operator && operatorButton && !operatorButton.fieldTypes.includes(value?.details.schema.fieldType || trac.BasicType.BASIC_TYPE_NOT_SET)) {
                    selectTab.operator = null
                }

                // Add the new state of the interface to the history
                QueryBuilderStore.caseReducers.saveHistory(state)
            }
        },
        /**
         * A reducer that ends a group in the select statement, a group is when you have a * b + c, you need to be able to close this term off
         * as complete in order to add a separate statement.
         */
        setEndGroup: (state) => {

            // If the user is trying to add a group to the calculation then this allows them to end the group
            const {objectKey, storeKey} = state.currentlyLoaded

            if (storeKey != null && objectKey != null) {

                const {selectTab} = state.uses[storeKey].query[objectKey]

                if (selectTab.added.length > 0) {

                    const selectToEdit = selectTab.added[selectTab.added.length - 1]
                    selectToEdit[selectToEdit.length - 1].isLastOfGroup = true
                }
            }
        },
        /**
         * A reducer that adds the values as set in the user interface to the select part of the query.
         */
        addToSelect: (state) => {

            // Add the values in the user interface as a term in the query
            const {objectKey, storeKey} = state.currentlyLoaded

            if (storeKey == null || objectKey == null) return

            const {selectTab} = state.uses[storeKey].query[objectKey]

            // Get the list of the current variables in the select
            const currentSelect = processSelectQuery(selectTab.added)

            // Set the validation message to on
            selectTab.validationChecked = true

            // If the user tried to add to select without a variable added then abort (and show the error message)
            if (!selectTab.variable) return

            // Check if the variable has already been added - we can only do this deterministically if
            // there is no operator as the selections have a 1-2-1 mapping to the variable name
            if (!selectTab.operator) {

                // Get the details of the select term trying to be added
                const newSelect = processSelectQuery([[{...selectTab}]])

                // Get the 'as' part of the select statement (the second element) and create a map of these, then see
                // if the new variable added has the same 'as' as one that already exists and if so refuse to add the new variable
                if (currentSelect.map(select => select[1]).includes(newSelect[0][1])) {
                    showToast("warning", "That variable already exists in the query and cannot be added again", "addToSelect")
                    return
                }
            }

            // If the user does not have an operator selected or if the last added select has a variable tagged as
            // the last of a group then we start a new group.
            const selectToCheck = selectTab.added[selectTab.added.length - 1]

            if (!selectTab.operator || selectTab.added.length === 0 || selectToCheck[selectToCheck.length - 1].isLastOfGroup === true) {

                // Add the new select term
                selectTab.added.push([cloneDeep(selectTab)])

                const indexOfLastAddedVariable = selectTab.added.length - 1

                // Add a unique ID to the added variable
                selectTab.added[indexOfLastAddedVariable][0].id = nanoid(5).toUpperCase().replace(/[-,_]/g, "X")

                // Some operators don't make sense if they are prepended to a variable that is the first in a term.
                // You have to edit what has been added to be safe
                if (selectTab.operator && ["*", "/", "%"].includes(selectTab.operator.value)) {

                    selectTab.added[indexOfLastAddedVariable][0].operator = null
                }

            } else {

                // If the user has an operator and the end of the previously added select is not marked as the end of a group then
                // continue the term.
                const indexToUpdate = selectTab.added.length - 1
                selectTab.added[indexToUpdate].push(cloneDeep(selectTab))
            }

            // Add the new state of the interface to the history
            QueryBuilderStore.caseReducers.saveHistory(state)
        },
        /**
         * A reducer that sets the number of rows to download from the results of a query.
         */
        setLimit: (state, action: PayloadAction<SelectValuePayload<number>>) => {

            // This stores the row limit to apply in the query
            const {objectKey, storeKey} = state.currentlyLoaded
            const {value} = action.payload

            if (storeKey != null && objectKey != null && value != null) {

                state.uses[storeKey].query[objectKey].limitTab.limit = value
            }
        },
        /**
         * A reducer that sets the variables to use to group the results back from the query.
         */
        setGroupBy: (state, action: PayloadAction<DeepWritable<SelectOptionPayload<Option<string, SchemaDetails>, true>>>) => {

            const {objectKey, storeKey} = state.currentlyLoaded
            const {value} = action.payload

            if (storeKey != null && objectKey != null) {

                state.uses[storeKey].query[objectKey].groupByTab.variables = value
            }
        },
        /**
         * A reducer that sets the variables to use to set the sorting of the results back from the query.
         */
        setOrderBy: (state, action: PayloadAction<DeepWritable<SelectOptionPayload<Option<string, SchemaDetails>, true>>>) => {

            const {objectKey, storeKey} = state.currentlyLoaded

            if (storeKey != null && objectKey != null) {

                const {orderByTab} = state.uses[storeKey].query[objectKey]
                const {value} = action.payload

                // Store the variables we want to order by i the store
                orderByTab.variables = value

                // Check if any of the selected order by variables are new and if so add a property to give them a
                // default ordering
                value.forEach(variable => {
                    if (!orderByTab.ordering.hasOwnProperty(variable.value)) {
                        orderByTab.ordering[variable.value] = configuration.orderingOptions[0]
                    }
                })
            }
        },
        /**
         * A reducer that sets the sorting order of a variable being used to in the order by part of the query.
         * For example, you can choose between ascending and descending.
         */
        setOrdering: (state, action: PayloadAction<SelectOptionPayload<Option<string>, false>>) => {

            const {id, value} = action.payload
            const {objectKey, storeKey} = state.currentlyLoaded

            if (storeKey != null && objectKey != null && typeof id === "string" && value) {

                state.uses[storeKey].query[objectKey].orderByTab.ordering[id] = value
            }
        },
        /**
         * A reducer that resets any stored information about a query that has been run.
         */
        resetResults: (state) => {

            const {objectKey, storeKey} = state.currentlyLoaded

            if (storeKey != null && objectKey != null) {

                const query = state.uses[storeKey].query[objectKey]
                console.log("LOG :: Wiping table state")
                query.result = {data: null, schema: [], metadata: {}, queryThatRan: null, tableState: {}}
            }
        },
        /**
         * A reducer that stores the state of the query result table so that if the user navigates away and then comes
         * back to the page.
         */
        getTableState: (state, action: PayloadAction<{ tableState: FullTableState }>) => {

            const {objectKey, storeKey} = state.currentlyLoaded

            if (storeKey != null && objectKey != null) {

                const {tableState} = action.payload

                const query = state.uses[storeKey].query[objectKey]

                // The below can not be done when a query is running, this is because before the query starts
                // we run the reset query that clears the table state held in the store. However, when the
                // status of the async request is set to pending that causes the table to unmount, that unmounting
                // triggers a useEffect function that saves the table state to the store. The net of this is that
                // the state of the table is not actually reset. So to get around this we do not let the table
                // save the state to the store when the query is running.
                if (query.execution.status !== "pending") state.uses[storeKey].query[objectKey].result.tableState = tableState
            }
        }
    },
    extraReducers: (builder) => {

        // A set of lifecycle reducers to run before/after the runQuery function
        builder.addCase(checkAndRunQuery.pending, (state) => {

            // Clear all the messages
            toast.dismiss()

            const {objectKey, storeKey} = state.currentlyLoaded

            // Exit if the values we need are not set
            if (storeKey == null || objectKey == null) return

            const query = state.uses[storeKey].query[objectKey]

            // Reset the data storage
            QueryBuilderStore.caseReducers.resetResults(state)

            // Take a copy of the query that is going to be run. This is so if the user changes the SQL interface
            // there is a record of what ran
            query.result.queryThatRan = `select ${query.editor.isTextLocked ? query.editor.objectQuery : query.editor.userQuery}`

            query.execution.status = "pending"
            query.execution.startTime = new Date().toISOString()
            query.execution.endTime = null
            query.execution.duration = null
        })
        builder.addCase(checkAndRunQuery.fulfilled, (state, action: PayloadAction<Promise<void | DataRow[]>>) => {

            const {objectKey, storeKey} = state.currentlyLoaded

            // Exit if the values we need are not set
            if (storeKey == null || objectKey == null) return

            const query = state.uses[storeKey].query[objectKey]

            if (Array.isArray(action.payload) && action.payload.length === 0) {

                query.result.data = []
                query.result.schema = []
                showToast("warning", "The query completed successfully but no results were returned", "runQuery/fulfilled/noResults")

            } else if (Array.isArray(action.payload) && action.payload.length > 0 && typeof query.result.queryThatRan === "string") {

                query.result.data = action.payload
                query.result.schema = query.editor.isTextLocked ? getSchemaOfNonUserEditedQuery(query) : guessSchemaOfUserEditedQuery(query.inputData.schema, action.payload, query.result.queryThatRan, configuration)
                showToast("success", "The query completed successfully", "runQuery/fulfilled/results")

            } else {

            }

            // Either way record the times
            query.execution.status = "succeeded"
            query.execution.endTime = new Date().toISOString()

            if (query.execution.startTime) {
                // duration is in seconds
                console.log(query.execution.endTime)
                 console.log(query.execution.startTime)
                query.execution.duration = differenceInSeconds(parseISO(query.execution.endTime), parseISO(query.execution.startTime))
            }
        })
        builder.addCase(checkAndRunQuery.rejected, (state, action) => {

            const {objectKey, storeKey} = state.currentlyLoaded

            // Exit if the values we need are not set
            if (storeKey == null || objectKey == null) return

            const query = state.uses[storeKey].query[objectKey]

            // Reset the data storage
            QueryBuilderStore.caseReducers.resetResults(state)

            query.execution.status = "failed"
            query.execution.startTime = null
            query.execution.endTime = null
            query.execution.duration = null

            const text = {
                title: "Failed to complete query",
                message: `The query did not complete successfully.`,
                details: action.error.message
            }

            showToast("error", text, "runQuery/rejected")
            console.error(action.error)
        })
    }
})

// Action creators are generated for each case reducer function
export const {
    addToSelect,
    createQueryEntry,
    getTableState,
    moveInHistory,
    resetQuery,
    setCurrentlyLoadedQuery,
    setEditorLocking,
    setEndGroup,
    setFieldTypeButton,
    setFunctionButton,
    setGroupBy,
    setLimit,
    setOrderBy,
    setOrdering,
    setOperatorButton,
    setSelectVariable,
    setUserQuery
} = QueryBuilderStore.actions

export default QueryBuilderStore.reducer