import {BasicTypeDetails, ButtonPayload, ConfirmButtonPayload, DeepWritable, Option, SchemaDetails, SelectOptionPayload, SelectPayload} from "../../../types/types_general";
import {configuration} from "./config_where_clause_builder";
import {convertDataBetweenTracTypes} from "../../utils/utils_general";
import {createSlice, PayloadAction} from "@reduxjs/toolkit";
import {isPrimitive, isTracDateOrDatetime, isTracNumber, isTracString} from "../../utils/utils_trac_type_chckers";
import {SingleValue} from "react-select";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * A function that calculates the full where clause as a string .
 * @param rules {Rule[]} The array of rules from part of the WhereClauseBuilderStore.
 * @returns - - The where clause for all the rules.
 */
export const calculateWhereClauseString = (rules: Rule[]) => {

    return rules.map((rule, i) => {

        const {variable, operator, value1, value2, logic, not} = rule

        // First value property is the option selected, the second is the value of the option
        const variableString = variable?.value != null ? variable.value : "<variable>"

        const operatorString = operator.value != null ? operator.value : "<operator>"

        const delimiter = variable?.details.schema && (isTracString(variable.details.schema.fieldType) || isTracDateOrDatetime(variable.details.schema.fieldType)) ? "'" : ""

        const wildcard = operator.value === "LIKE" ? "%" : ""

        let value1String

        if (operator.value != null && ["is NULL", "is NOT NULL"].includes(operator.value)) {
            value1String = ""
        } else if (operator.value != null && ["in", "not in"].includes(operator.value) && typeof value1 === "string") {
            value1String = " (" + value1.split(",").filter(value => value !== "").map(value => `${delimiter}${value}${delimiter}`).join(", ") + ")"
        } else if (value1 != null) {
            value1String = ` ${delimiter}${wildcard}${value1}${wildcard}${delimiter}`
        } else {
            value1String = ` ${delimiter}${wildcard}<value>${wildcard}${delimiter}`
        }

        const value2String = operator.value != null && !["between"].includes(operator.value) ? "" : value2 != null ? ` and ${delimiter}${value2}${delimiter}` : ` and ${delimiter}<value>${delimiter}`

        const logicString = i < rules.length - 1 ? ` ${logic}${not ? " NOT" : ""}` : ""

        return `${variableString} ${operatorString}${value1String}${value2String}${logicString}`

    }).join(" ")
}

/**
 * A function that returns an object containing the default values of all the where clause options. This
 *  is used when the user selects a new dataset to load into the where clause builder. A new object is
 *  created for the where clause for that dataset.
 */

// We have separate functions for setting up each of the properties of the overlay object, this gives us finer control over resetting it, for example if the
// user cocks up and wants to reset the interface we do not want to remove the information about their selected dataset.
function setupInitialQueryInputData(): WhereClause["inputData"] {
    return {
        schema: null
    }
}

function setupWhereClauseWhereTab(): WhereTab {

    return {
        objectVersion: {

            isValid: true,
            rules: []
        },
        stringVersion: ""
    }
}

export type WhereTab = {
    objectVersion: {
        isValid: boolean,
        rules: Rule[]
    },
    stringVersion: string
}


export type WhereClause = {
    inputData: {
        schema: null | trac.metadata.IFieldSchema[],
    },
    // The where property is an array of where clauses each with an array of rules. The QueryBuilder
    // component does not need an array as if you load a dataset to query it can only have one where
    // statement (albeit with multiple rules). It is the OverlayBuilder component that needs multiple
    // where clauses for each dataset, in that component the user can declare multiple overlays for a
    // single dataset, each with multiple rules. It's a f**king headache to be honest but not too bad
    // if you build it in from the ground up.
    whereTab: WhereTab[]
    validationChecked: boolean
}

export function setupInitialWhereClause(): WhereClause {

    return {
        inputData: setupInitialQueryInputData(),
        whereTab: [],
        validationChecked: false
    }
}

/**
 * An interface for each rule in the where clause.
 */
export type Rule = {
    variable: SingleValue<Option<string, SchemaDetails>>
    operator: Option<string, BasicTypeDetails>
    value1: SelectPayload<Option, false>["value"]
    value2: SelectPayload<Option, false>["value"]
    logic: "AND" | "OR"
    not: boolean
    validation: { variable: boolean, operator: boolean, value1: boolean, value2: boolean }
}

/**
 * A function that returns an object containing the default values for a new rule in a clause. This is
 * used when the user selects to add a rule to a where clause.
 */
export function setupNewRule(): Rule {

    return ({
        variable: null,
        // This needs to match one of the options in operatorOptions
        operator: {value: "=", label: "equal to", details: {basicTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL, trac.DATE, trac.DATETIME, trac.STRING, trac.BOOLEAN]}},
        value1: null,
        value2: null,
        logic: "AND",
        not: false,
        validation: {variable: false, operator: true, value1: false, value2: true}
    })
}

// Define a type for the slice state
export interface WhereClauseBuilderStoreState {
    currentlyLoaded: {
        storeKey: null | keyof WhereClauseBuilderStoreState["uses"], objectKey: null | string
    },
    uses: {
        dataAnalytics: {
            // The where clause is an array of clauses, the QueryBuilder does not need an array as you load up
            whereClause: Record<string, WhereClause>
        },
        runAFlow: {
            // The where clause is an array of clauses, the QueryBuilder does not need an array as you load up
            whereClause: Record<string, WhereClause>
        }
    }
}

// If you need to extend the application to use the tool in a new page or component then simply create a new storeKey object and
// load that property in the new scene. The existing reducers all the key (called storeKey) to set values.
const initialState: WhereClauseBuilderStoreState = {

    currentlyLoaded: {
        storeKey: null, objectKey: null
    },
    // Copy this if you want to add a new use case
    uses: {
        dataAnalytics: {
            whereClause: {}
        },
        runAFlow: {
            whereClause: {}
        }
    }
}

export const WhereClauseBuilderStore = createSlice({

    name: 'whereClauseBuilderStore',
    initialState: initialState,
    reducers: {
        /**
         * A reducer that resets the store to its initial value.
         */
        resetStore: () => initialState,
        /**
         * A reducer that sets the object ID of the dataset selected by the user.
         */
        setCurrentlyLoadedWhereClause: (state, action: PayloadAction<{ storeKey: keyof WhereClauseBuilderStoreState["uses"], objectKey: string }>) => {

            const {storeKey, objectKey} = action.payload

            state.currentlyLoaded.storeKey = storeKey
            state.currentlyLoaded.objectKey = objectKey
        },
        /**
         * A reducer that adds a section to allow a where clause to be created by the user for their selected dataset. By creating
         * a new entry keyed by the object ID of the dataset the user can move between datasets and recover their query.
         */
        createWhereClauseEntry: (state, action: PayloadAction<{ storeKey: keyof WhereClauseBuilderStoreState["uses"], objectKey: string, schema: trac.metadata.IFieldSchema[] }>) => {

            // This checks whether there is a property in the store for the user selected dataset and
            // if not creates an entry to be able to build an SQL where clause for it
            const {storeKey, objectKey, schema} = action.payload

            if (!state.uses[storeKey].whereClause.hasOwnProperty(objectKey)) {

                // Add a where clause for the corresponding overlay
                state.uses[storeKey].whereClause[objectKey] = setupInitialWhereClause()

                state.uses[storeKey].whereClause[objectKey].inputData.schema = schema

                // Add the structure for the where tab information
                state.uses[storeKey].whereClause[objectKey].whereTab.push(setupWhereClauseWhereTab())
            }
        },
        /**
         * A reducer that adds a where clause to a dataset already in the store.
         */
        addWhereClause: (state, action: PayloadAction<ButtonPayload>) => {

            const {objectKey, storeKey} = state.currentlyLoaded

            if (storeKey != null && objectKey != null) {

                // Add the structure for the where tab information
                state.uses[storeKey].whereClause[objectKey].whereTab.push(setupWhereClauseWhereTab())
            }
        },
        /**
         * A reducer that resets the query to its original default state. Used when the user completely
         * cocks up.
         */
        resetWhereClause: (state, action: PayloadAction<number>) => {

            const {objectKey, storeKey} = state.currentlyLoaded
            const {payload: whereIndex} = action

            if (storeKey != null && objectKey != null) {

                // If we need to reset a particular where clause then we use that to reset a particular position
                // in the array e.g. in the OverlayBuilder we may be asked to reset the 3rd overlay for a dataset
                // i=2
                state.uses[storeKey].whereClause[objectKey].whereTab[whereIndex] = setupWhereClauseWhereTab()
            }
        },
        /**
         * A reducer that adds a rule to the where clause.
         */
        addRule: (state, action: PayloadAction<ButtonPayload>) => {

            const {objectKey, storeKey} = state.currentlyLoaded
            const {index: whereIndex} = action.payload

            if (storeKey != null && objectKey != null && whereIndex != null) {

                const {rules} = state.uses[storeKey].whereClause[objectKey].whereTab[whereIndex].objectVersion

                rules.push(setupNewRule())

                WhereClauseBuilderStore.caseReducers.updateWhereClauseString(state, {type: "updateWhereClauseString", payload: whereIndex})
                WhereClauseBuilderStore.caseReducers.validateWhereClause(state, {type: "validateWhereClause", payload: whereIndex})
                WhereClauseBuilderStore.caseReducers.setWhereClauseValidationChecked(state, {
                    type: "setWhereClauseValidationChecked",
                    payload: false
                })
            }
        },
        /**
         * A reducer that deletes a rule from the where clause.
         */
        deleteRule: (state, action: PayloadAction<ConfirmButtonPayload>) => {

            const {objectKey, storeKey} = state.currentlyLoaded
            const {id: ruleIndex, index: whereIndex} = action.payload

            if (storeKey != null && objectKey != null && typeof ruleIndex === "number" && whereIndex != null) {

                const {rules} = state.uses[storeKey].whereClause[objectKey].whereTab[whereIndex].objectVersion

                rules.splice(ruleIndex, 1)

                WhereClauseBuilderStore.caseReducers.updateWhereClauseString(state, {type: "updateWhereClauseString", payload: whereIndex})
                WhereClauseBuilderStore.caseReducers.validateWhereClause(state, {type: "validateWhereClause", payload: whereIndex})
                WhereClauseBuilderStore.caseReducers.setWhereClauseValidationChecked(state, {
                    type: "setWhereClauseValidationChecked",
                    payload: false
                })
            }
        },
        /**
         * A reducer that sets the logic to apply when there are more than one where clause rule. This can be
         * 'AND', 'OR', 'AND NOT' or "OR NOT"
         */
        setLogic: (state, action: PayloadAction<ButtonPayload>) => {

            const {objectKey, storeKey} = state.currentlyLoaded

            // id is the index of the rule and name is the logic value
            const {id: ruleIndex, index: whereIndex, name} = action.payload

            if (storeKey != null && objectKey != null && typeof ruleIndex === "number" && whereIndex != null) {

                const rule = state.uses[storeKey].whereClause[objectKey].whereTab[whereIndex].objectVersion.rules[ruleIndex]

                if (name === "AND" || name === "OR") {
                    rule.logic = name
                } else {
                    rule.not = !rule.not
                }

                WhereClauseBuilderStore.caseReducers.updateWhereClauseString(state, {type: "updateWhereClauseString", payload: whereIndex})
            }
        },
        /**
         * A reducer that sets the variable to use in a where clause rule. e.g. where balance > 0, here you are
         * setting the balance variable.
         */
        setWhereClauseVariable: (state, action: PayloadAction<SelectOptionPayload<Option<string, SchemaDetails>, false>>) => {

            const {objectKey, storeKey} = state.currentlyLoaded
            const {id: ruleIndex, index: whereIndex, value} = action.payload

            if (storeKey != null && objectKey != null && typeof ruleIndex === "number" && whereIndex != null) {

                // Get the rule from the array
                const rule = state.uses[storeKey].whereClause[objectKey].whereTab[whereIndex].objectVersion.rules[ruleIndex]

                // Get the current fieldType of the selected variable e.g. INTEGER and the operator e.g. =
                const oldFieldType = rule.variable?.details.schema?.fieldType
                const oldOperator = rule.operator.value

                // Update the variable
                rule.variable = action.payload.value
                rule.validation.variable = action.payload.isValid

                // If the user changed the variable type e.g. INTEGER => STRING then the currently selected
                // operator may not be valid, here we check this and if it is invalid we blank it
                const isOldOperatorValid = configuration.operatorOptions.filter(operator => operator.details.basicTypes.includes(value?.details.schema?.fieldType || trac.BasicType.BASIC_TYPE_NOT_SET)).some(operator => operator.value === oldOperator)

                // The operator to set if the old one is not available to the new variable
                const defaultOperator = configuration.operatorOptions.find(operator => operator.value === "=")

                // If the old operator is not available for the new variable then we reset to something that is
                if (!isOldOperatorValid && defaultOperator) {

                    rule.operator = defaultOperator
                    rule.validation.operator = true

                    // Blank the second value stored for between operators
                    rule.value2 = null
                    rule.validation.value2 = true

                } else if (!isOldOperatorValid && !defaultOperator) {

                    throw new Error("The defaultOperator could not be identified")
                }

                // If the user changed the variable type then the value they are using, for example A = 5, may be invalid
                // for the new type, here we check this and if the value is invalid we blank it. This first clause only
                // works when the value is not set using an option.
                if (oldFieldType && oldFieldType !== value?.details.schema.fieldType) {

                    if (rule.value1 != null && isPrimitive(rule.value1) && oldFieldType != null && value?.details.schema.fieldType) {

                        // TODO if we implement the background query then this will need to also be checked against the lookup
                        rule.value1 = convertDataBetweenTracTypes(oldFieldType, value.details.schema.fieldType, rule.value1)

                        if (rule.value1 == null) {
                            rule.validation.value1 = false
                        }

                    } else {

                        // This is for when the value is set via an option and the fieldType changes
                        rule.value1 = null
                        rule.validation.value1 = false
                    }
                }

                // If the old operator was between and is still valid then we need to check the second between value is still
                // ok, if it is not we blank it
                if (oldOperator === "BETWEEN" && oldFieldType !== value?.details.schema.fieldType) {

                    if (rule.value2 != null && isPrimitive(rule.value2) && oldFieldType != null && value?.details.schema.fieldType) {

                        // TODO if we implement the background query then this will need to also be checked against the lookup
                        rule.value2 = convertDataBetweenTracTypes(oldFieldType, value.details.schema.fieldType, rule.value2)

                        if (rule.value2 == null) {
                            rule.validation.value2 = false
                        }

                    } else {

                        // This is for when the value is set via an option and the fieldType changes
                        rule.value2 = null
                        rule.validation.value2 = false
                    }
                }

                WhereClauseBuilderStore.caseReducers.updateWhereClauseString(state, {type: "updateWhereClauseString", payload: whereIndex})
                WhereClauseBuilderStore.caseReducers.validateWhereClause(state, {type: "validateWhereClause", payload: whereIndex})
                WhereClauseBuilderStore.caseReducers.setWhereClauseValidationChecked(state, {type: "setWhereClauseValidationChecked", payload: false})
            }
        },
        /**
         * A reducer that sets the operator to use in a where clause rule. e.g. where balance > 0, here you are
         * setting the > operator.
         */
        setOperator: (state, action: PayloadAction<SelectOptionPayload<Option<string, BasicTypeDetails>, false>>) => {

            // Set the operator to apply in a where clause rule
            const {objectKey, storeKey} = state.currentlyLoaded
            const {id: ruleIndex, index: whereIndex, isValid, value} = action.payload

            if (storeKey != null && objectKey != null && typeof ruleIndex === "number" && whereIndex != null && value) {

                // Get the rule from the array
                const rule = state.uses[storeKey].whereClause[objectKey].whereTab[whereIndex].objectVersion.rules[ruleIndex]

                // When changing from between to something else we need to blank the second between value
                if (rule.operator.value === "BETWEEN" && value?.value !== "BETWEEN") {
                    rule.value2 = null
                    rule.validation.value2 = true
                }

                // If the user is using the null operators we need to blank the values
                if (!value?.value || ["is NULL", "is NOT NULL"].includes(value.value)) {
                    rule.value1 = null
                    rule.value2 = null
                    rule.validation.value1 = true
                    rule.validation.value2 = true
                }

                // When changing from an in operator to something else we need to blank the value if the variable
                // is a number because the input will be a string
                if (["in", "not in"].includes(rule.operator.value) && (value?.value == null || !["in", "not in"].includes(value.value)) && isTracNumber(rule.variable?.details.schema.fieldType)) {
                    rule.value1 = null
                    rule.validation.value1 = false
                }

                // When going from IN or NOT IN for a string that is using a select box as the values then
                // if the new operator is also a select but one that is not multi select then we take the first option.
                // TODO selects used to set in is not supported yet
                if (Array.isArray(rule.value1) && ["in", "not in"].includes(rule.operator.value) && (value?.value == null || !["in", "not in"].includes(value.value))) {
                    rule.value1 = rule.value1 && rule.value1.length > 0 ? rule.value1[0] : null

                    if (rule.value1 == null) {
                        rule.validation.value1 = false
                    }
                }

                rule.operator = value
                rule.validation.operator = isValid

                WhereClauseBuilderStore.caseReducers.updateWhereClauseString(state, {type: "updateWhereClauseString", payload: whereIndex})
                WhereClauseBuilderStore.caseReducers.validateWhereClause(state, {type: "validateWhereClause", payload: whereIndex})
                WhereClauseBuilderStore.caseReducers.setWhereClauseValidationChecked(state, {type: "setWhereClauseValidationChecked", payload: false})
            }
        },
        /**
         * A reducer that sets the value to use in a where clause rule. e.g. where balance > 0, here you are
         * setting the 0.
         */
        setValue1: (state, action: PayloadAction<DeepWritable<SelectPayload<Option, false>>>) => {

            // Set the value in the where clause rule
            const {objectKey, storeKey} = state.currentlyLoaded
            const {id: ruleIndex, index: whereIndex} = action.payload

            if (storeKey != null && objectKey != null && typeof ruleIndex === "number" && whereIndex != null) {

                // Get the rule from the array
                const rule = state.uses[storeKey].whereClause[objectKey].whereTab[whereIndex].objectVersion.rules[ruleIndex]

                rule.value1 = action.payload.value
                rule.validation.value1 = action.payload.isValid

                WhereClauseBuilderStore.caseReducers.updateWhereClauseString(state, {type: "updateWhereClauseString", payload: whereIndex})
                WhereClauseBuilderStore.caseReducers.validateWhereClause(state, {type: "validateWhereClause", payload: whereIndex})
                WhereClauseBuilderStore.caseReducers.setWhereClauseValidationChecked(state, {type: "setWhereClauseValidationChecked", payload: false})
            }
        },
        /**
         * A reducer that sets the second variable to use in a where clause rule using the between operator.
         */
        setValue2: (state, action: PayloadAction<DeepWritable<SelectPayload<Option, false>>>) => {

            const {objectKey, storeKey} = state.currentlyLoaded
            const {id: ruleIndex, index: whereIndex} = action.payload

            if (storeKey != null && objectKey != null && typeof ruleIndex === "number" && whereIndex != null) {

                // Get the rule from the array
                const rule = state.uses[storeKey].whereClause[objectKey].whereTab[whereIndex].objectVersion.rules[ruleIndex]

                rule.value2 = action.payload.value
                rule.validation.value2 = action.payload.isValid

                WhereClauseBuilderStore.caseReducers.updateWhereClauseString(state, {type: "updateWhereClauseString", payload: whereIndex})
                WhereClauseBuilderStore.caseReducers.validateWhereClause(state, {type: "validateWhereClause", payload: whereIndex})
                WhereClauseBuilderStore.caseReducers.setWhereClauseValidationChecked(state, {
                    type: "setWhereClauseValidationChecked",
                    payload: false
                })
            }
        },
        /**
         * A reducer that updates the where part of the query shown in the editor, this takes the state for the component and converts
         * all the settings into a valid where SQL query string.
         */
        updateWhereClauseString: (state, action: PayloadAction<number>) => {

            const {objectKey, storeKey} = state.currentlyLoaded
            const {payload: whereIndex} = action

            if (storeKey != null && objectKey != null) {

                const {rules} = state.uses[storeKey].whereClause[objectKey].whereTab[whereIndex].objectVersion

                state.uses[storeKey].whereClause[objectKey].whereTab[whereIndex].stringVersion = calculateWhereClauseString(rules)
            }
        },
        /**
         * A function that checks if the where clause rules are valid and have all the selections for the whole statement.
         */
        validateWhereClause: (state, action: PayloadAction<number>) => {

            // Check that the where clause is complete
            const {objectKey, storeKey} = state.currentlyLoaded
            const {payload: whereIndex} = action

            if (storeKey != null && objectKey != null) {

                const {objectVersion} = state.uses[storeKey].whereClause[objectKey].whereTab[whereIndex]

                objectVersion.isValid = objectVersion.rules.length === 0 || objectVersion.rules.every(rule => rule.validation.variable === true && rule.validation.value1 === true && rule.validation.value2 === true && rule.validation.operator === true)
            }
        },
        /**
         * A reducer that sets whether to show any validation information in the user interface for all where clauses for a given
         * objectKey.
         */
        setWhereClauseValidationChecked: (state, action: PayloadAction<boolean>) => {

            // Check that the where clause is complete
            const {objectKey, storeKey} = state.currentlyLoaded

            if (storeKey != null && objectKey != null) {

                state.uses[storeKey].whereClause[objectKey].validationChecked = action.payload
            }
        },
        /**
         * A reducer that deletes a whereClause for a corresponding overlayKey. This is needed when the user deletes an overlay and the overlay and where clause definitions
         * need to be removed.
         */
        deleteWhereClause: (state, action: PayloadAction<ConfirmButtonPayload>) => {

            const {objectKey, storeKey} = state.currentlyLoaded
            const {index: overlayIndex} = action.payload

            if (storeKey != null && objectKey != null) {

                // Remove the overlay
                state.uses[storeKey].whereClause[objectKey].whereTab = state.uses[storeKey].whereClause[objectKey].whereTab.filter((item, i) => i !== overlayIndex);
            }
        }
    }
})

// Action creators are generated for each case reducer function
export const {
    addRule,
    addWhereClause,
    deleteWhereClause,
    deleteRule,
    createWhereClauseEntry,
    resetWhereClause,
    setCurrentlyLoadedWhereClause,
    setLogic,
    setOperator,
    setWhereClauseValidationChecked,
    setValue1,
    setValue2,
    setWhereClauseVariable,
    validateWhereClause
} = WhereClauseBuilderStore.actions

export default WhereClauseBuilderStore.reducer