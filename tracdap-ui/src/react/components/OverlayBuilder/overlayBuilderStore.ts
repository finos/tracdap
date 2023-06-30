import {
    BasicTypeDetails,
    ButtonPayload,
    ConfirmButtonPayload,
    DeepWritable,
    Option,
    SchemaDetails,
    SelectOptionPayload,
    SelectPayload,
    SelectTogglePayload,
    SelectValuePayload
} from "../../../types/types_general";
import {configuration} from "./config_overlay_builder";
import {createSlice, nanoid, PayloadAction} from "@reduxjs/toolkit";
import {convertDataBetweenTracTypes} from "../../utils/utils_general";
import {isOption, isPrimitive, isTracDateOrDatetime, isTracNumber, isTracString} from "../../utils/utils_trac_type_chckers";
import {SingleValue} from "react-select";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {Types} from "../../../config/config_trac_classifications";
import {WhereTab} from "../WhereClauseBuilder/whereClauseBuilderStore";

// A newline character in a string, used to layout query string in the browser
// https://stackoverflow.com/questions/1761051/difference-between-n-and-r
const newLine = "\r\n"
// A tab character in a string, used to layout query string in the browser
const tab = "\t"

/**
 * A function that calculates the overlay string, this can handle overlays that use apportioning
 * and where clauses.
 * @param changeTab {ChangeTab} The object containing information about an overlay which can be converted into an SQL string.
 * @param whereTab {ChangeTab} The object containing information about an overlay which can be converted into an SQL string.
 */
export const calculateOverlayString = (changeTab: void | ChangeTab, whereTab: void | WhereTab) => {

    let finalSql = ""

    if (changeTab == null || whereTab == null) {

    } else if (!changeTab.apportion && whereTab.objectVersion.rules.length === 0) {

        // No apportioning and no where clause
        finalSql = calculateOverlayBasicFormWithoutWhere(changeTab, true)

    } else if (!changeTab.apportion && whereTab.objectVersion.rules.length > 0) {

        // No apportioning but with a where clause
        finalSql = calculateOverlayBasicFormWithWhere(changeTab, whereTab)

    } else if (changeTab.apportion && whereTab.objectVersion.rules.length === 0) {

        // Apportioning and no where clause
        finalSql = calculateOverlayStringWithApportion(changeTab, true)

    } else if (changeTab.apportion && whereTab.objectVersion.rules.length > 0) {

        // Apportioning and where clause
        finalSql = calculateOverlayStringWithApportion(changeTab, true, whereTab)

    }

    return `UPDATE dataset${newLine}${finalSql};`
}

/**
 * A function that calculates the overlay SQL where there is no where clause and no apportionment. There is an option
 * to include or exclude the variable assignment, this is so that this function can be used to calculate the SQL for
 * overlays that include where clauses.
 *
 * @example This produces a string like:
 *
 * 10 + pd_12m AS pd_12m
 *
 * @param changeTab {ChangeTab} The object containing information about an overlay which can be converted into an SQL string.
 * @param includeAsPart - Whether to add 'AS <variable>' at the end. Being able to control this means that the
 * function can be used in multiple ways.
 * @param valueToAssign {void|string} When this is set then this function is being used as part of the calculation of an overlay
 * with a where clause but not apportionment. The default value '10 + pd_12m' needs to be replaced with a where clause
 * statement that can be passed through as an argument.
 * @returns - - The overlay SQL clause.
 */
const calculateOverlayBasicFormWithoutWhere = (changeTab: ChangeTab, includeAsPart: boolean, valueToAssign: void | string) => {

    const {direction, overlayVariable, value} = changeTab

    // First value property is the overlay variable option selected, the second is the value of the option
    const overlayVariableString = overlayVariable != null ? overlayVariable.value : "<variable>"

    const delimiter = overlayVariable?.details.schema && (isTracString(overlayVariable.details.schema.fieldType) || isTracDateOrDatetime(overlayVariable.details.schema.fieldType)) ? "'" : ""

    let valueString = isOption(value) && value.value != null ? value.value.toString() : value != null ? (typeof value === "number" && direction?.value === "percent" ? value + 1 : value).toString() : "<value>"

    let directionString: string = " "

    if (direction == null) {
        directionString = " <type> "
    } else if (direction.value === "=") {
        directionString = ""
    } else if (direction.value === "level") {
        directionString = " + "
    } else if (direction.value === "percent") {
        directionString = " * "
    }

    let directionVariableString: string

    if (direction?.value === "=") {
        directionVariableString = ""
    } else {
        directionVariableString = `${overlayVariableString}`
    }

    // You only have to round the percent calculation, the '=' and 'level' are already going to create an integer as the input always
    // returns an integer value
    const integerRoundEnd = direction?.value === "percent" && overlayVariable?.details.schema.fieldType === trac.INTEGER ? ", 0) " : " "
    const integerRoundStart = direction?.value === "percent" && overlayVariable?.details.schema.fieldType === trac.INTEGER ? "ROUND(" : ""

    const finalString = valueToAssign == null ? `${integerRoundStart}${delimiter}${valueString}${delimiter}${directionString}${directionVariableString}${integerRoundEnd}` : valueToAssign

    return `${includeAsPart ? `SET ${overlayVariableString} =` : ""} ${finalString}`
}

/**
 * A function that calculates the overlay SQL where there is a where clause. This can handle both with and without a where
 * apportionment.
 *
 * @example This produces a string like:
 *
 * CASE
 *    WHEN <variable> = <value> THEN 0.1 * pd_12m
 *    ELSE pd_12m
 * END AS pd_12m
 *
 * @param changeTab {ChangeTab} The object containing information about an overlay which can be converted into an SQL string.
 * @param whereTab {WhereTab} The object containing information about a where clause which can be converted into an SQL string.
 * @param valueToAssign - The value to assign in the where clause case statement e.g. 'WHEN X < 0 THEN Y' this the 'Y'
 * part. If it is not passed as an argument then we calculate it, it is passed through there is an apportionment being
 * used, and we need to add more logic to calculate the string to used to include the apportionment logic.
 * @returns - The overlay SQL clause.
 */
const calculateOverlayBasicFormWithWhere = (changeTab: ChangeTab, whereTab: WhereTab, valueToAssign: void | string) => {

    const {overlayVariable} = changeTab

    // The overlay variable option selected
    const overlayVariableString = overlayVariable != null ? overlayVariable.value : "<variable>"

    // The string to use in the case string, in 'when <x> then <y>' this gives the <y> part. Because this is the
    // same string as an overlay string for a non-apportioned, non-where clause overlay, so we just use that
    // function unless a value is passed in.
    const caseString = valueToAssign == null ? calculateOverlayBasicFormWithoutWhere(changeTab, false) : valueToAssign

    return `SET ${overlayVariableString} = ${caseString}${newLine}WHERE ${whereTab.stringVersion}`
}

/**
 * A function that calculates the overlay apportion string, this is the denominator needed to calculate the additional value to each row.
 * @param changeTab {ChangeTab} The object containing information about an overlay which can be converted into an SQL string.
 * @param whereTab {ChangeTab} The object containing information about an overlay which can be converted into an SQL string.
 * @param whichVariable {apportion | overlay} Whether to use the overlay variable or apportion variable. This is needed because it is
 * used in two cases, first to calculate the denominator in calculating the apportioning ratio i.e. in 'A/SUM(A)' this is the 'SUM(A)' part.
 * Second, it is used to calculate the size of the overlay when it is defined as a percentage of the overlay variable. For example, if we need
 * to calculate a 10% increase as 0.1 * SUM(B) where B is the overlay variable we can use this function but need to be able to swap to the
 * overlay variable.
 * @returns - - The overlay SQL clause.
 */
const calculateOverlayApportionDenominatorString = (changeTab: ChangeTab, whereTab: void | WhereTab, whichVariable: "apportion" | "overlay"): string => {

    const {apportionVariable, overlayVariable} = changeTab

    // See function notes
    const apportionVariableString = whichVariable === "apportion" ? (apportionVariable != null ? apportionVariable.value : "<variable>") : (overlayVariable != null ? overlayVariable.value : "<variable>")

    // If there is no where clause then the apportionment denominator is a simple sum on the apportionment column
    if (!whereTab) {

        return `(select SUM(${apportionVariableString}) from dataset)`
    }

    // If there is a where clause and an apportionment then the denominator has to have case where logic to
    // ensure that the sum value corresponds to the accounts that the apportionment is being applied to
    return `${newLine}${newLine}${tab}(select SUM(${apportionVariableString}) WHERE ${whereTab.stringVersion} from dataset)${newLine}${newLine}`
}

/**
 * A function that calculates the overlay SQL where there is apportionment. This can handle both where there is
 * a where clause and where there is not. There is an option to include or exclude the variable assignment, this
 * is so that this function can be used to calculate the SQL for overlays that include where clauses.
 *
 * @example Apportionment with a where clause:
 *
 * CASE
 *    WHEN date = '2022-11-10' THEN pd_12m + (0.1 *
 *
 *    (select SUM(
 *        CASE
 *            WHEN date = '2022-11-10' THEN pd_12m
 *            ELSE 0
 *        END
 *    ) from dataset)
 *
 *   * pd_lifetime /
 *
 *    (select SUM(
 *        CASE
 *            WHEN date = '2022-11-10' THEN pd_lifetime
 *            ELSE 0
 *        END
 *    ) from dataset)
 *
 *    )
 *    ELSE pd_12m
 * END AS pd_12m AS pd_12m
 *
 * @example: Apportionment without a where clause:
 *
 * pd_12m + (1000 * pd_lifetime / (select SUM(pd_lifetime) from dataset)) AS pd_12m
 */
const calculateOverlayStringWithApportion = (changeTab: ChangeTab, includeAsPart: boolean, whereTab: void | WhereTab) => {

    const {apportion, apportionVariable, direction, overlayVariable, value} = changeTab

    // The overlay variable option selected
    const overlayVariableString = overlayVariable != null ? overlayVariable.value : "<variable>"

    // The apportionment variable option selected
    const apportionVariableString = apportionVariable != null ? apportionVariable.value : "<variable>"

    // The denominator in the apportioning SQL
    let apportionSumString = calculateOverlayApportionDenominatorString(changeTab, whereTab, "apportion")

    let apportionAmountString = calculateOverlayApportionDenominatorString(changeTab, whereTab, "overlay")

    let valueString: string
    if (isOption(value) && value.value != null && direction?.value === "level") {
        valueString = value.value.toString()
    } else if (isOption(value) && value.value != null && direction?.value === "percent") {
        valueString = `${value.value.toString()} * ${apportionAmountString} `
    } else if (!isOption(value) && value && direction?.value === "level") {
        valueString = value.toString()
    } else if (!isOption(value) && value && direction?.value === "percent") {
        valueString = `${value.toString()} * ${apportionAmountString} `
    } else {
        valueString = "<value>"
    }

    let directionString: string
    if (direction?.value === "=") {
        directionString = " "
    } else if (direction?.value === "level") {
        directionString = " + "
    } else if (direction?.value === "percent" && !apportion) {
        directionString = " * "
    } else if (direction?.value === "percent" && apportion) {
        // If you are apportioning then a percent increase is still an amount added on, but that amount is calculated as a percent of the total
        // e.g. pd_12m + (0.1 * (select SUM(pd_12m) from dataset)  * pd_lifetime / (select SUM(pd_lifetime) from dataset)) AS pd_12m
        directionString = " + "
    } else {
        directionString = "<overlay>"
    }

    const integerRoundStart = overlayVariable?.details.schema.fieldType === trac.INTEGER ? "ROUND(" : ""
    const integerRoundEnd = overlayVariable?.details.schema.fieldType === trac.INTEGER ? ", 0)" : ""

    const valueToAssign = `${overlayVariableString}${directionString}(${integerRoundStart}${valueString} * ${apportionVariableString} / ${apportionSumString}${integerRoundEnd})`

    if (whereTab) {
        return `${calculateOverlayBasicFormWithWhere(changeTab, whereTab, valueToAssign)}`
    } else {
        return `${calculateOverlayBasicFormWithoutWhere(changeTab, true, valueToAssign)}`
    }
}

/**
 * A function that returns an object containing the default values of all the options. This is
 * used when the user selects a new dataset to overlay into the overlay builder. A new object is stored.
 */
export type ChangeTab = {
    direction: SingleValue<Option<"=" | "percent" | "level", BasicTypeDetails>>,
    apportion: boolean,
    // A list of all TRAC basic types
    fieldTypes: trac.BasicType[]
    // On load the change is invalid as no variable is selected
    // The variable to change
    overlayVariable: SingleValue<Option<string, SchemaDetails>>
    // The value for the overlay
    value: SelectPayload<Option, false>["value"]
    // The variable to apportion by
    apportionVariable: SingleValue<Option<string, SchemaDetails>>
    isValid: boolean
    validationChecked: boolean
    validation: { overlayVariable: boolean, direction: boolean, apportionVariable: boolean, value: boolean, description: boolean }
    description: null | string,
    id: string
}

export type Overlay = {
    inputData: {
        // The key of the overlay, in a flow this will be a flow output node key
        overlayKey: null | string,
        // The schema of the output dataset
        schema: null | trac.metadata.IFieldSchema[],
        allFieldTypes: trac.BasicType[]
    },
    changeTab: ChangeTab[],
    editor: {}
}

// We have separate functions for setting up each of the properties of the overlay object, this gives us finer control over resetting it, for example if the
// user cocks up and wants to reset the interface we do not want to remove the information about their selected dataset.
function setupInitialQueryInputData(): Overlay["inputData"] {
    return {
        overlayKey: null,
        schema: null,
        allFieldTypes: Types.tracBasicTypes.map(dataType => dataType.type)
    }
}

function setupInitialQueryForChangeTab(): ChangeTab {
    return {
        direction: {value: "level", label: "Change level by", details: {basicTypes: [trac.FLOAT, trac.INTEGER, trac.DECIMAL]}},
        fieldTypes: Types.tracBasicTypes.map(dataType => dataType.type),
        overlayVariable: null,
        // Note null is a valid value to set the variable to in the overlay
        value: null,
        apportionVariable: null,
        apportion: false,
        isValid: false,
        validationChecked: false,
        validation: {overlayVariable: false, direction: true, apportionVariable: true, value: true, description: false},
        description: null,
        id: nanoid(10)
    }
}

function setupInitialQueryForEditor(): Overlay["editor"] {
    return {}
}

export function setupInitialOverlay(): Overlay {

    return {
        inputData: setupInitialQueryInputData(),
        changeTab: [],
        editor: setupInitialQueryForEditor()
    }
}

// Define a type for the slice state
export interface OverlayBuilderStoreState {

    currentlyLoaded: {
        storeKey: null | keyof OverlayBuilderStoreState["uses"], overlayKey: null | string
    },
    uses: {
        runAFlow: {
            change: Record<string, Overlay>
        }
    }
}

// If you need to extend the application to use the tool in a new page or component then simply create a new storeKey object and
// load that property in the new scene. The existing reducers all the key (called storeKey) to set values.
const initialState: OverlayBuilderStoreState = {

    currentlyLoaded: {
        storeKey: null, overlayKey: null
    },
    // Copy this if you want to add a new use case
    uses: {
        runAFlow: {
            change: {},
        }
    }
}

export const OverlayBuilderStore = createSlice({

    name: 'overlayBuilderStore',
    initialState: initialState,
    reducers: {
        /**
         * A reducer that resets the store to its initial value.
         */
        resetStore: () => initialState,
        /**
         * A reducer that resets the store to its initial value for an individual use case.
         */
        resetOverlaysForSingleUser: (state, action: PayloadAction<{ storeKey: keyof OverlayBuilderStoreState["uses"] }>) => {

            const {storeKey} = action.payload

            state.uses[storeKey].change = {}
        },
        /**
         * A reducer that sets the socket (data) of the output dataset selected by the user to overlay. So for example if the
         * overlay is for an output dataset called "pd_forecast" this is the value of overlayKey.
         */
        setCurrentlyLoadedOverlay: (state, action: PayloadAction<{ storeKey: keyof OverlayBuilderStoreState["uses"], overlayKey?: null | string }>) => {

            const {storeKey, overlayKey} = action.payload

            if (overlayKey != null) {
                if (state.currentlyLoaded.storeKey != storeKey) state.currentlyLoaded.storeKey = storeKey
                if (state.currentlyLoaded.overlayKey != overlayKey) state.currentlyLoaded.overlayKey = overlayKey
            }
        },
        /**
         * A reducer that adds a section to allow an overlay to be created by the user for their selected dataset. By creating
         * a new entry keyed by the node and socket of the dataset the user can move between datasets and recover their overlays.
         */
        createOverlayEntry: (state, action: PayloadAction<{ storeKey: keyof OverlayBuilderStoreState["uses"], overlayKey?: null | string, schema: trac.metadata.IFieldSchema[] }>) => {

            // This checks whether there is a property in the store for the user selected dataset and
            // if not creates an entry to be able to build an SQL query for it
            const {storeKey, overlayKey, schema} = action.payload

            if (overlayKey != null && !state.uses[storeKey].change.hasOwnProperty(overlayKey)) {

                // It's an array as a dataset can have many overlays defined for it
                state.uses[storeKey].change[overlayKey] = setupInitialOverlay()
                // Store the schema of the dataset as described by the model
                state.uses[storeKey].change[overlayKey].inputData.schema = schema
                state.uses[storeKey].change[overlayKey].inputData.overlayKey = overlayKey
            }
        },
        /**
         * A reducer that removes the currently selected variable for the overlay statement, this can be used where the user
         * makes a change to the overlay definition that means the current variable is no longer applicable.
         */
        resetSelectVariable: (state, action: PayloadAction<number>) => {

            const {overlayKey, storeKey} = state.currentlyLoaded
            const overlayIndex = action.payload

            if (storeKey != null && overlayKey != null) {

                const changeTab = state.uses[storeKey].change[overlayKey].changeTab[overlayIndex]

                // Set the validation message to off
                changeTab.validationChecked = false

                // Remove the variable
                changeTab.overlayVariable = null
                changeTab.validation.overlayVariable = false
            }
        },
        /**
         * A reducer that sets the field types to filter by.
         */
        setFieldTypeButton: (state, action: PayloadAction<ButtonPayload & { fieldTypes: trac.BasicType[] }>) => {

            const {overlayKey, storeKey} = state.currentlyLoaded
            const {fieldTypes, id: overlayIndex} = action.payload

            if (storeKey != null && overlayKey != null && typeof overlayIndex == "number") {

                console.log(overlayIndex)
                const changeTab = state.uses[storeKey].change[overlayKey].changeTab[overlayIndex]

                // If we are updating the field type filters then this is just the value, if not then we have a toggle
                changeTab["fieldTypes"] = fieldTypes

                // fieldType with a fallback
                const fieldType = changeTab.overlayVariable?.details.schema.fieldType || trac.BasicType.BASIC_TYPE_NOT_SET

                // If the user changes the variable filter but has selected a variable that is not in the filtered list
                // then we remove the variable selected
                if (changeTab.overlayVariable && !fieldTypes.includes(fieldType)) {
                    OverlayBuilderStore.caseReducers.resetSelectVariable(state, {payload: overlayIndex, type: "resetSelectVariable"})
                }
            }
        },
        /**
         * A reducer that sets an option for the overlayVariable. If overlayVariable changes type then we need to also look
         * at the direction, changeType and apportionVariable options again to see if they are still valid.
         */
        setOverlayVariable: (state, action: PayloadAction<DeepWritable<SelectOptionPayload<Option<string, SchemaDetails>, false>>>) => {

            const {overlayKey, storeKey} = state.currentlyLoaded
            const {value, isValid, id: overlayIndex} = action.payload

            if (storeKey != null && overlayKey != null && typeof overlayIndex == "number") {

                const changeTab = state.uses[storeKey].change[overlayKey].changeTab[overlayIndex]

                // Get the current fieldType of the selected variable e.g. INTEGER and the operator e.g. =
                const oldFieldType = changeTab.overlayVariable?.details.schema?.fieldType

                changeTab.overlayVariable = value
                changeTab.validation.overlayVariable = isValid

                // Set the validation message to off if they change something
                changeTab.validationChecked = false

                // If the user changes the variable to one where the selected direction is not valid
                // the remove that direction selection e.g. if it's a STRING, increase makes no sense
                if (value && changeTab.direction != null && !changeTab.direction?.details.basicTypes.includes(value?.details?.schema.fieldType || trac.BasicType.BASIC_TYPE_NOT_SET)) {

                    const defaultOperator = configuration.directionOptions.find(directionOption => directionOption.value === "=")

                    changeTab.direction = defaultOperator || null
                    changeTab.validation.direction = !!defaultOperator
                }

                // If the user changes the variable to one that is not a number then apportionment is not possible
                if (value && !isTracNumber(value?.details.schema.fieldType)) {
                    changeTab.apportionVariable = null
                    changeTab.validation.apportionVariable = true
                    changeTab.apportion = false
                }

                // If the user changed the variable type then the value they are using, for example A = 5, may be invalid
                // for the new type, here we check this and try and convert it to the new type. If the value is invalid
                // we blank it.

                // Has there been a change in field type
                if ((oldFieldType || trac.STRING) !== value?.details.schema.fieldType) {

                    // If there is a value for the overlay set, it's not an option, and we know the new field type
                    if (changeTab.value != null && isPrimitive(changeTab.value) && value?.details.schema.fieldType) {

                        if (oldFieldType != null) {
                            // Try to convert the value to an equivalent value for the field type e.g. INTEGER -> STRING could be 5 => '5'
                            changeTab.value = convertDataBetweenTracTypes(oldFieldType, value.details.schema.fieldType, changeTab.value)

                        } else {
                            // If the user sets a value before they set the overlay variable oldFieldType will not exist, so we still
                            // want to keep the value when they set the overlay variable, here we set the existing value as a string
                            // which is safe and then try and convert to the new overlay variable type.
                            changeTab.value = convertDataBetweenTracTypes(trac.STRING, value.details.schema.fieldType, changeTab.value.toString())
                        }

                        // Update the validation for the value if it was nulled
                        if (changeTab.value == null) {
                            changeTab.validation.value = false
                        }

                    } else {

                        // This is for when the value is set via an option and the fieldType changes
                        changeTab.value = null
                        changeTab.validation.value = false
                    }
                }
            }
        },
        /**
         * A reducer that sets the direction option for the overlay.
         */
        setDirection: (state, action: PayloadAction<SelectOptionPayload<Option<"=" | "level" | "percent", BasicTypeDetails>, false>>) => {

            const {overlayKey, storeKey} = state.currentlyLoaded
            const {value, isValid, id: overlayIndex} = action.payload

            if (storeKey != null && overlayKey != null && typeof overlayIndex == "number") {

                const changeTab = state.uses[storeKey].change[overlayKey].changeTab[overlayIndex]

                // Add in the new value
                changeTab.direction = value
                changeTab.validation.direction = isValid

                // If the user changes the direction to a level change we may need to remove the variable if it is not a number
                if (!value?.details.basicTypes.includes(changeTab.overlayVariable?.details.schema.fieldType || trac.BasicType.BASIC_TYPE_NOT_SET)) {
                    changeTab.overlayVariable = null
                    changeTab.validation.overlayVariable = false
                }

                // If the user changes the direction to be equal to a value then apportionment can not be applied
                if (value?.value === "=") {
                    changeTab.apportion = false
                    changeTab.apportionVariable = null
                    changeTab.validation.apportionVariable = true
                }
            }
        },
        /**
         * A reducer that sets whether to apportion the overlay by a variable in the dataset.
         */
        toggleApportion: (state, action: PayloadAction<SelectTogglePayload>) => {

            const {overlayKey, storeKey} = state.currentlyLoaded
            const {value, id: overlayIndex} = action.payload

            if (storeKey != null && overlayKey != null && typeof overlayIndex == "number") {

                const changeTab = state.uses[storeKey].change[overlayKey].changeTab[overlayIndex]

                // Add in the new value
                changeTab.apportion = value || false
                changeTab.apportionVariable = null
            }
        },
        /**
         * A reducer that sets an option for the overlayVariable. If overlayVariable changes type then we need to also look
         * at the direction, changeType and apportionVariable options again to see if they are still valid.
         */
        setApportionVariable: (state, action: PayloadAction<DeepWritable<SelectOptionPayload<Option<string, SchemaDetails>, false>>>) => {

            const {overlayKey, storeKey} = state.currentlyLoaded
            const {value, isValid, id: overlayIndex} = action.payload

            if (storeKey != null && overlayKey != null && typeof overlayIndex == "number") {

                const changeTab = state.uses[storeKey].change[overlayKey].changeTab[overlayIndex]

                changeTab.apportionVariable = value
                changeTab.validation.apportionVariable = isValid

                // Set the validation message to off if they change something
                changeTab.validationChecked = false
            }
        },
        /**
         * A reducer that adds an overlay to the corresponding overlayKey.
         */
        addOverlay: (state) => {

            const {overlayKey, storeKey} = state.currentlyLoaded

            if (storeKey != null && overlayKey != null) {

                // Add an overlay for the overlayKey
                const overlays = state.uses[storeKey].change[overlayKey].changeTab
                overlays.push(setupInitialQueryForChangeTab())
            }
        },
        /**
         * A reducer that sets the variable to use in a where clause rule. e.g. where balance > 0, here you are
         * setting the balance variable.
         */
        setValue: (state, action: PayloadAction<DeepWritable<SelectPayload<Option, false>>>) => {

            const {overlayKey, storeKey} = state.currentlyLoaded
            const {id: overlayIndex, isValid, value} = action.payload

            if (storeKey != null && overlayKey != null && typeof overlayIndex === "number") {

                // Get an overlay for the overlayKey
                const overlay = state.uses[storeKey].change[overlayKey].changeTab[overlayIndex]

                overlay.value = value
                overlay.validation.value = isValid
            }
        },
        /**
         * A reducer that deletes an overlay corresponding overlayKey.
         */
        deleteOverlay: (state, action: PayloadAction<ConfirmButtonPayload>) => {

            const {overlayKey, storeKey} = state.currentlyLoaded
            const {index: overlayIndex} = action.payload

            if (storeKey != null && overlayKey != null) {

                // Remove the overlay
                state.uses[storeKey].change[overlayKey].changeTab = state.uses[storeKey].change[overlayKey].changeTab.filter((item, i) => i !== overlayIndex);
            }
        },
        /**
         * A reducer that sets the variable to use in a where clause rule. e.g. where balance > 0, here you are
         * setting the balance variable.
         */
        setDescription: (state, action: PayloadAction<SelectValuePayload<string>>) => {

            const {overlayKey, storeKey} = state.currentlyLoaded
            const {id: overlayIndex, isValid, value} = action.payload

            if (storeKey != null && overlayKey != null && typeof overlayIndex === "number") {

                // Store the user set description in the store
                state.uses[storeKey].change[overlayKey].changeTab[overlayIndex].description = value

                state.uses[storeKey].change[overlayKey].changeTab[overlayIndex].validation.description = isValid
            }
        }
    }
})


// Action creators are generated for each case reducer function
export const {

    addOverlay,
    createOverlayEntry,
    deleteOverlay,
    resetOverlaysForSingleUser,
    resetSelectVariable,
    setApportionVariable,
    setCurrentlyLoadedOverlay,
    setDescription,
    setDirection,
    setFieldTypeButton,
    setOverlayVariable,
    setValue,
    toggleApportion
} = OverlayBuilderStore.actions

export default OverlayBuilderStore.reducer