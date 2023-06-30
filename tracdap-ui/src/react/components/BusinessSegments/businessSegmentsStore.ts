/**
 * This slice acts as the store for what business segment filters to apply to various scenes in TRAC-UI using the
 * {@link BusinessSegmentMenu} component. For example when running a flow the user can filter the flows to choose
 * from by business segment.
 *
 * @category Redux store
 * @module businessSegmentsStore
 */

import {convertRowIntoOption, isOptionSame} from "../../utils/utils_general";
import {createSlice, PayloadAction} from '@reduxjs/toolkit'
import {isDefined, isKeyOf} from "../../utils/utils_trac_type_chckers";
import {Option, SelectOptionPayload, StoreStatus, UiBusinessSegmentsDataRow} from "../../../types/types_general";
import {SingleValue} from "react-select";
import {convertStringToInteger} from "../../utils/utils_string";
import {makeArrayOfObjectsUnique} from "../../utils/utils_arrays";

/**
 * An interface for the object containing all the options for each level. The main benefit of defining this in
 * its own interface is that we can get the keys of the interface and use them as the type for other parts of
 * the store, for example we can use 'keyof OptionsByLevel'.
 */
export interface OptionsByLevel {
    level_1: undefined | Option<string>[],
    level_2: undefined | Option<string>[],
    level_3: undefined | Option<string>[],
    level_4: undefined | Option<string>[]
}

/**
 * An interface for the businessSegmentsStore.ts Redux store.
 */
export interface BusinessSegmentsStoreState {

    businessSegments: {
        /**
         * An option that allows 'All' business segments to be selected.
         */
        allOption: Option<string>
        /**
         * All the options for each business segment level. These are derived from the business segment definitions
         * which are owned by the {@link applicationSetupStore}.
         */
        masterOptions: Record<keyof OptionsByLevel, SingleValue<Option<string>>>[]
         /**
         * A message associated with the {@link status}. This is not used currently.
         */
        message?: string
        /** The status of the process to convert the business segments dataset owned by the {@link applicationSetupStore} into
         * a set of options. This is needed because if the application state is reset (due to a change in tenant for
         * example) then we need to have a hook to recalculate them. The value of this status is used as that hook.
         */
        status: StoreStatus
        /**
         * The levels that have options set for them.
         *
         * @example
         * ["level_1", "level_2", "level_3"]
         */
        levels: (keyof OptionsByLevel)[]
        /**
         * The maximum number of business segments to show, this is based on which levels the user
         * has set options for.
         */
        maxNumberOfSegments: 1 | 2 | 3 | 4
    }
    /**
     * The different uses of this store across the application. Each use has its own state.
     */
    uses: Record<"runAFlow" | "runAModel",
        {
            /**
             * Whether the business segments should be hierarchical (the options update when you change a higher level) or
             * flat (the options update when you change any level).
             */
            type: "flat" | "hierarchical"
            /**
             * Each use case has their own set of options which reflect what selections they have made within the
             * hierarchy.
             */
            options: OptionsByLevel
            /**
             * The selected option for each level.
             */
            selectedOptions: Record<keyof OptionsByLevel, SingleValue<Option<string>>>
            /**
             *  Used to work out which set of options need to be updated after a change.
             */
            lastLevelChanged: undefined | keyof OptionsByLevel
        }>
}

// This is the initial state of the store.

// If you need to extend the application to use business segments in a new page or component then simply copy the initial
// state to a new property and load that property in the new scene. The existing reducers all use a key (called storeKey)
// to set values. You will need to update the type interface for the store above too.
const initialState: BusinessSegmentsStoreState = {

    businessSegments: {
        allOption: {value: "ALL", label: "All"},
        levels: [],
        maxNumberOfSegments: 4,
        message: undefined,
        masterOptions: [],
        status: 'idle'
    },
    uses: {
        runAFlow: {
            lastLevelChanged: undefined,
            options: {level_1: undefined, level_2: undefined, level_3: undefined, level_4: undefined},
            selectedOptions: {level_1: null, level_2: null, level_3: null, level_4: null},
            type: "flat"
        },
        runAModel: {
            lastLevelChanged: undefined,
            options: {level_1: undefined, level_2: undefined, level_3: undefined, level_4: undefined},
            selectedOptions: {level_1: null, level_2: null, level_3: null, level_4: null},
            type: "flat"
        }
    }
}

export const businessSegmentsStore = createSlice({
    name: 'businessSegmentsStore',
    initialState: initialState,
    reducers: {

        /**
         * A reducer that takes the business segment definitions and processes it into a set of options for the
         * SelectOption component. There is a hook in {@link BusinessSegmentMenu} that runs this reducer
         * when the business segments option dataset changes.
         */
        processBusinessSegments: (state, action: PayloadAction<UiBusinessSegmentsDataRow[]>) => {

            state.businessSegments.status = "succeeded"

            // We will add an 'All' option to each level
            const {allOption} = state.businessSegments

            // We are going to convert the business segments dataset into a four column array where each
            // column is an option (or null) ... so [{level_1: {value: "x1", label: "y1"}... {level_4: {value: "x4", label: "y4"}}...]
            // We start by adding the 'All' options first in one row of the dataset
            const masterOptions: { [key in keyof OptionsByLevel]: SingleValue<Option<string>> }[] = [{
                level_1: allOption,
                level_2: allOption,
                level_3: allOption,
                level_4: allOption
            }]

            // This is a count of the non 'All' options available
            const newOptionsPerLevel: number[] = [0, 0, 0, 0]

            // For each row of the business segments dataset
            action.payload.forEach(row => {

                // Add a row to the dataset options
                let newRow: { [key in keyof OptionsByLevel]: SingleValue<Option<string>> } = {
                    level_1: null,
                    level_2: null,
                    level_3: null,
                    level_4: null
                }

                // Fill in the options from the data
                for (let group = 1; group <= 4; group++) {

                    const level = `level_${group}`
                    // In Typescript, we tell it that the ID column (the values in the options) can be a string or null
                    // The convertRowIntoOption guarantees that only string values come back in the option
                    if (isKeyOf(newRow, level)) {

                        newRow[level] = convertRowIntoOption<string | null>(row, `GROUP_0${group}_ID`, `GROUP_0${group}_NAME`)

                        // Count the non-null options only
                        if (newRow[level] != null) {
                            newOptionsPerLevel[group - 1]++
                        }
                    }
                }

                masterOptions.push(newRow)
            })

            // Put the master options in the store
            state.businessSegments.masterOptions = masterOptions

            // Find those levels (given by their position) that have options
            const levelsWithOptions = newOptionsPerLevel.map((count, i) => count > 0 ? i + 1 : null).filter(isDefined)

            // Create a tuple with their original index and the position in the array - this is the mapping to use.
            // Remove mappings that don't change anything
            const levelMapping = levelsWithOptions.map((index, i) => [index, i + 1]).filter(tuple => tuple[0] !== tuple[1])

            // This remaps the business segment options to the left, so if the user set up level 1 and level 3 options
            // in the ApplicationSetup scene we don't have to show a segment selector for level 2 with just an 'All'
            // option, instead this remapping will move level 3 into level 2
            if (levelMapping.length > 0) {

                masterOptions.map(row => {

                    let newRow = {...row}
                    levelMapping.forEach(tuple => {

                        const wasLevel = `level_${tuple[1]}`
                        const tobeLevel = `level_${tuple[0]}`

                        if (isKeyOf(newRow, wasLevel) && isKeyOf(newRow, tobeLevel)) {
                            newRow[tobeLevel] = row[wasLevel]
                            newRow[wasLevel] = null
                        }
                    })

                    return newRow
                })
            }

            // This 'if' is a Typescript test
            if (levelsWithOptions.length === 1 || levelsWithOptions.length === 2 || levelsWithOptions.length === 3 || levelsWithOptions.length === 4) state.businessSegments.maxNumberOfSegments = levelsWithOptions.length

            // Create a list of the levels that need to be processed each update, this is also the number of
            // select boxes to show
            let levels: (keyof OptionsByLevel)[] = []

            for (let i = 1; i <= levelsWithOptions.length; i++) {
                let level = `level_${i}`
                // We know that masterOptions will have at least one row for the 'All' options
                if (isKeyOf(masterOptions[0], level)) levels.push(level)
            }

            // Always show at least one option box with 'All' in it if there are no business segments configured
            if (levels.length === 0) {
                levels.push(`level_1`)
            }

            // Levels here tells us how many select boxes to show in the component
            state.businessSegments.levels = levels

            // So now that we have the master options list we need to put the options and the selected values
            // in each use case, the default is the all option
            Object.keys(state.uses).forEach(storeKey => {

                // This if is just a Typescript typeguard
                if (isKeyOf(state.uses, storeKey)) {

                    // Depending on what type of menu we are showing to the user we initialise the options
                    // in a hierarchical layout we set the options as the user selects items. When a flat layout is used
                    // we fill all the options up
                    if (state.uses[storeKey].type === "flat") {

                        Object.keys(state.uses[storeKey].options).forEach(level => {

                            if (isKeyOf(state.uses[storeKey].options, level) && isKeyOf(state.uses[storeKey].selectedOptions, level)) {

                                state.uses[storeKey].options[level] = makeArrayOfObjectsUnique(state.businessSegments.masterOptions.map(row => row[level]).filter(isDefined))
                                // If a component remounts we do not want to reset the option to 'all' unless there is no option
                                if (!state.uses[storeKey].selectedOptions[level]) state.uses[storeKey].selectedOptions[level] = allOption
                            }
                        })

                    } else {

                        state.uses[storeKey].options.level_1 = makeArrayOfObjectsUnique(state.businessSegments.masterOptions.map(row => row.level_1).filter(isDefined))
                        // If a component remounts we do not want to reset the option to 'all' unless there is no option
                        if (!state.uses[storeKey].selectedOptions.level_1) state.uses[storeKey].selectedOptions.level_1 = allOption
                    }
                }
            })
        },
        /**
         * A reducer that runs when the user selects a new business segment for any level. It
         * gets the current business segment options and selections from the store and updates each level. This
         * means that the options update to reflect the selections further up in the tree. At the end of updating
         * the selects it triggers a callback function if one is available.
         */
        recalculateSegmentOptions: (state, action: PayloadAction<SelectOptionPayload<Option<string>, false>>) => {

            // The level of the business segment updated by the user & which part of the store we need to update
            const {name: level, storeKey, value} = action.payload

            // We will use the allOption as a default option
            const {allOption} = state.businessSegments

            // Protection for Typescript
            if (storeKey === undefined || !isKeyOf(state.uses, storeKey)) {

                throw new Error(`The storeKey is not a valid key of the businessSegmentsStore.uses`)
            }

            // Protection for Typescript
            if (typeof level !== "string" || !isKeyOf(state.uses[storeKey].options, level)) {

                throw new Error(`The level ${level} is not a valid key of the businessSegmentsStore.uses[${storeKey}]`)
            }

            // Record the last level changed
            state.uses[storeKey].lastLevelChanged = level

            // Set the updated value, if they cleared the value then replace with 'All'
            state.uses[storeKey].selectedOptions[level] = value || allOption

            // Get the number from the level string
            const levelAsNumber = convertStringToInteger(level)

            // This is the list of potential options we have to update
            const levelsToUpdate = state.businessSegments.levels

            const {type} = state.uses[storeKey]

            // If a flat menu then if we have not changed the first level we set everything upstream to 'All'
            // We can't guess the hierarchy that would be needed to get to their
            if (type === "flat") {

                for (let i = 1; i < levelAsNumber; i++) {

                    let levelAsString = `level_${i}`

                    // If a value has not been selected put the 'All' option in if available
                    if (isKeyOf(state.uses[storeKey].selectedOptions, levelAsString) && state.uses[storeKey].selectedOptions[levelAsString] == null) {

                        state.uses[storeKey].selectedOptions[levelAsString] = allOption
                    }
                }
            }

            // Update the options for each level
            levelsToUpdate.forEach(levelToUpdateAsString => {

                // Get the full business segment list
                let newOptions = [...state.businessSegments.masterOptions]

                // Iteratively filter down the master business segment list by all the levels selected prior and including
                // the one changed - this gives the valid rows to use to create the options for the
                // levels after the one that was changed
                for (let j = 1; j <= levelAsNumber; j++) {

                    let levelJAsString = `level_${j}`

                    if (isKeyOf(state.uses[storeKey].selectedOptions, levelJAsString)) {

                        let selectedOption = state.uses[storeKey].selectedOptions[levelJAsString]

                        newOptions = newOptions.filter((row) => {

                            const masterOptionsRowItem = isKeyOf(row, levelJAsString) ? row[levelJAsString] : undefined

                            // Keep the options when
                            // 1. There is no selected item so keep them all
                            // 2. The selected option is the 'All' one
                            // 3. The option in the list is the 'All' one
                            // 4. The selected option and the option in the list are not the 'All' one, but they are the same
                            return selectedOption == null || selectedOption.value === "ALL" || (masterOptionsRowItem && (masterOptionsRowItem.value === "ALL" || masterOptionsRowItem.value === selectedOption.value))
                        })
                    }
                }

                // Put the options calculated in the store, the filter is because some the items are null
                state.uses[storeKey].options[levelToUpdateAsString] = makeArrayOfObjectsUnique(newOptions.map(row => row[levelToUpdateAsString]).filter(isDefined))

                // Now we have to work out if the selected values for the levels we are updating are
                // still valid, if they are we keep them, if not then we remove them

                // See if it is in the new options for the level
                const isOptionStillAvailable = (state.uses[storeKey].options[levelToUpdateAsString] || []).find(row => isOptionSame(row, state.uses[storeKey].selectedOptions[levelToUpdateAsString]))

                // Note that we do not autofill 'All' for hierarchical menus as for these we need to call the function
                // to search for the relevant items in TRAC when the last select in the hierarchical version is changed.
                // So we should not automatically populate it.
                state.uses[storeKey].selectedOptions[levelToUpdateAsString] = isOptionStillAvailable ? isOptionStillAvailable : type === "flat" ? allOption : null
            })
        }
    }
})

// Action creators are generated for each case reducer function
export const {
    processBusinessSegments,
    recalculateSegmentOptions,
} = businessSegmentsStore.actions

export default businessSegmentsStore.reducer