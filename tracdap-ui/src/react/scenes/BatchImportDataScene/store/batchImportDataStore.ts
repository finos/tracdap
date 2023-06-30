/**
 * This slice acts as the store for the {@link BatchImportDataScene}.
 * @module batchImportDataStore
 * @category Redux store
 */

import {createAsyncThunk, createSlice, PayloadAction} from '@reduxjs/toolkit';
import {isDefined} from "../../../utils/utils_trac_type_chckers";
import {RootState} from "../../../../storeController";
import {SelectDatePayload, SelectTogglePayload, StoreStatus, UiBatchImportDataRow} from "../../../../types/types_general";
import {setTracValue, showToast, sOrNot, wasOrWere} from "../../../utils/utils_general";
import {submitJobs} from "../../../utils/utils_trac_api";
import {toast} from "react-toastify";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the batchImportDataStore.ts Redux store.
 */
export interface batchImportDataStoreState {

    selectedDates: Record<string, null | string>
    selectedImportData: Record<string, true>
    validation: {
        isValid: Record<string, boolean>, validationChecked: boolean
    }
    importJobs: {
        status: StoreStatus
    }
}

// This is the initial state of the store.
const initialState: batchImportDataStoreState = {

    selectedDates: {},
    selectedImportData: {},
    validation: {
        isValid: {}, validationChecked: false
    },
    importJobs: {
        status: "idle"
    }
}

/**
 * A function that takes the selected batch import datasets and created a data import job for each and
 * submits them to TRAC.
 */
export const runImportJobs = createAsyncThunk<// Return type of the payload creator
    PromiseSettledResult<trac.api.IJobStatus>[],
    // First argument to the payload creator
    void,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('batchImportDataStore/runImportJobs', async (_, {getState}) => {

    // Get what we need from the store
    const {"trac-tenant": tenant} = getState().applicationStore.cookies

    // Get what we need from the store
    const {data} = getState().applicationSetupStore.tracItems.items.ui_batch_import_data.currentDefinition
    const {selectedImportData} = getState().batchImportDataStore

    // Get an array of the datasets that the user has selected to load
    const selectedDatasetKeys = Object.keys(selectedImportData)

    // TODO do search for datasets to see if loaded already here
    // TODO we will need a way to tag the datasets created with tags

    // Create an array of API function calls, one for each dataset to be loaded
    let jobs = data.filter(row => row.DATASET_ID != null && selectedDatasetKeys.includes(row.DATASET_ID)).map(row => {

        const date = row.DATASET_ID !== null ? selectedImportData[row.DATASET_ID] : null

        // TODO there is not an import data defined in the TRAC API yet
        const jobRequest: trac.api.JobRequest = trac.api.JobRequest.create({
            tenant: tenant,
            job: {
                jobType: trac.JobType.IMPORT_DATA
            },
            jobAttrs: [
                {
                    attrName: "key",
                    value: setTracValue(trac.STRING, "data_import")
                },
                {
                    attrName: "name",
                    // TODO Convert this to the regex
                    value: setTracValue(trac.STRING, `Import of dataset '${row.DATASET_NAME}' (${row.DATASET_ID}) ${date ? "for '${date}'" : ""}for '${date}'`)
                },
                {
                    attrName: "description",
                    value: setTracValue(trac.STRING, `Import of dataset with ID '${row.DATASET_ID}' this is transferred from '${row.DATASET_SOURCE_SYSTEM}'`)
                },
                {
                    attrName: "import_method",
                    // TODO add to attributes table and set these in the component not here?
                    value: setTracValue(trac.STRING, "user_interface")
                },
                {
                    attrName: "show_in_search_results",
                    value: setTracValue(trac.BOOLEAN, true)
                }
            ]
        })

        // Add in the business segments for the job if they are set
        if (row.BUSINESS_SEGMENTS !== null) {

            const items = row.BUSINESS_SEGMENTS.split("||").map(businessSegment => setTracValue(trac.STRING, businessSegment))

            // Wrap the array of values into an array type attribute
            jobRequest.jobAttrs.push({
                attrName: "business_segments",
                value: {
                    type: {basicType: trac.BasicType.ARRAY, arrayType: {basicType: trac.STRING}},
                    arrayValue: {items: items}
                }
            })
        }

        return jobRequest
    })

    return await submitJobs(jobs)
})

export const batchImportDataStore = createSlice({
    name: 'batchImportDataStore',
    initialState: initialState,
    reducers: {
        /**
         * A reducer that resets the state of the store to its initial value.
         */
        resetStore: () => initialState,
        /**
         * A reducer that adds a dataset to the import batch, the only parameter that can be set is
         * the date of the data to import.
         */
        addOrRemoveImportDataFromBatch: (state, action: PayloadAction<SelectTogglePayload>) => {

            // name is the unique ID for the dataset to load, id is the frequency of the data which tells
            // us whether a null date is allowed
            const {id: frequency, name, value} = action.payload

            // Appease the Typescript gods
            if (typeof name !== "string") return

            // Add the dataset ID to the batch job
            if (value === true && !state.selectedImportData.hasOwnProperty(name)) {

                state.selectedImportData[name] = true
                // If the frequency is undefined then one is not needed, so when adding it to the
                // batch means it will be valid (the user isn't shown a widget to set one). If the
                // frequency is not undefined then we need one for the import to be valid, so we check
                // if one has been set.
                state.validation.isValid[name] = Boolean(frequency == null || state.selectedDates[name] != null)

            } else if (value !== true) {

                delete state.selectedImportData[name]
                delete state.validation.isValid[name]
            }
        },
        /**
         * A reducer that runs when the user makes a change to the definitions of the batch data imports in
         * the ApplicationSetupScene. This makes sure that the selected datasets are still valid. The user
         * could remove or delete options or change information about the date needed to import tge data.
         */
        reconcileToBatchImportDataUpdate: (state, action: PayloadAction<UiBatchImportDataRow[]>) => {

            // Don't do anything if nothing has been selected
            if (Object.keys(state.selectedImportData).length === 0) return

            const {payload: data} = action

            // What have we selected
            const selectedDatasetIds = Object.keys(state.selectedImportData)
            // What can now be selected
            const newListOfDatasetIds = data.map(row => row.DATASET_ID)
            // What is available but disabled
            const newListOfDisabledDatasetIds = data.filter(row => row.DISABLED).map(row => row.DATASET_ID)

            // If items we have added to our batch import have been deleted or disabled then we need to
            // remove them from our list
            const selectionsToRemove = selectedDatasetIds.filter(item => !newListOfDatasetIds.includes(item))
            const selectionsToTurnOff = selectedDatasetIds.filter(item => newListOfDisabledDatasetIds.includes(item))

            if (selectionsToRemove.length > 0 || selectionsToTurnOff.length > 0) {
                [...new Set([...selectionsToRemove, ...selectionsToTurnOff])].forEach(item => delete state.selectedImportData[item])
            }

            // If a data load has a missing DATASET_FREQUENCY field we take that as not needing a date set.
            // So we set the date to null and say it's valid. We do this here because if the user edits the
            // list and reloads the data we need any changes in DATASET_FREQUENCY to be reflected
            data.forEach(row => {

                if (row.DATASET_ID !== null && Object.keys(state.selectedImportData).includes(row.DATASET_ID)) {

                    if (row["DATASET_FREQUENCY"] === null && state.selectedDates[row.DATASET_ID] === null) {

                        // If it is now null and either it was already or the user has not set anything make
                        // sure its seen as valid
                        state.validation.isValid[row.DATASET_ID] = true

                    } else if (row["DATASET_FREQUENCY"] === null && state.selectedDates[row.DATASET_ID] !== null) {

                        // Null the existing value and say that this is now valid
                        state.selectedDates[row.DATASET_ID] = null
                        state.validation.isValid[row.DATASET_ID] = true

                    } else if (row["DATASET_FREQUENCY"] !== null && state.selectedImportData[row.DATASET_ID] === null) {

                        // If the dataset needs a frequency but one has not been set make sure that this is seen as invalid
                        state.validation.isValid[row.DATASET_ID] = false
                    }
                }
            })
        },
        /**
         * A reducer that sets the date associated with the dataset to import, so you may say load the September 2019 snapshot. The
         * application will convert this date to the right format to identify the dataset and initiate the import.
         */
        setDate: (state, action: PayloadAction<SelectDatePayload>) => {

            const {isValid, name, value} = action.payload

            // Appease the Typescript gods
            if (typeof name !== "string") return

            state.selectedDates[name] = value
            state.validation.isValid[name] = isValid
        },
        /**
         * A reducer that sets whether to show the validation messages for the dates for each upload.
         */
        setValidationChecked: (state, action: PayloadAction<boolean>) => {

            state.validation.validationChecked = action.payload
        }
    },
    extraReducers: (builder) => {

        // A set of lifecycle reducers to run before/after the runImportJobs function
        builder.addCase(runImportJobs.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.importJobs.status = "pending"
        })
        builder.addCase(runImportJobs.fulfilled, (state, action: PayloadAction<PromiseSettledResult<trac.api.IJobStatus>[]>) => {

            state.importJobs.status = "succeeded"

            const numberOfSelections = Object.keys(state.selectedImportData).length

            // The indices of the requests that failed
            const errors: number[] = action.payload.map((response, i) => response.status === "rejected" ? i : undefined).filter(isDefined)

            const succeededRequests: trac.api.IJobStatus[] = action.payload.map(response => response.status === "fulfilled" ? response.value : undefined).filter(isDefined)

            // We track each if the jobs by using Promise.allSettled, so we can message based on how many failed or succeeded, jobs
            // that successfully get submitted can fail afterwards
            if (errors.length === 0) {

                const text = {
                    title: "All of your imports have been successfully submitted.",
                    message: `The jobs to batch import ${numberOfSelections} dataset${sOrNot(numberOfSelections)} into TRAC ${wasOrWere(numberOfSelections)} successfully started. You can see progress in the 'Find a job' page.`,
                    details: succeededRequests.map(item => item.jobId?.objectId).filter(isDefined)
                }

                showToast("success", text, "batchImportDataStore/runImportJobs/fulfilled")

            } else if (errors.length < succeededRequests.length) {

                const text = {
                    title: "Not all of your imports have been successfully submitted.",
                    message: `${errors.length} of your ${numberOfSelections} batch imports have failed, the progress of the successfully submitted tasks can still be viewed in the 'Find a job'  page.`,
                    details: succeededRequests.map(item => item.jobId?.objectId).filter(isDefined)
                }

                showToast("warning", text, "batchImportDataStore/runImportJobs/fulfilled")

            } else if (errors.length === succeededRequests.length) {

                showToast("error", {
                    message: "All of your batch imports have failed, this may mean that there is an issue with the application",
                }, "batchImportDataStore/runImportJobs/fulfilled")

            }
        })
        builder.addCase(runImportJobs.rejected, (state, action) => {

            state.importJobs.status = "failed"

            const numberOfSelections = Object.keys(state.selectedImportData).length
            const text = {
                title: "Failed to batch import the data",
                message: `The jobs to batch import the ${numberOfSelections} dataset${sOrNot(numberOfSelections)} did not complete successfully.`,
                details: action.error.message
            }

            showToast("error", text, "batchImportDataStore/runImportJobs/rejected")
            console.error(action.error)
        })
    }
})

// Action creators are generated for each case reducer function
export const {
    addOrRemoveImportDataFromBatch,
    reconcileToBatchImportDataUpdate,
    setDate,
    setValidationChecked
} = batchImportDataStore.actions

export default batchImportDataStore.reducer