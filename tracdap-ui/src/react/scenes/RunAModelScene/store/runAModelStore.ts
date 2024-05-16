/**
 * This slice acts as the store for the {@link RunAModelScene}.
 * @module runAModelStore
 * @category Redux store
 */

import {
    buildParametersForSingleModel,
    categoriseModelInputTypes,
    createModelInputOptionsTemplate,
    createModelSelectedInputOptionsTemplate,
    getInputCountFromModel,
    setProcessedInputs,
    setProcessedParameters,
    validateModelSelections
} from "../../../utils/utils_flows";
import {BusinessSegmentsStoreState} from "../../../components/BusinessSegments/businessSegmentsStore";
import {
    ButtonPayload,
    DeepWritable,
    GenericGroup,
    JobInputsByCategory,
    ListsOrTabs,
    OnCreateNewOptionPayload,
    Option,
    OptionLabels,
    SearchOption,
    SelectOptionPayload,
    SelectPayload,
    StoreStatus,
    TracGroup,
    ValidationOfModelInputs
} from "../../../../types/types_general";
import {checkForBatchMetadata, checkForMetadata} from "../../../store/applicationStore";
import cloneDeep from "lodash.clonedeep";
import {convertArrayToOptions} from "../../../utils/utils_arrays";
import {convertSearchResultsIntoOptions, extractValueFromTracValueObject, sortOptionsByObjectTimeStamp} from "../../../utils/utils_trac_metadata";
import {createAsyncThunk, createSlice, PayloadAction} from '@reduxjs/toolkit';
import {createTag, createTagsFromAttributes, extractDefaultValues} from "../../../utils/utils_attributes_and_parameters";
import {getGroupOptionIndex, goToTop, hideDuplicatedGroupOptions, showToast, updateObjectOptionLabels} from "../../../utils/utils_general";
import {hasOwnProperty, isDefined, isGroupOption, isKeyOf, isObject, isTagOptionArray} from "../../../utils/utils_trac_type_chckers";
import {resetAttributesToDefaults, setAttributesAutomatically} from "../../../components/SetAttributes/setAttributesStore";
import {RootState} from "../../../../storeController";
import {searchByBusinessSegments, searchByMultipleAndClauses, submitJob} from "../../../utils/utils_trac_api";
import {SingleValue} from "react-select";
import {toast} from "react-toastify";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {UiAttributesProps} from "../../../../types/types_attributes_and_parameters";
import {setupRerun} from "../../RunAFlowScene/store/runAFlowStore";

/**
 * An interface for the 'inputs' entry point in the store state.
 */
export interface Inputs {

    // The full definition of the inputs
    inputDefinitions: Record<string, UiAttributesProps>
    // A list of the inputs that are optional, required and support
    inputTypes: JobInputsByCategory
    // What was the last input updated by the user, used as a rendering optimisation
    lastInputChanged: null | string
    // Whether the inputs are in lists that have headers or in tabs
    listsOrTabs: ListsOrTabs
    // Any message associated with the API request to get the selected data option.
    message: undefined | string
    // The input option selected by the user
    selectedInputOptions: Record<string, SingleValue<SearchOption>>
    // Whether to show keys rather than node labels in the UI menu, seeing the keys helps sometimes
    showKeysInsteadOfLabels: boolean
    // The status of the API request to get the selected data option.
    status: StoreStatus
}

// A function that initialises the 'models' entry point in the store state
function setupInputs(): Inputs {
    return {
        inputDefinitions: {},
        inputTypes: {required: [], optional: [], categories: {}},
        lastInputChanged: null,
        listsOrTabs: "lists",
        message: undefined,
        selectedInputOptions: {},
        showKeysInsteadOfLabels: true,
        status: 'idle'
    }
}

/**
 * An interface for the 'model' entry point in the store state.
 */
export interface Model {
    // Whether the job needs to be built, changing the model or asking to re-run a model sets this to false
    jobHasBeenBuilt: boolean
    // Any message associated with the API request to get the metadata object.
    message: undefined | string,
    // The metadata for the chosen model
    modelMetadata: undefined | trac.metadata.ITag
    // The number of API calls needed to build the model, this is used to show the progress bar
    numberOfApiCalls: { toDo: number, completed: number }
    // The status of the API request to get the metadata object.
    status: StoreStatus
    // Has the user changed the configuration away from the default
    userChanged: { something: boolean, what: { input: boolean, parameter: boolean } },
}

// A function that initialises the 'model' entry point in the store state
function setupModel(): Model {
    return {
        jobHasBeenBuilt: false,
        message: undefined,
        modelMetadata: undefined,
        numberOfApiCalls: {toDo: 0, completed: 0},
        status: 'idle',
        userChanged: {something: false, what: {input: false, parameter: false}},
    }
}

/**
 * An interface for the 'models' entry point in the store state.
 */
export interface Models {
    // Any message associated with the API request to get the list of models.
    message: undefined | string
    // The metadata that comes from TRAC that contains models that match the search criteria. We also collect the
    // ones that the user has added manually by pasting an object ID or key and those that we had to load in order
    // to rerun a job.
    modelOptions: TracGroup
    // The status of the API request to get the list of metadata.
    status: StoreStatus
    // User selected model from the list
    selectedModelOption: SingleValue<SearchOption>
}

// A function that initialises the 'models' entry point in the store state
function setupModels(): Models {
    return {
        message: undefined,
        modelOptions: [
            {label: "Added by user", options: []},
            {label: "Loaded for re-run", options: []},
            {label: "Search results", options: []}
        ],
        selectedModelOption: null,
        status: 'idle'
    }
}

/**
 * An interface for the 'parameters' entry point in the store state.
 */
export interface Parameters {
    // What was the last parameter updated by the user, used as a rendering optimisation
    lastParameterChanged: null | string
    // The full definition of the parameters
    parameterDefinitions: Record<string, UiAttributesProps>
    // Whether to show parameter labels or the key in the model setup
    showKeysInsteadOfLabels: boolean
    // The values set for the parameters
    values: Record<string, SelectPayload<Option, boolean>["value"]>
}

// A function that initialises the 'parameters' entry point in the store state
function setupParameters(): DeepWritable<Parameters> {

    return ({
        lastParameterChanged: null,
        parameterDefinitions: {},
        showKeysInsteadOfLabels: false,
        values: {}
    })
}

/**
 * An interface for the 'validation' entry point in the store state.
 */
export interface Validation {
    // In summary can the job be run
    canRun: boolean
    // Do the selected input datasets have schemas that match the model definition
    modelInputSchemasNotMatchingModelInputs: ValidationOfModelInputs
    // validation status of the individual selected/set values for the job, excluding attributes
    isValid: {
        inputs: Record<string, boolean>, parameters: Record<string, boolean>
    }
    // Has the validation for the individual selected/set values for the job been run, if so we will be showing any validation errors
    validationChecked: boolean
    // Do input options exist for all non-optional datasets in the chain (any added in for re-run or manually added count)
    nonOptionalInputOptionsExistForAll: boolean,
}

// A function that initialises the 'validation' entry point in the store state
export function setupValidation(): Validation {

    return {
        canRun: true,
        modelInputSchemasNotMatchingModelInputs: {},
        isValid: {inputs: {}, parameters: {}},
        validationChecked: false,
        nonOptionalInputOptionsExistForAll: true
    }
}

/**
 * An interface for the RunAModelStoreState Redux store.
 */
export interface RunAModelStoreState {

    // Whether the user is viewing the setup or review page
    setUpOrReview: "setup" | "review"
    // How to format the labels in the options, these are stored outside the main equivalent parts of the store
    // so that if a new model is selected these options are retained
    optionLabels: {
        models: OptionLabels
        inputs: OptionLabels
    }
    // Information about a job we might be trying to rerun
    rerun: {
        job: undefined | trac.metadata.ITag,
        jobParameters: undefined | Record<string, null | string | boolean | number | Option | Option[]>
        jobInputs: undefined | Record<string, SingleValue<SearchOption>>
    }
    models: Models,
    model: Model
    parameters: Parameters
    inputs: Inputs,
    validation: Validation
    job: {
        // Any message associated with the API request to  kick off the job.
        message: undefined | string
        // The status of the API request to kick off the job.
        status: StoreStatus
    }
}

// This is the initial state of the store. Note attributes are handled in the setAttributesStore.
// Note that those areas without a setup function are designed to persist if an option is changed
// by the user, they are 'session settings'.
const initialState: RunAModelStoreState = {

    // Whether the user is viewing the setup or review page
    setUpOrReview: "setup",
    // How to format the labels in the options, these are stored outside the main equivalent parts of the store
    // so that if a new model is selected these options are retained
    optionLabels: {
        inputs: {
            showObjectId: false,
            showVersions: false,
            showCreatedDate: false,
            showUpdatedDate: true
        },
        models: {
            showObjectId: false,
            showVersions: false,
            showCreatedDate: false,
            showUpdatedDate: true
        }
    },
    models: setupModels(),
    model: setupModel(),
    // Information about a job we might be trying to rerun
    rerun: {
        job: undefined,
        jobParameters: undefined,
        jobInputs: undefined
    },
    parameters: setupParameters(),
    inputs: setupInputs(),
    validation: setupValidation(),
    job: {
        message: undefined,
        status: "idle",
    }
}

/**
 * A function that searches TRAC for models that match the selected business segments.
 */
export const getModels = createAsyncThunk<// Return type of the payload creator
    SearchOption[],
    // First argument to the payload creator
    { storeKey: keyof BusinessSegmentsStoreState["uses"] } | ButtonPayload,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAModelStore/getModels', async (payload, {getState}) => {

    // The storeKey is needed to get the business segments selected by the user from the businessSegmentsStore
    const storeKey = hasOwnProperty(payload, "storeKey") ? payload.storeKey : payload.name

    // Protection for Typescript
    if (typeof storeKey !== "string") {
        throw new Error(`The storeKey value '${storeKey}' was not a string`)
    }

    if (!isKeyOf(getState().businessSegmentsStore.uses, storeKey)) {
        throw new Error(`The storeKey ${storeKey} is not a valid key of the businessSegmentsStore.uses`)
    }

    // Get the parameters we need from the store
    const {cookies: {"trac-tenant": tenant}, tracApi: {searchAsOf}, clientConfig: {searches: {maximumNumberOfOptionsReturned}}} = getState().applicationStore

    // Get the selected options for each business segment level and also which levels we are using.
    const {uses: {[storeKey]: {selectedOptions}}, businessSegments: {levels}} = getState().businessSegmentsStore

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")

    // Put the selected business segment options into an array and filter, the isDefined is there to make Typescript
    // happy that there are no null values
    const options = levels.map(level => selectedOptions[level]).filter(option => option !== null && option.value !== null && option.value.toUpperCase() !== "ALL").filter(isDefined)

    // Initiate the search for models by business segments
    const searchResults = await searchByBusinessSegments({
        maximumNumberOfOptionsReturned,
        objectType: trac.ObjectType.MODEL,
        options,
        searchAsOf,
        tenant
    })

    // The parameters that specify how the labels in the options should be formed
    const {showCreatedDate, showObjectId, showUpdatedDate, showVersions} = getState().runAModelStore.optionLabels.models

    // Return the options with the formatted labels
    return convertSearchResultsIntoOptions(searchResults, showObjectId, showVersions, showUpdatedDate, showCreatedDate, false)
})

/**
 * A function that stores the selected model option into the store. It also clears any attributes set by the user.
 *
 * @remarks We have to make this a thunk rather than a reducer because we need to access the reducers from other slices. The only
 * way to run these reducers is with dispatch which is not available in a reducer but is available using a thunk.
 */
export const setModel = createAsyncThunk<// Return type of the payload creator
    SelectOptionPayload<SearchOption, false>,
    // First argument to the payload creator
    SelectOptionPayload<SearchOption, false>,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAModelStore/setModel', async (payload, {dispatch}) => {

    // Rest the attributes back to the default, if the user has changed the model then we reset the
    // attributes back to the default as they may be no longer valid, this clearing will not be
    // visible to the user as the attributes are on the review page and the widget to change the
    // model is on the setup page.
    dispatch(resetAttributesToDefaults({storeKey: "runAModel"}))

    // We want to set the name for the job as the name of the model as a starting point
    const modelName = payload.value?.tag.attrs?.name?.stringValue ?? null
    const businessSegments = extractValueFromTracValueObject(payload.value?.tag.attrs?.business_segments)

    // Update the job attribute
    dispatch(setAttributesAutomatically({storeKey: "runAModel", values: {name: modelName, business_segments: businessSegments.value ?? null}}))

    return payload
})

/**
 * A function that receives the selected input option, gets the metadata for it if needed and then passes
 * the updated option to the store.
 */
export const setInput = createAsyncThunk<// Return type of the payload creator
    SelectOptionPayload<SearchOption, false>,
    // First argument to the payload creator
    SelectOptionPayload<SearchOption, false>,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAModelStore/setInput', async (payload, {dispatch}) => {

    // We need to get the metadata for the new option if it has not already been retrieved.
    let tag: trac.metadata.ITag | undefined

    // See note below
    let newPayload = {...payload}

    // Only make the call for the metadata if the tag does not include the definition property which indicates that
    // we have the search result but not the actual metadata
    if (newPayload.value?.tag?.header && !newPayload.value.tag?.definition) {

        // Get the option metadata - this is using the client side metadata store
        tag = await dispatch(checkForMetadata(newPayload.value.tag.header)).unwrap()
    }

    // This is a complete faff, but we have to do it because Redux will throw an error if we mutate the read
    // only parts of the payload. So instead we have to create a completely new option object.
    // Don't copy the icon, so it does not appear in the selected option for inputs.
    if (newPayload.value) {
        newPayload.value = {
            value: newPayload.value.value,
            label: newPayload.value.label,
            tag: tag || newPayload.value.tag,
            disabled: newPayload.value.disabled
        }
    }

    return newPayload
})

/**
 * A function that searches for a model by its object ID and then carries out a series of additional API calls to
 * get lists of the available options for each input dataset. These are processed as arrays that can be directly placed into
 * {@link SelectOption} components. The function also takes the most recent option from the list of options and sets that
 * as the default to use in the job. The only case it does not try to assign a default to are the optional data inputs.
 */
export const buildRunAModelJobConfiguration = createAsyncThunk<// Return type of the payload creator
    {
        modelMetadata: undefined | trac.metadata.ITag
    },
    // First argument to the payload creator
    void,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAModelStore/buildRunAModelJobConfiguration', async (_, {getState, dispatch}) => {

    // Update the progress bar, first call is for the model definition
    dispatch(setNumberOfApiCalls(1))

    // Get what we need from the store
    const {selectedModelOption} = getState().runAModelStore.models
    const {rerun} = getState().runAModelStore

    // If the user did not set an option there is nothing to get, this can happen if the SelectModel component is
    // set as clearable and the user clears it
    if (!selectedModelOption || selectedModelOption.value == null) return {
        modelMetadata: {...setupModel().modelMetadata}
    }

    // If the option does not have a tag selector then we can't proceed
    if (selectedModelOption.tag.header == null) {
        throw new Error("The selected option does not have a tag selector")
    }

    // Get the model metadata - this is using the client side metadata store
    const modelMetadata: trac.metadata.ITag = await dispatch(checkForMetadata(selectedModelOption.tag.header)).unwrap()

    // If the model does not have a model definition then we can't proceed
    if (!modelMetadata.definition?.model) {
        throw new Error("The model option does not have a model definition to run")
    }

    // We will need to do a search for the options for each input (m), then for each we get the metadata for
    // the first options for all inputs as a single request (1). For all the inputs we may need to get the schemas of the
    // initially selected object as well, but again we can do this as a single request (1).
    let numberOfCalls = getInputCountFromModel(modelMetadata.definition.model) + 1 + 1

    // Update the progress bar
    dispatch(setNumberOfApiCalls(numberOfCalls))

    // Run the function to search for input datasets and handle the selections, this is a separate function, so it can be run independently
    await dispatch(getInputs({modelDefinition: modelMetadata.definition.model, modelAttrs: modelMetadata.attrs}))

    // Get the parameters from the selected models and set the default values
    dispatch(getParameters({isSettingUpRerun: Boolean(rerun.job !== undefined), modelMetadata, updateValidation: false}))

    return {
        // Model
        modelMetadata
    }
})

/**
 * A function that a model's metadata and then carries out a series of additional API calls to get lists of
 * the available options for input datasets. These are processed as arrays that can be directly placed into
 * {@link SelectOption} components. The function also takes the latest option from the list of options and
 * sets that as the default to use in the job, in some cases additional calls to get the schema are needed.
 * The only case it does not try to assign a default to are the optional data inputs.
 */
export const getInputs = createAsyncThunk<// Return type of the payload creator
    {
        // Inputs
        inputDefinitions: Inputs["inputDefinitions"]
        inputsThatWereNulled: string[]
        inputTypes: Inputs["inputTypes"]
        selectedInputOptions: Inputs["selectedInputOptions"]
        updateValidation: boolean
    },
    // First argument to the payload creator, there are two cases when this is run. First, as part of the
    // 'buildRunAModelJobConfiguration' function, which runs when the user selects a model and in which case the
    // model information needs to be passed in as it is not in the store. Second, when the user is
    // refreshing the list of inputs and the function is called from a generic component, in which case the
    // model details are not passed in but picked up from the store.
    void | {
    modelDefinition: trac.metadata.IModelDefinition,
    modelAttrs: trac.metadata.ITag["attrs"]
},
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAModelStore/getInputs', async (payload, {getState, dispatch}) => {

    // Get what we need from the store
    const {cookies: {"trac-tenant": tenant}, tracApi: {searchAsOf}, clientConfig: {searches: {maximumNumberOfOptionsReturned}}} = getState().applicationStore
    const {rerun} = getState().runAModelStore
    const {showVersions, showObjectId, showUpdatedDate, showCreatedDate} = getState().runAModelStore.optionLabels.inputs
    const {inputDefinitions: existingInputDefinitions, selectedInputOptions: existingSelectedInputOptions} = getState().runAModelStore.inputs

    // When there is no payload we are refreshing the list of inputs for a model that is already configured
    const isRefresh = payload === undefined

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")

    // Getting the model information depends on whether a new job is being set up (no payload) or we are updating an
    // already set up list of inputs
    const modelDefinition = !isRefresh ? payload?.modelDefinition : getState().runAModelStore.model.modelMetadata?.definition?.model
    const modelAttrs = !isRefresh ? payload?.modelAttrs : getState().runAModelStore.model.modelMetadata?.attrs

    if (modelDefinition == undefined) throw new Error("modelDefinition is undefined")
    if (modelAttrs == undefined) throw new Error("modelAttrs is undefined")

    // For each input we need to search for available options and then get the metadata for the default option
    // if one exists.
    const inputOptions = createModelInputOptionsTemplate(modelDefinition)

    // If the user loads a job to rerun and then changes model we want the inputs in the "LOADED FOR RERUN" to be still available.
    // This allows the user to pick a job to re-run and apply all the selections to a new (updated model). Same goes for the
    // "ADDED BY USER" inputs.
    Object.keys(inputOptions).forEach(key => {

        if (existingInputDefinitions.hasOwnProperty(key)) {
            const {options: existingOptions} = existingInputDefinitions[key]
            if (existingOptions !== undefined && isGroupOption(existingOptions)) {

                if (isTagOptionArray(existingOptions[0].options)) inputOptions[key][0].options = existingOptions[0].options
                if (isTagOptionArray(existingOptions[1].options)) inputOptions[key][1].options = existingOptions[1].options
            }
        }
    })

    const inputDataSearches = Object.keys(inputOptions).map(async (inputKey) => {

        // This is a default search by the key of the input
        const defaultSearchExpression: trac.metadata.ISearchExpression = {
            term: {
                attrName: "key",
                attrType: trac.BasicType.STRING,
                operator: trac.SearchOperator.EQ,
                searchValue: {
                    type: {basicType: trac.BasicType.STRING},
                    stringValue: inputKey
                }
            }
        }

        // Get the list of options from the key string.
        const terms: trac.metadata.ISearchExpression[] = [defaultSearchExpression]

        const results = await searchByMultipleAndClauses({
            includeOnlyShowInSearchResultsIsTrue: true,
            maximumNumberOfOptionsReturned,
            objectType: trac.ObjectType.DATA,
            searchAsOf,
            tenant,
            terms
        })

        // Update the progress bar (inside each async call)
        dispatch(updateCompletedApiCalls())

        // We sort by objectTimestamp rather than the default tagTimestamp so that when tags are changed the
        // order does not change
        return {type: "data", key: inputKey, results: results.sort(sortOptionsByObjectTimeStamp)}
    })

    // Execute the array of searches. Note that the returned value is an array with the objects inside having
    // results as an array of tags.
    const searchResults = await Promise.all(inputDataSearches)

    // We need to make a set of additional requests to get the metadata for the default options selected.
    // This list and its order needs to be kept in sync with inputBatchMetadataRequest
    let inputRequestTypes: { type: "data-new" | "data-rerun", key: string }[] = []

    // An array of tag headers that we need to get the full metadata for, for example if we are setting a default option we
    // need to get the definition of it, this list contains the header for the request. This list and its order needs to be
    // kept in sync with 'inputRequestTypes'
    let inputBatchMetadataRequest: trac.metadata.ITagSelector[] = []

    // WARNING
    // If you change the below code you need to make sure that you always key the objects by the key used by the model.
    // You should never key by the key of the input dataset as there is not always a 1-2-1 mapping.

    // The selected inputs and models to fill in with the default selections
    const selectedInputOptions = createModelSelectedInputOptionsTemplate(modelDefinition)
    const inputTypes = categoriseModelInputTypes(modelDefinition)
    // The undefined arguments are for when building from a flow, and you have things like the labels for the nodes available
    let inputDefinitions = setProcessedInputs(selectedInputOptions, inputTypes, undefined, modelAttrs)

    let schemasBatchMetadataRequest: trac.metadata.ITagSelector[] = []

    // We need to keep track of which inputs were nulled because they were not in the refreshed list, if these are
    // not optional then the validation for them should be turned to false
    let inputsThatWereNulled: string[] = []

    // Now go through the search results for each input in the model
    searchResults.forEach(searchResult => {

        // Results here is an array of tags
        const {key, results} = searchResult

        // Turn the inputs search results into options. tagVersion is not used in setting the option key so that
        // when re-running a job the options from the searches can match the object used in the job that is being
        // re-run even if the tag has been updated since.
        inputOptions[key][2].options = convertSearchResultsIntoOptions(results, showObjectId, showVersions, showUpdatedDate, showCreatedDate, false)

        // First handle the case where we are re-running a job
        if (rerun.job && !isRefresh) {

            // Get the tag selector of the object used (by the key in the model)
            const inputUsed = rerun.job.definition?.job?.runModel?.inputs?.[key]

            if (inputUsed != undefined) {

                // Add the header for the additional tag we need to a list
                inputBatchMetadataRequest.push(inputUsed)

                // Add the extra request to the list of requests to make
                inputRequestTypes.push({type: "data-rerun", key})
            }

            // The selected option is set when the metadata for it comes back
        }
        // Now handle where is a new run being set up, or a refresh of the options is happening
        else {

            // Has the user already selected an option, say for a model that they have just changed
            const alreadySelectedOption = existingSelectedInputOptions[key]

            // Does the existing option exist in the new search results
            const {index} = alreadySelectedOption != null ? getGroupOptionIndex(alreadySelectedOption, [inputOptions[key][2]]) : {index: -1}

            // If the user has already selected an item, then if this item still exists after changing the model or hitting the refresh button then
            // retain that as the selected option. If we are building a new model and the item does not exist then pick the first item in the list,
            // if we are refreshing the options and the selected item does not exist then null the selection.
            // We need to clone the alreadySelectedOption as it is read only, and we modify it with the tag later on
            const selectedOption = index > -1 ? cloneDeep(alreadySelectedOption) : (!isRefresh ? (inputOptions[key][2].options[0] || null) : null)

            // Save the selected option
            selectedInputOptions[key] = selectedOption

            if (alreadySelectedOption !== null && selectedOption === null) {
                inputsThatWereNulled.push(key)
            }

            // Get the tag for the selected input option, these won't be in the search results, so we have to make extra metadata requests
            // to get it
            if (selectedOption?.tag?.header && !selectedOption?.tag?.definition?.data) {

                // Add the header for the additional tag we need to a list
                inputBatchMetadataRequest.push(selectedOption?.tag?.header)

                // Add the extra request to the list of requests to make
                inputRequestTypes.push({type: "data-new", key})
            }
        }
    })

    // Get the input metadata - this is using the client side metadata store, this consists of the metadata
    // we need for the default selected options
    const inputMetadataResults = await dispatch(checkForBatchMetadata(inputBatchMetadataRequest)).unwrap()

    // Update the progress bar
    dispatch(updateCompletedApiCalls())

    // We need to make a set of additional requests to get the metadata for the schemas of input datasets where schema
    // objects were used rather than defined natively for the objects selected. This list and its order needs
    // to be kept in sync with schemasBatchMetadataRequest as it will be used to optimise the setting up of the job
    let schemaRequestTypes: { type: "schema", key: string, dataType: "data-rerun" | "data-new" }[] = []

    // So now we need to put the metadata into the options for the currently selected data inputs
    inputMetadataResults.forEach((result, i) => {

        if (inputRequestTypes[i].type === "data-rerun") {

            // Turn the search result into an option in a different part of the dropdown, we
            // don't have to add the tag into the tag property as this function will do this for us (we already got the full metadata
            // whereas in the search results we didn't)
            inputOptions[inputRequestTypes[i].key][1].options = convertSearchResultsIntoOptions([result], showObjectId, showVersions, showUpdatedDate, showCreatedDate, false)

            const selectedOption = cloneDeep(inputOptions[inputRequestTypes[i].key][1].options[0]) || null

            // Add an icon to the group to say that this is the option that matches the job to re-run, index 1 is the re-run section
            inputOptions[inputRequestTypes[i].key][1].options[0].icon = "bi-check-circle"

            // Set the option as the selected option, don't add an icon as inputs have the policy status displayed with the header
            selectedInputOptions[inputRequestTypes[i].key] = selectedOption

            // If the rerun option exists in the search list then we update the option there too
            if (selectedOption) {

                // Is the selected option in the re-run section (index 1), if so hide it in the search options (index 2)
                inputOptions[inputRequestTypes[i].key] = hideDuplicatedGroupOptions(selectedOption, inputOptions[inputRequestTypes[i].key], 1, 2)
            }

            // If the dataset was loaded using a schema then we need to get that in order to validate the
            // schema is OK
            if (result.definition?.data?.schemaId && !result.definition?.data?.schema) {

                // Check if we already requested the schema
                const schemaAlreadyRequested = schemasBatchMetadataRequest.find(schemaBatchRequest => schemaBatchRequest.objectId === result.definition?.data?.schemaId?.objectId && schemaBatchRequest.objectVersion === result.definition?.data?.schemaId?.objectVersion)

                if (schemaAlreadyRequested == undefined) {
                    schemasBatchMetadataRequest.push(result.definition?.data?.schemaId)

                    // Add the extra request to the list of requests to make
                    schemaRequestTypes.push({type: "schema", key: inputRequestTypes[i].key, dataType: "data-rerun"})
                }
            }

        } else if (inputRequestTypes[i].type === "data-new") {

            // Get the current selected option
            let selectedOption = cloneDeep(selectedInputOptions[inputRequestTypes[i].key]) || null

            // If there is an option selected
            if (selectedOption) {

                // Put the metadata tag into the option
                selectedOption.tag = result

                // If the dataset was loaded using a schema then we need to get that in order to validate the
                // schema is OK
                if (result.definition?.data?.schemaId && !result.definition?.data?.schema) {

                    // Check if we already requested the schema
                    const schemaAlreadyRequested = schemasBatchMetadataRequest.find(schemaBatchRequest => schemaBatchRequest.objectId === result.definition?.data?.schemaId?.objectId && schemaBatchRequest.objectVersion === result.definition?.data?.schemaId?.objectVersion)

                    if (schemaAlreadyRequested == undefined) {
                        schemasBatchMetadataRequest.push(result.definition?.data?.schemaId)

                        // Add the extra request to the list of requests to make
                        schemaRequestTypes.push({type: "schema", key: inputRequestTypes[i].key, dataType: "data-new"})
                    }
                }

                // Set the option as the selected option
                selectedInputOptions[inputRequestTypes[i].key] = selectedOption

                // Find the option in the option group, index 2 is the added for search section
                const {tier, index} = getGroupOptionIndex(selectedOption, [inputOptions[inputRequestTypes[i].key][2]])

                // If found then put the option with the tag added back into the list
                if (tier > -1 && index > -1) {
                    inputOptions[inputRequestTypes[i].key][2].options[index] = selectedOption
                }
            }
        }
    })

    // Get the input metadata - this is using the client side metadata store, this consists of the metadata
    // we need for the default selected options
    const schemaMetadataResults = await dispatch(checkForBatchMetadata(schemasBatchMetadataRequest)).unwrap()

    // Update the progress bar, we said there was one call for schemas so even if the array is empty and no request was sent
    // we need to update the progress by 1
    dispatch(updateCompletedApiCalls())

    schemaMetadataResults.forEach((schemaTag, i) => {

        // Get the key of the dataset that this schema was for, this does not have an icon
        const selectedOption = cloneDeep(selectedInputOptions[schemaRequestTypes[i].key]) || null

        // Check the schema that matches the selected item's metadata
        if (selectedOption?.tag?.definition?.data && selectedOption.tag.definition?.data?.schemaId?.objectId === schemaTag.header?.objectId && selectedOption.tag.definition?.data?.schemaId?.objectVersion === schemaTag.header?.objectVersion) {

            // Put the schema into the selected option
            selectedOption.tag.definition.data.schema = schemaTag.definition?.schema

            // Put the selected item into the added for re-rerun section
            if (schemaRequestTypes[i].dataType === "data-rerun") {

                inputOptions[schemaRequestTypes[i].key][1].options[0] = selectedOption
                // Reapply the icon as that is lost when we put the selected option into the section
                inputOptions[schemaRequestTypes[i].key][1].options[0].icon = "bi-check-circle"
            }

            // Set the option as the selected option
            selectedInputOptions[schemaRequestTypes[i].key] = selectedOption

            // Find the option in the option group
            const {tier, index} = getGroupOptionIndex(selectedOption, [inputOptions[schemaRequestTypes[i].key][2]])

            // If found then put the option with the tag added back into the list
            if (tier > -1 && index > -1) {
                inputOptions[schemaRequestTypes[i].key][2].options[index] = selectedOption
            }

            // Is the selected option in the re-run section (index 1), if so hide it in the search options (index 2)
            inputOptions[schemaRequestTypes[i].key] = hideDuplicatedGroupOptions(selectedOption, inputOptions[schemaRequestTypes[i].key], 1, 2)
        }
    })

    // Put the options for the input datasets into the input definitions
    isRefresh && Object.keys(inputDefinitions).forEach(key => {
        inputDefinitions[key].options = inputOptions[key]

        // If the user has selected to re-run a job and then refreshed the list of datasets then the new options here
        // can contain the same option, we want to avoid the duplication
        if (rerun.job && inputOptions[key][1].options?.[0] && isRefresh) {

            // Is the selected option in the re-run section (index 1), if so hide it in the search options (index 2)
            inputOptions[key] = hideDuplicatedGroupOptions(inputOptions[key][1].options[0], inputOptions[key], 1, 2)
        }
    })

    // Put the options for the input datasets into the input definitions
    Object.keys(inputDefinitions).forEach(key => {

        // If the user has selected to re-run a job and then refreshed the list of datasets then the new options here
        // can contain the same option, we want to avoid the duplication
        if (rerun.job && inputOptions[key][1].options?.[0] && isRefresh) {

            // If there is an item in the re-run section (index 1), if so hide it in the search options (index 2)
            inputOptions[key] = hideDuplicatedGroupOptions(inputOptions[key][1].options[0], inputOptions[key], 1, 2)
        }

        inputDefinitions[key].options = inputOptions[key]
    })

    return {
        inputDefinitions,
        inputsThatWereNulled,
        inputTypes,
        selectedInputOptions,
        // When the payload is not passed then the user is not building a job but refreshing the inputs.
        // When building a job the 'buildRunAModelJobConfiguration' function updates the validation, but when
        // refreshing we need to update it in this function.
        updateValidation: Boolean(payload === undefined)
    }
})


/**
 * A reducer that adds an option for an input to the list of options. This runs when the user pastes
 * an object ID or key into a SelectOption component for a model and passes the TRAC object information to this
 * function to handle. It gets the metadata for the model and adds that into the option.
 */
export const addInputOption = createAsyncThunk<// Return type of the payload creator
    { id: string, option: SearchOption },
    // First argument to the payload creator.
    OnCreateNewOptionPayload<trac.metadata.ITag>,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAModelStore/addInputOption', async (payload, {getState, dispatch}) => {

    // Get the variables that determine how to show the labels
    const {showObjectId, showVersions, showUpdatedDate, showCreatedDate} = getState().runAModelStore.optionLabels.inputs

    const {id, newOptions} = payload

    if (typeof id !== "string") throw new TypeError("The id passed to the 'addInputOption' function was not a string")

    if (newOptions.length === 0 || !newOptions[0].tag.header) throw new Error("No new option was passed to the 'addInputOption' function")

    const metadata = await dispatch(checkForMetadata(newOptions[0].tag.header)).unwrap()

    // Update the label to reflect how the user has asked for the labels to be shown
    const optionWithUpdatedLabel = updateObjectOptionLabels(newOptions, showObjectId, showVersions, showUpdatedDate, showCreatedDate)?.[0] ?? null

    optionWithUpdatedLabel.tag = metadata

    return {id, option: optionWithUpdatedLabel}
})

/**
 * A function that receives details of the job that the user has selected to run and put this is the store. It also creates some
 * other objects that list the models, inputs and parameters that can be passed to the {@link ParameterMenu} for it to use to
 * show if the selected value matches the policy.
 *
 * @remarks We have to make this a thunk rather than a reducer because we need to access the reducers from other slices. The only
 * way to run these reducers is with dispatch which is not available in a reducer but is available using a thunk.
 */
export const setJobToRerun = createAsyncThunk<// Return type of the payload creator
    { job: trac.metadata.ITag, model: trac.metadata.ITag },
    // First argument to the payload creator
    { job: trac.metadata.ITag, model: trac.metadata.ITag },
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAModelStore/setJobToRerun', async (payload, {dispatch}) => {

    // Rest the attributes back to the default, if the user has changed the model then we reset the
    // attributes back to the default as they may be no longer valid, this clearing will not be
    // visible to the user as the attributes are on the review page and the widget to change the
    // model is on the setup page.
    dispatch(resetAttributesToDefaults({storeKey: "runAModel"}))

    // We want to set the name for the job as the name of the job that is being rerun
    const jobName = payload.job?.attrs?.name?.stringValue ?? null
    const businessSegments = extractValueFromTracValueObject(payload.job.attrs?.business_segments)

    // Update the job attribute
    dispatch(setAttributesAutomatically({storeKey: "runAModel", values: {name: jobName, business_segments: businessSegments.value ?? null}}))

    return payload
})

/**
 * A function that initiates a job in TRAC  it gets the information for the model, collates all the parameters,
 * and inputs the user selected and converts them into the format required by the TRAC API.
 */
export const runJob = createAsyncThunk<// Return type of the payload creator
    trac.api.IJobStatus,
    // First argument to the payload creator
    void,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAModelStore/runJob', async (_, {dispatch, getState}) => {

    // Get what we need from the store
    const {"trac-tenant": tenant} = getState().applicationStore.cookies

    // First extract the parameters
    const {values: parameterValues, parameterDefinitions} = getState().runAModelStore.parameters

    let parameters: Record<string, trac.metadata.IValue> = {}

    Object.entries(parameterDefinitions).forEach(([key, value]) => {

        const tag = createTag(key, value.basicType, parameterValues[key])
        if (tag.value) parameters[key] = tag.value
    })

    // Next extract the inputs
    const {selectedInputOptions} = getState().runAModelStore.inputs

    let inputs: Record<string, trac.metadata.ITagSelector> = {}

    Object.entries(selectedInputOptions).forEach(([key, value]) => {

        if (value?.tag?.header) inputs[key] = value.tag.header
    })

    // Next get the model
    const {selectedModelOption} = getState().runAModelStore.models

    if (selectedModelOption == null) throw new Error("No model is selected to run")

    // Next get the attributes
    const {processedAttributes, values} = getState().setAttributesStore.uses.runAModel.attributes

    const jobAttrs = createTagsFromAttributes(processedAttributes, values)

    // If the user set the job tag then extract that into a separate tag array to set on all the outputs from the job
    const outputAttrs = typeof values.job_tag === "string" ? createTagsFromAttributes({"job_tag": processedAttributes.job_tag}, {"job_tag": values.job_tag}) : undefined

    const jobRequest = {
        tenant: tenant,
        job: {
            jobType: trac.JobType.RUN_MODEL,
            runModel: {
                model: selectedModelOption.tag.header,
                inputs,
                parameters,
                outputAttrs
            }
        },
        jobAttrs: jobAttrs,
    }

    // This is a bit of hackery or magic not sure which. We want to return the result from the
    // successful submission, but we also want to clear the attributes the user entered.
    return await submitJob(jobRequest).then((result) => {
        dispatch(resetAttributesToDefaults({storeKey: "runAModel"}))
        return result
    });
})

export const runAModelStore = createSlice({
    name: 'runAModelStore',
    initialState: initialState,
    reducers: {
        /**
         * A reducer that resets the store to its original state, this runs when the user changes the business
         * segment searches.
         */
        resetState: () => initialState,
        /**
         * A reducer that changes the scene to review mode, where all the information for the job is shown. "_"
         * is an unused variable.
         */
        reviewJob: (state) => {

            goToTop()
            state.setUpOrReview = "review"
        },
        /**
         * A reducer that changes the scene to set up mode, where all the various inputs/parameters can be changed.
         */
        setUpJob: (state) => {
            goToTop()
            state.setUpOrReview = "setup"
        },
        /**
         * A reducer that sets whether to show the inputs in tabs or in a headed list.
         */
        setListsOrTabs: (state) => {

            state.inputs.listsOrTabs = state.inputs.listsOrTabs === "lists" ? "tabs" : "lists"
        },
        /**
         * A reducer that sets whether to show the input keys in the menu or the labels.
         */
        setShowKeys: (state, action: PayloadAction<{ name: "inputs" | "parameters" }>) => {

            state[action.payload.name].showKeysInsteadOfLabels = !state[action.payload.name].showKeysInsteadOfLabels
        },
        /**
         * A reducer that sets whether to show the validation messages for the job selections, usually when the user sets to move to review.
         */
        setValidationChecked: (state, action: PayloadAction<{ value: boolean }>) => {

            const {value} = action.payload
            state.validation.validationChecked = value
        },
        /**
         * A reducer that sets the number of API calls are needed to build the model. This is used to show progress to
         * the user.
         */
        setNumberOfApiCalls: (state, {payload}: PayloadAction<number>) => {

            state.model.numberOfApiCalls = {toDo: payload, completed: 0}
        },
        /**
         * A reducer that updates how many of the API calls have completed when building the model. This is used to show
         * progress to the user.
         */
        updateCompletedApiCalls: (state, {payload}: PayloadAction<undefined | number>) => {

            state.model.numberOfApiCalls.completed = state.model.numberOfApiCalls.completed + (payload ? payload : 1)
        },
        /**
         * A reducer that runs when the user changes one of the options in the Toolbar component to change how the
         * option labels show (e.g. showing the model versions). The name and id are used to set the property in the
         * store to update. After the change the labels on all the relevant options in the store and the selected option
         * are updated.
         */
        updateOptionLabels: (state, action: PayloadAction<{ name: "models" | "inputs", id: "showObjectId" | "showVersions" | "showCreatedDate" | "showUpdatedDate" }>) => {

            const {id, name} = action.payload
            state.optionLabels[name][id] = !state.optionLabels[name][id]

            if (name === "models") {

                // Get the variables that determine how to show the labels
                const {showObjectId, showVersions, showUpdatedDate, showCreatedDate} = state.optionLabels.models

                // If the user changes how they want the labels to display we update all the options
                state.models.modelOptions[0].options = updateObjectOptionLabels(state.models.modelOptions[0].options, showObjectId, showVersions, showUpdatedDate, showCreatedDate)
                state.models.modelOptions[1].options = updateObjectOptionLabels(state.models.modelOptions[1].options, showObjectId, showVersions, showUpdatedDate, showCreatedDate)
                state.models.modelOptions[2].options = updateObjectOptionLabels(state.models.modelOptions[2].options, showObjectId, showVersions, showUpdatedDate, showCreatedDate)

                // We also update the label on the selected option if set
                if (state.models.selectedModelOption) {
                    state.models.selectedModelOption = updateObjectOptionLabels([state.models.selectedModelOption], showObjectId, showVersions, showUpdatedDate, showCreatedDate)[0]
                }

            } else if (name === "inputs") {

                // Get the variables that determine how to show the labels
                const {showVersions, showObjectId, showUpdatedDate, showCreatedDate} = state.optionLabels.inputs

                // If the user changes how they want the labels to display we update all the options
                Object.values(state.inputs.inputDefinitions).forEach(input => {
                    if (input.options && isGroupOption(input.options)) {
                        // @ts-ignore
                        input.options[0].options = updateObjectOptionLabels(input.options[0].options, showObjectId, showVersions, showUpdatedDate, showCreatedDate)
                        // @ts-ignore
                        input.options[1].options = updateObjectOptionLabels(input.options[1].options, showObjectId, showVersions, showUpdatedDate, showCreatedDate)
                        // @ts-ignore
                        input.options[2].options = updateObjectOptionLabels(input.options[2].options, showObjectId, showVersions, showUpdatedDate, showCreatedDate)
                    }
                })

                // We also update the label on the selected option if set
                Object.entries(state.inputs.selectedInputOptions).forEach(([key, selectedOption]) => {
                    if (selectedOption) {
                        state.inputs.selectedInputOptions[key] = updateObjectOptionLabels([selectedOption], showObjectId, showVersions, showUpdatedDate, showCreatedDate)[0]
                    }
                })
            }
        },
        /**
         * A reducer that adds an option for a model to the list of options. This runs when the user pastes an object
         * ID or key into a SelectOption component for the models and passes the TRAC object information to this
         * function to handle.
         */
        addModelOption: (state, action: PayloadAction<OnCreateNewOptionPayload<trac.metadata.ITag>>) => {

            // Get the variables that determine how to show the labels
            const {showObjectId, showVersions, showUpdatedDate, showCreatedDate} = state.optionLabels.models

            const {newOptions} = action.payload

            // Update the label to reflect any user changes to what they want
            const optionWithUpdatedLabel = updateObjectOptionLabels(newOptions, showObjectId, showVersions, showUpdatedDate, showCreatedDate)[0]

            // Add the new option to the list, other added items are retained
            state.models.modelOptions[0].options.push(optionWithUpdatedLabel)

            // Set the option to be the new value
            state.models.selectedModelOption = optionWithUpdatedLabel

            // If the selected model already exists in the list then the SelectOption component will just find it rather than search for it but to be safe
            // we remove any added items from the search results, if the user searches by object key there may be a case that this is needed
            state.models.modelOptions[2].options = state.models.modelOptions[2].options.filter(option => option.value !== optionWithUpdatedLabel.value)

            // Remove any info about re-running a job
            state.rerun = setupRerun()

            // Tell the application that this job has not been built
            state.model.jobHasBeenBuilt = false

            showToast("success", "New model option successfully added", "runAModelStore/addModelOption")
        },
        /**
         * A reducer that takes the selected model for a job and gets the parameters from the model definition,
         * It also handles when the user has selected to re-run a job. This is run when building a job after
         * selecting a model.
         */
        getParameters: (state, action: PayloadAction<{ modelMetadata: Model["modelMetadata"], updateValidation: boolean, isSettingUpRerun: boolean }>) => {

            const {isSettingUpRerun, modelMetadata, updateValidation} = action.payload

            // First extract the parameter values from the models selected
            let newParametersFromModels = buildParametersForSingleModel(modelMetadata)

            // If a job has been built in response to the user wanting to rerun a job we need to set the parameters
            // to be the same as the job

            // The setProcessedParameters function has all the logic to set the default values of each parameter, including when it is an array.
            // Rather than duplicate the logic here, we modify the default values from the parametersFromModels array. Since we are re-running
            // a job we don't have to check the types as the original values were good to use originally, but we check anyway
            if (isSettingUpRerun) {
                Object.entries(state.rerun.job?.definition?.job?.runModel?.parameters || {}).forEach(([key, value]) => {

                    const parameterIndex = newParametersFromModels.findIndex(parametersFromModel => parametersFromModel.key === key)

                    if (parameterIndex > -1 && value?.type?.basicType === newParametersFromModels[parameterIndex].value.paramType?.basicType) {
                        newParametersFromModels[parameterIndex].value.defaultValue = value
                    }
                })
            }

            // Convert the model parameters into parameter definitions that can be used by the ParameterMenu component.
            // So if the parameter has a default value or a label set in the model then these are used.
            let newParameterDefinitions = setProcessedParameters(newParametersFromModels)

            // Create a new set of values for the parameters
            let newParameterValues = extractDefaultValues(newParameterDefinitions)

            state.parameters.parameterDefinitions = newParameterDefinitions
            state.parameters.values = newParameterValues
            state.parameters.lastParameterChanged = null

            // Update the job validation only is not being in isolation and not with 'buildRunAModelJobConfiguration
            if (updateValidation) runAModelStore.caseReducers.validateJobSelections(state)
        },
        /**
         * A reducer that takes the user set value for a model parameter and stores the value and the validation in the store.
         * Note that we have to make the payload writeable (have no readonly properties) as part of the SelectOptionPayload
         * is read only.
         */
        setParameter: (state, action: PayloadAction<DeepWritable<SelectPayload<Option, boolean>>>) => {

            if (action.payload.id && typeof action.payload.id === "string" && hasOwnProperty(state.parameters.values, action.payload.id)) {

                state.parameters.values[action.payload.id] = action.payload.value

                state.parameters.lastParameterChanged = action.payload.id
                state.model.userChanged.what.parameter = true
                state.model.userChanged.something = true

                state.validation.isValid.parameters[action.payload.id] = action.payload.isValid
                state.validation.validationChecked = false
            }
        },
        /**
         * A reducer that checks all the parameters and input datasets selected and works out if there are any issues
         * that will mean that the job will error.
         */
        validateJobSelections: (state) => {

            const {model: {modelMetadata}, inputs: {selectedInputOptions, inputDefinitions, inputTypes}} = state

            const inputOptions: Record<string, GenericGroup[]> = {}
            Object.entries(inputDefinitions).forEach(([key, inputDefinition]) => {

                if (inputDefinition.options && isGroupOption(inputDefinition.options)) inputOptions[key] = inputDefinition.options
            })

            // Run all the validation tests to check that the selections are valid, the 'review job' button is disabled if a check fails
            state.validation = {
                ...{
                    isValid: state.validation.isValid,
                    validationChecked: state.validation.validationChecked
                }, ...validateModelSelections(modelMetadata?.definition?.model, selectedInputOptions, inputOptions, inputTypes)
            }
        }
    },
    extraReducers: (builder) => {

        // A set of lifecycle reducers to run before/after the getmodels function
        builder.addCase(getModels.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.models.status = "pending"
            state.models.message = undefined
        })
        builder.addCase(getModels.fulfilled, (state, action: PayloadAction<SearchOption[]>) => {

            state.models.status = "succeeded"
            state.models.message = undefined
            state.models.modelOptions[2].options = action.payload

            // If the user has selected to re-run a job and then refreshed the list of flows then the payload here
            // can contain the same option, we want to avoid the duplication
            if (state.models.modelOptions[1].options?.[0] && state.rerun.job) {

                // Is the selected option in the re-run section (index 1), if so hide it in the search options (index 2)
                state.models.modelOptions = hideDuplicatedGroupOptions(state.models.modelOptions[1].options[0], state.models.modelOptions, 1, 2)
            }

            // Now that we have the new options we need to check if the selected model options are still
            // in the list if it is we do nothing, if not we need to remove the selected option and also clear the part
            //  of the state dealing with the configured job
            if (state.models.selectedModelOption && action.payload.findIndex(option => state.models.selectedModelOption && state.models.selectedModelOption.value === option.value) === -1) {
                state.models.selectedModelOption = null
                // WARNING
                // You must be careful resetting some parts of state can trigger changes that cause hooks to execute,
                state.model = setupModel()
                state.inputs = setupInputs()
                state.parameters = setupParameters()
                state.validation = setupValidation()
            }
        })
        builder.addCase(getModels.rejected, (state, action) => {

            state.models.status = "failed"

            const text = {
                title: "Failed to get a list of models",
                message: "The search for models based on your business segments did not complete successfully.",
                details: action.error.message
            }

            showToast("error", text, "runAModelStore/getModels/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the buildRunAModelJobConfiguration function
        builder.addCase(buildRunAModelJobConfiguration.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.model.status = "pending"
            state.model.message = undefined
        })
        builder.addCase(buildRunAModelJobConfiguration.fulfilled, (state, action: PayloadAction<{
            // Model
            modelMetadata: undefined | trac.metadata.ITag
        }>) => {

            state.model.status = "succeeded"
            state.model.message = undefined
            state.model.jobHasBeenBuilt = true
            state.model.modelMetadata = action.payload.modelMetadata

            // Update the job validation
            runAModelStore.caseReducers.validateJobSelections(state)
        })
        builder.addCase(buildRunAModelJobConfiguration.rejected, (state, action) => {

            state.model.status = "failed"

            const text = {
                title: "Failed to get the model",
                message: "The request to get the selected model did not complete successfully.",
                details: action.error.message
            }

            showToast("error", text, "runAModelStore/buildRunAModelJobConfiguration/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the getInputs function
        builder.addCase(getInputs.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.inputs.status = "pending"
            state.inputs.message = undefined
        })
        builder.addCase(getInputs.fulfilled, (state, action: PayloadAction<{
            // Inputs
            inputDefinitions: Inputs["inputDefinitions"]
            inputsThatWereNulled: string[]
            inputTypes: Inputs["inputTypes"]
            selectedInputOptions: Inputs["selectedInputOptions"]
            updateValidation: boolean
        }>) => {

            // Update the input API status
            state.inputs.status = "succeeded"
            state.inputs.message = undefined

            // Update the inputs
            state.inputs.inputDefinitions = action.payload.inputDefinitions
            state.inputs.inputTypes = action.payload.inputTypes
            state.inputs.selectedInputOptions = action.payload.selectedInputOptions

            // Any inputs that were nulled need to have their validation set to false if they are non-optional
            action.payload.inputsThatWereNulled.forEach(key => {
                if (!state.inputs.inputTypes.optional.includes(key)) {
                    state.validation.isValid.inputs[key] = false
                }
            })

            // Update the job validation only is not being in isolation and not with 'buildRunAModelJobConfiguration
            if (action.payload.updateValidation) runAModelStore.caseReducers.validateJobSelections(state)
        })
        builder.addCase(getInputs.rejected, (state, action) => {

            state.inputs.status = "failed"

            const text = {
                title: "Failed to get the input datasets",
                message: "The request to get the input datasets for the model did not complete successfully.",
                details: action.error.message
            }

            showToast("error", text, "runAModelStore/getInputs/rejected")
            console.error(action.error)
        })

        // A single lifecycle reducers to run after the setModel function, it has no async parts so does not need pending and failure functions
        builder.addCase(setModel.fulfilled, (state, action: PayloadAction<SelectOptionPayload<SearchOption, false>>) => {

            state.models.selectedModelOption = action.payload.value

            // If the user changes selection then remove any details of the job that needs to be rerun. This is set
            // if the user selects a job to rerun.
            state.rerun.job = undefined
            // Tell the UI that the job has not been built for this user selected model option
            state.model.jobHasBeenBuilt = false
        })

        // A set of lifecycle reducers to run before/after the setInput function
        builder.addCase(setInput.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.inputs.status = "pending"
            state.inputs.message = undefined
        })
        builder.addCase(setInput.fulfilled, (state, action: PayloadAction<SelectOptionPayload<SearchOption, false>>) => {

            state.inputs.status = "succeeded"
            state.inputs.message = undefined

            const {id, isValid, value} = action.payload

            if (typeof id === "string" && hasOwnProperty(state.inputs.selectedInputOptions, id)) {

                state.validation.isValid.inputs[id] = isValid
                state.validation.validationChecked = false

                // Note value can be null
                state.model.userChanged.what.input = true
                state.model.userChanged.something = true
                state.inputs.lastInputChanged = id
                state.inputs.selectedInputOptions[id] = value

                if (value) {

                    // The current options
                    const {options} = state.inputs.inputDefinitions[id]

                    // If the options exist and they are for groups
                    if (options && isGroupOption(options)) {

                        // Since the selected input now has the metadata for the model we also need to update the option in the group
                        // We need to do this for both the re-run and the search results options
                        const {tier: tier1, index: index1} = getGroupOptionIndex(value, [options[1]])

                        if (tier1 > -1 && index1 > -1) {
                            options[1].options[index1] = {
                                ...value,
                                disabled: options[1].options[index1].disabled,
                                icon: options[1].options[index1].icon
                            }
                        }

                        const {tier: tier2, index: index2} = getGroupOptionIndex(value, [options[2]])

                        if (tier2 > -1 && index2 > -1) {
                            options[2].options[index2] = {
                                ...value,
                                disabled: options[2].options[index2].disabled,
                                icon: options[2].options[index2].icon
                            }
                        }
                    }
                }

                // Update the job validation
                runAModelStore.caseReducers.validateJobSelections(state)
            }
        })
        builder.addCase(setInput.rejected, (state, action) => {

            state.inputs.status = "failed"

            const text = {
                title: "Failed to set a the selected input",
                message: "The request to set the selected option and get its details did not complete successfully.",
                details: action.error.message
            }

            showToast("error", text, "runAModelStore/setInput/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the addInputOption function
        builder.addCase(addInputOption.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.inputs.status = "pending"
            state.inputs.message = undefined
        })
        builder.addCase(addInputOption.fulfilled, (state, action: PayloadAction<{ id: string, option: SearchOption }>) => {

            state.inputs.status = "succeeded"
            state.inputs.message = undefined

            const {id, option} = action.payload

            // The current set of options for the input
            const options = state.inputs.inputDefinitions[id].options

            // This only works if the options for the inputs are in groups
            if (options && isGroupOption(options)) {

                // Add the new option to the list, other added items are retained
                options[0].options.push(option)
            }

            // Set the option to be the new value
            state.inputs.selectedInputOptions[id] = option

            // Update the job validation
            runAModelStore.caseReducers.validateJobSelections(state)

            showToast("success", `New input option successfully added`, "runAModelStore/addInputOption")
        })
        builder.addCase(addInputOption.rejected, (state, action) => {

            state.models.status = "failed"
            state.inputs.status = "failed"

            const text = {
                title: "Failed to add the mew option",
                message: "The request to add a new input option and get its details did not complete successfully.",
                details: action.error.message
            }

            showToast("error", text, "runAModelStore/runAModelStore/rejected")
            console.error(action.error)
        })

        // A single lifecycle reducers to run after the setJobToRerun function, it has no async parts so does not need pending and failure functions
        builder.addCase(setJobToRerun.fulfilled, (state, action: PayloadAction<{ job: trac.metadata.ITag, model: trac.metadata.ITag }>) => {

            const {job, model} = action.payload

            // Get the variables that determine how to show the labels
            const {
                showObjectId,
                showVersions,
                showUpdatedDate,
                showCreatedDate
            } = state.optionLabels.models

            // Is the tag for a run model
            if (job.definition?.job?.runModel && model && job.header?.objectId) {


                state.setUpOrReview = "setup"
                state.rerun.job = job
                state.rerun.jobParameters = {}

                // Extract the parameters used by the job into an object and convert them into the format used by the
                // ParameterMenu component
                Object.entries(job?.definition?.job?.runModel?.parameters || {}).forEach(([key, value]) => {

                    const extractedValue = extractValueFromTracValueObject(value)

                    let policyValue: null | number | string | boolean | Option | Option[] = null

                    if (Array.isArray(extractedValue.value)) {

                        policyValue = convertArrayToOptions(extractedValue.value, true, extractedValue.subBasicType)

                    } else if (!isObject(extractedValue.value)) {
                        policyValue = extractedValue.value
                    }

                    if (state.rerun.jobParameters) state.rerun.jobParameters[key] = policyValue
                })

                state.rerun.jobInputs = {}

                Object.entries(job?.definition?.job?.runModel?.inputs || {}).forEach(([key, value]) => {

                    const policyValue: SearchOption[] = convertSearchResultsIntoOptions([{header: value}], false, false, false, false, false)

                    if (state.rerun.jobInputs) state.rerun.jobInputs[key] = policyValue[0]
                })

                // We need to tell the UI to rebuild the job to get the right options and set the policy icons
                state.model.jobHasBeenBuilt = false

                const newModelOption = convertSearchResultsIntoOptions([model], showObjectId, showVersions, showUpdatedDate, showCreatedDate, false)

                // Add an icon to say that this is the option that matches the job to re-run
                newModelOption[0].icon = "bi-check-circle"

                // Note that the whole array is replaced with a single option, index 1 is the "ADDED FOR RERUN" sub list
                state.models.modelOptions[1].options = [...newModelOption]
                state.models.selectedModelOption = {...newModelOption[0]}

                // Find the option in the option group
                const {tier, index} = getGroupOptionIndex(newModelOption[0], [state.models.modelOptions[2]])

                // If found then set the option as disabled, which will hide it
                if (tier > -1 && index > -1) {
                    state.models.modelOptions[2].options[index].disabled = true
                }
            }
        })

        // A set of lifecycle reducers to run before/after the runModelJob function
        builder.addCase(runJob.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.job.status = "pending"
            state.job.message = undefined
        })
        builder.addCase(runJob.fulfilled, (state, action: PayloadAction<trac.api.IJobStatus>) => {

            state.job.status = "succeeded"
            state.job.message = undefined

            const text = `The job to run the model in TRAC was successfully started with job ID ${action.payload.jobId?.objectId}, you can see its progress in the 'Find a job' page.`
            showToast("success", text, "runAModelStore/runJob/fulfilled")
        })
        builder.addCase(runJob.rejected, (state, action) => {

            state.job.status = "failed"

            const text = {
                title: "Failed to start the job",
                message: "The request to start the job did not complete successfully.",
                details: action.error.message
            }

            showToast("error", text, "runAModelStore/runJob/rejected")
            console.error(action.error)
        })
    }
})

// Action creators are generated for each case reducer function
export const {
    addModelOption,
    getParameters,
    resetState,
    reviewJob,
    setListsOrTabs,
    setNumberOfApiCalls,
    setParameter,
    setShowKeys,
    setUpJob,
    setValidationChecked,
    updateCompletedApiCalls,
    updateOptionLabels
} = runAModelStore.actions

export default runAModelStore.reducer