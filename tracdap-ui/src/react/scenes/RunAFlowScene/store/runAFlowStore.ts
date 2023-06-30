/**
 * This slice acts as the store for the {@link RunAFlowScene}.
 * @module runAFlowStore
 * @category Redux store
 */

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
    ValidationOfModelInputs,
    ValidationOfModelsAndFlowData,
    ValidationOfModelsParameters
} from "../../../../types/types_general";
import {checkForBatchMetadata, checkForMetadata} from "../../../store/applicationStore";
import cloneDeep from "lodash.clonedeep";
import {convertArrayToOptions} from "../../../utils/utils_arrays";
import {createBlankTracGroup, getGroupOptionIndex, goToTop, hideDuplicatedGroupOptions, showToast, updateObjectOptionLabels} from "../../../utils/utils_general";
import {createAsyncThunk, createSlice, PayloadAction} from '@reduxjs/toolkit'
import {
    buildParametersAcrossMultipleModels,
    categoriseFlowInputTypes,
    categoriseFlowModelTypes,
    createFlowInputOptionsTemplate,
    createFlowModelOptionsTemplate,
    createFlowSelectedInputOptionsTemplate,
    createFlowSelectedModelOptionsTemplate,
    getInputCountFromFlow,
    getModelCountFromFlow,
    setProcessedInputs,
    setProcessedParameters,
    validateFlowSelections
} from "../../../utils/utils_flows";
import {createTag, createTagsFromAttributes, extractDefaultValues} from "../../../utils/utils_attributes_and_parameters";
import {convertSearchResultsIntoOptions, extractValueFromTracValueObject, sortOptionsByObjectTimeStamp} from "../../../utils/utils_trac_metadata";
import {hasOwnProperty, isDefined, isGroupOption, isKeyOf, isObject, isTagOptionArray} from "../../../utils/utils_trac_type_chckers";
import {resetAttributesToDefaults, setAttributesAutomatically} from "../../../components/SetAttributes/setAttributesStore";
import {resetOverlaysForSingleUser} from "../../../components/OverlayBuilder/overlayBuilderStore";
import {RootState} from "../../../../storeController";
import {searchByBusinessSegments, searchByMultipleAndClauses, submitJob} from "../../../utils/utils_trac_api";
import {SingleValue} from "react-select";
import {toast} from "react-toastify";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {UiAttributesProps} from "../../../../types/types_attributes_and_parameters";

// A generic option for a policy. The empty tag is for Typescript to be happy that this matches the expected structure
const noPolicyOption = {value: "NONE", label: "No governance policy applied", tag: {}}

/**
 * An interface for the 'flows' entry point in the store state.
 */
export interface Flows {
    // The metadata that comes from TRAC that contains flows that match the search criteria. We also collect the
    // ones that the user has added manually by pasting an object ID or key and those that we had to load in order
    // to rerun a job.
    flowOptions: TracGroup
    // The status of the API request to get the list of flows.
    status: StoreStatus
    // Any message associated with the API request to get the list of flows.
    message: undefined | string
    // User selected flow from the list
    selectedFlowOption: SingleValue<SearchOption>
}

// A function that initialises the 'flows' entry point in the store state
function setupFlows(): Flows {
    return {
        message: undefined,
        flowOptions: createBlankTracGroup(),
        selectedFlowOption: null,
        status: 'idle'
    }
}

/**
 * An interface for the 'flow' entry point in the store state.
 */
export interface Flow {
    // Whether the job needs to be built, changing the flow or asking to re-run a flow sets this to false
    jobHasBeenBuilt: boolean
    // The metadata for the chosen flow
    flowMetadata: undefined | trac.metadata.ITag
    // Any message associated with the API request to get the metadata object.
    message: undefined | string,
    // The number of API calls needed to build the flow, this is used to show the progress bar
    numberOfApiCalls: { toDo: number, completed: number }
    // The status of the API request to get the metadata object.
    status: StoreStatus
    // Has the user changed the configuration away from the default
    userChanged: { something: boolean, what: { model: boolean, input: boolean, parameter: boolean } },
}

// A function that initialises the 'flow' entry point in the store state
function setupFlow(): Flow {
    return {
        jobHasBeenBuilt: false,
        flowMetadata: undefined,
        message: undefined,
        status: 'idle',
        numberOfApiCalls: {toDo: 0, completed: 0},
        userChanged: {something: false, what: {model: false, input: false, parameter: false}}
    }
}

/**
 * An interface for the 'policies' entry point in the store state.
 */
export interface Policies {
    // The metadata that comes from TRAC that contains policies that match the search criteria.
    options: SearchOption[],
    // The status of the API request to get the list of metadata.
    status: StoreStatus,
    // Any error associated with the API request to get the list of metadata.
    message: undefined | string,
    // User selected policy from the list
    selectedPolicyOption: SingleValue<SearchOption>
}

// A function that initialises the 'policies' entry point in the store state
function setupPolicies(): Policies {
    return {
        options: [],
        status: 'idle',
        message: undefined,
        selectedPolicyOption: null
    }
}

/**
 * An interface for the 'policy' entry point in the store state.
 */
export interface Policy {
    // The status of the API request to get the metadata object.
    status: StoreStatus
    // Any error associated with the API request to get the metadata object.
    message: undefined | string
    // The selected flow object, this is the full object not just the metadata.
    selectedPolicyObject: undefined | trac.metadata.ITag
}

// A function that initialises the 'policy' entry point in the store state
function setupPolicy(): Policy {
    return {
        status: "idle",
        message: undefined,
        selectedPolicyObject: undefined
    }
}

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

// A function that initialises the 'inputs' entry point in the store state
export function setupInputs(): Inputs {

    return {
        inputDefinitions: {},
        inputTypes: {required: [], optional: [], categories: {}},
        lastInputChanged: null,
        listsOrTabs: "lists",
        message: undefined,
        selectedInputOptions: {},
        showKeysInsteadOfLabels: false,
        status: 'idle'
    }
}

/**
 * An interface for the 'models' entry point in the store state.
 */
export interface Models {

    // Any message associated with the API request to get the selected model options
    message: undefined | string
    // A list of the options for each model
    modelOptions: Record<string, TracGroup>
    // The model types e.g. main or sub
    modelTypes: { allModels: string[], mainModels: string[], subModels: string[], subModelsByModelKey: Record<string, string[]> }
    // The model option selected by the user
    selectedModelOptions: Record<string, SingleValue<SearchOption>>
    // Whether to show model labels or their key in the flow
    showKeysInsteadOfLabels: boolean
    // The status of the API request to get the selected model options
    status: StoreStatus
}

// A function that initialises the 'models' entry point in the store state
export function setupModels(): Models {

    return {
        message: undefined,
        modelOptions: {},
        modelTypes: {allModels: [], mainModels: [], subModels: [], subModelsByModelKey: {}},
        selectedModelOptions: {},
        showKeysInsteadOfLabels: false,
        status: "idle"
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
    // Whether to show parameter labels or the key in the flow setup
    showKeysInsteadOfLabels: boolean
    // The values set for the parameters
    values: Record<string, SelectPayload<Option, boolean>["value"]>
}

// A function that initialises the 'models' entry point in the store state
export function setupParameters(): DeepWritable<Parameters> {

    return {
        lastParameterChanged: null,
        parameterDefinitions: {},
        showKeysInsteadOfLabels: false,
        values: {}
    }
}

/**
 * An interface for the 'validation' entry point in the store state.
 */
export interface Validation {
    // In summary can the job be run
    canRun: boolean
    // Do the selected input datasets have schemas that match the model definition
    flowInputSchemasNotMatchingModelInputs: ValidationOfModelInputs
    // validation status of the individual selected/set values for the job, excluding attributes
    isValid: {
        models: Record<string, boolean>, inputs: Record<string, boolean>, parameters: Record<string, boolean>
    }
    // Has the validation for the individual selected/set values for the job been run, if so we will be showing any validation errors
    validationChecked: boolean
    // Do model options exist for all models in the chain (any added in for re-run or manually added count)
    modelOptionsExistForAll: boolean,
    // Do the selected models have all the correct inputs and outputs defined in the flow
    modelsMatchFlowModelInputsAndOutputs: ValidationOfModelsAndFlowData
    // Do input options exist for all non-optional datasets in the chain (any added in for re-run or manually added count)
    nonOptionalInputOptionsExistForAll: boolean,
    // Are there any parameters where the basic types are different in different models
    parametersWithMultipleBasicTypes: ValidationOfModelsParameters
}

// A function that initialises the 'validation' entry point in the store state
export function setupValidation(): Validation {

    return {
        canRun: true,
        flowInputSchemasNotMatchingModelInputs: {},
        isValid: {models: {}, inputs: {}, parameters: {}},
        modelOptionsExistForAll: true,
        modelsMatchFlowModelInputsAndOutputs: {},
        validationChecked: false,
        nonOptionalInputOptionsExistForAll: true,
        parametersWithMultipleBasicTypes: []
    }
}

/**
 * An interface for the 'rerun' entry point in the store state.
 */
export interface Rerun {
    job: undefined | trac.metadata.ITag,
    jobParameters: undefined | Record<string, null | string | boolean | number | Option | Option[]>
    jobModels: undefined | Record<string, SingleValue<SearchOption>>
    jobInputs: undefined | Record<string, SingleValue<SearchOption>>
}

// A function that initialises the 'rerun' entry point in the store state
export function setupRerun(): Rerun {

    return {
        job: undefined,
        jobParameters: undefined,
        jobModels: undefined,
        jobInputs: undefined
    }
}

/**
 * An interface for the runAFlowStore Redux store.
 */
export interface RunAFlowStoreState {

    // Whether the user is viewing the setup or review page
    setUpOrReview: "setup" | "review"
    // How to format the labels in the options, these are stored outside the main equivalent parts of the store
    // so that if a new flow is selected these options are retained
    optionLabels: {
        flows: OptionLabels
        policies: OptionLabels
        models: OptionLabels
        inputs: OptionLabels
    }
    flows: Flows
    flow: Flow
    // Information about a job we might be trying to rerun
    rerun: Rerun
    policies: Policies
    policy: Policy
    models: Models
    modelChain: {
        showModelChain: boolean,
        modelInfoToShow: Record<string, boolean>
    }
    parameters: Parameters
    inputs: Inputs
    validation: Validation
    job: {
        // The status of the API request to kick off the job.
        status: StoreStatus
        // Any message associated with the API request to  kick off the job.
        message: undefined | string
    }
}

// This is the initial state of the store. Note attributes are handled in the setAttributesStore.
// Note that those areas without a setup function are designed to persist if an option is changed
// by the user, they are 'session settings'.
const initialState: RunAFlowStoreState = {

    setUpOrReview: "setup",
    optionLabels: {
        flows: {
            showObjectId: false,
            showVersions: false,
            showCreatedDate: false,
            showUpdatedDate: true
        },
        policies: {
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
        },
        inputs: {
            showObjectId: false,
            showVersions: false,
            showCreatedDate: false,
            showUpdatedDate: true
        }
    },
    flows: setupFlows(),
    flow: setupFlow(),
    rerun: setupRerun(),
    policies: setupPolicies(),
    policy: setupPolicy(),
    models: setupModels(),
    modelChain: {
        showModelChain: false,
        modelInfoToShow: {},
    },
    inputs: setupInputs(),
    parameters: setupParameters(),
    validation: setupValidation(),
    job: {
        status: "idle",
        message: undefined
    }
}

/**
 * A function that searches TRAC for flows that match the selected business segments.
 */
export const getFlows = createAsyncThunk<// Return type of the payload creator
    SearchOption[],
    // First argument to the payload creator
    { storeKey: keyof BusinessSegmentsStoreState["uses"] } | ButtonPayload,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAFlowStore/getFlows', async (payload, {getState}) => {

    // The storeKey is needed to get the business segments selected by the user from the businessSegmentsStore
    const storeKey = hasOwnProperty(payload, "storeKey") ? payload.storeKey : payload.name

    // Protection for Typescript
    if (typeof storeKey !== "string") {
        throw new Error(`The storeKey value '${storeKey}' was not a string`)
    }

    if (!isKeyOf(getState().businessSegmentsStore.uses, storeKey)) {
        throw new Error(`The storeKey '${storeKey}' is not a valid key of businessSegmentsStore.uses`)
    }

    // Get the parameters we need from the store
    const {cookies: {"trac-tenant": tenant}, tracApi: {searchAsOf}, clientConfig: {searches: {maximumNumberOfOptionsReturned}}} = getState().applicationStore

    // Get the selected business segment options for each level and also which levels we are using.
    const {uses: {[storeKey]: {selectedOptions}}, businessSegments: {levels}} = getState().businessSegmentsStore

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")

    // Put the selected options into an array and filter, the isDefined is there to make Typescript happy that there
    // are no null values
    const options = levels.map(level => selectedOptions[level]).filter(option => option !== null && option.value !== null && option.value.toUpperCase() !== "ALL").filter(isDefined)

    // Initiate the search for flows by business segments
    const searchResults = await searchByBusinessSegments({
        maximumNumberOfOptionsReturned,
        objectType: trac.ObjectType.FLOW,
        options,
        searchAsOf,
        tenant
    })

    // The parameters that specify how the labels in the options should be formed
    const {showCreatedDate, showObjectId, showUpdatedDate, showVersions} = getState().runAFlowStore.optionLabels.flows

    // Return the options with the formatted labels
    return convertSearchResultsIntoOptions(searchResults, showObjectId, showVersions, showUpdatedDate, showCreatedDate, false)
})

/**
 * A function that searches TRAC for policies that can be applied to the selected flow.
 */
export const getPolicies = createAsyncThunk<// Return type of the payload creator
    SearchOption[],
    // First argument to the payload creator
    void | ButtonPayload,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAFlowStore/getPolicies', async (payload, {getState}) => {

    // The parameters that specify how the labels in the options should be formed
    const {
        showObjectId,
        showCreatedDate,
        showUpdatedDate,
        showVersions
    } = getState().runAFlowStore.optionLabels.policies

    const searchResults: trac.metadata.ITag[] = []

    const options = convertSearchResultsIntoOptions(searchResults, showObjectId, showVersions, showUpdatedDate, showCreatedDate, false)

    // The empty tag in the noPolicyOption object is so Typescript is happy that it matches the expected structure
    options.unshift(noPolicyOption)

    return options
})

/**
 * A function that gets the metadata for the selected policy
 */
export const getPolicy = createAsyncThunk<// Return type of the payload creator
    void | trac.metadata.ITag,
    // First argument to the payload creator
    void,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAFlowStore/getPolicy', async (payload, {getState}) => {

    // Get the selected option from the store and check if it is the 'No policy' option
    const {selectedPolicyOption} = getState().runAFlowStore.policies

    if (selectedPolicyOption && selectedPolicyOption.value === "NONE") return

    if (selectedPolicyOption && selectedPolicyOption.tag.definition?.job?.runFlow) return selectedPolicyOption.tag

    return
})

/**
 * A function that receives the selected model option, gets the metadata for it if needed and then passes
 * the updated option to the store.
 */
export const setModel = createAsyncThunk<// Return type of the payload creator
    SelectOptionPayload<SearchOption, false>,
    // First argument to the payload creator
    SelectOptionPayload<SearchOption, false>,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAFlowStore/setModel', async (payload, {dispatch}) => {

    // We need to get the full metadata for the new option if it has not already been retrieved.
    let tag: trac.metadata.ITag | undefined

    // See note below
    let newPayload = {...payload}

    // Only make the call for the metadata if the tag does not include the definition property which indicates that
    // we have the search result but not the actual full metadata
    if (newPayload.value?.tag?.header && !newPayload.value.tag?.definition) {

        // Get the option metadata - this is using the client side metadata store
        tag = await dispatch(checkForMetadata(newPayload.value.tag.header)).unwrap()

        // This is a complete faff, but we have to do it because Redux will throw an error if we mutate the read
        // only parts of the payload. So instead we have to create a completely new option object.
        newPayload.value = {
            value: newPayload.value.value,
            label: newPayload.value.label,
            tag: tag || newPayload.value.tag,
            icon: newPayload.value.icon,
            disabled: newPayload.value.disabled
        }
    }

    return newPayload
})

/**
 * A function that stores the selected flow option into the store. It also clears any attributes set by the user.
 *
 * @remarks We have to make this a thunk rather than a reducer because we need to access the reducers from other slices. The only
 * way to run these reducers is with dispatch which is not available in a reducer but is available using a thunk.
 */
export const setFlow = createAsyncThunk<// Return type of the payload creator
    SelectOptionPayload<SearchOption, false>,
    // First argument to the payload creator
    SelectOptionPayload<SearchOption, false>,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAFlowStore/setFlow', async (payload, {dispatch}) => {

    // Rest the attributes back to the default, if the user has changed the model then we reset the
    // attributes back to the default as they may be no longer valid, this clearing will not be
    // visible to the user as the attributes are on the review page and the widget to change the
    // model is on the setup page.
    dispatch(resetAttributesToDefaults({storeKey: "runAFlow"}))

    // Reset the overlays set for the flow, we do not carry these over when a flow changes
    dispatch(resetOverlaysForSingleUser({storeKey: "runAFlow"}))

    // We want to set the name for the job as the name of the model as a starting point
    const flowName = payload.value?.tag.attrs?.name?.stringValue ?? null
    const businessSegments = extractValueFromTracValueObject(payload.value?.tag.attrs?.business_segments)

    // Update the job attribute
    dispatch(setAttributesAutomatically({storeKey: "runAFlow", values: {name: flowName, business_segments: businessSegments.value ?? null}}))

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
    }>('runAFlowStore/setInput', async (payload, {dispatch}) => {

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
 * A function that searches for a flow by its object ID and then carries out a series of additional API calls to
 * get lists of the available options for each model and input dataset. These are processed as arrays that
 * can be directly placed into {@link SelectOption} components. The function also creates a job and configures that job to
 * run, to do this it takes the latest option from the list of options and sets that as the default to use in the
 * job. The only case it does not try to assign a default to are the optional data inputs.
 */
export const buildRunAFlowJobConfiguration = createAsyncThunk<// Return type of the payload creator
    {
        flowMetadata: Flow["flowMetadata"]
    },
    // First argument to the payload creator
    void,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAFlowStore/buildRunAFlowJobConfiguration', async (_, {getState, dispatch}) => {

    // Update the progress bar, first call is for the flow definition
    dispatch(setNumberOfApiCalls(1))

    // Get what we need from the store
    const {selectedFlowOption} = getState().runAFlowStore.flows
    const {rerun} = getState().runAFlowStore

    // If the user did not set an option there is nothing to get, this can happen if the SelectFLow component is
    // set as clearable and the user clears it
    if (!selectedFlowOption || selectedFlowOption.value == null) return {
        flowMetadata: {...setupFlow().flowMetadata}
    }

    // If the option does not have a tag selector then we can't proceed
    if (selectedFlowOption.tag.header == null) {
        throw new Error("The selected option does not have a tag selector")
    }

    // Get the flow metadata - this is using the client side metadata store
    const flowMetadata: trac.metadata.ITag = await dispatch(checkForMetadata(selectedFlowOption.tag.header)).unwrap()

    // If the flow does not have a flow definition then we can't proceed
    if (!flowMetadata.definition?.flow) {
        throw new Error("The flow option does not have a flow definition to run")
    }

    // We will need to do a search for the options for each model (n) and input (m), then for each we get the metadata for
    // the first options for all models and inputs as a single request (1). For all the inputs we may need to get the schemas of the
    // initially selected object as well, but again we can do this as a single request (1).
    let numberOfCalls = getModelCountFromFlow(flowMetadata.definition.flow) + getInputCountFromFlow(flowMetadata.definition.flow) + 1 + 1 + 1

    // Update the progress bar
    dispatch(setNumberOfApiCalls(numberOfCalls))

    // Get the list of policies in TRAC, since this function is triggered using a useEffect we can use the state
    // value for the option and do not have to pass it
    if (!rerun.job) await dispatch(getPolicies())

    // Run the function to search for input datasets and handle the selections, this is a separate function, so it can be run independently
    await dispatch(getInputs({flowDefinition: flowMetadata.definition.flow, flowAttrs: flowMetadata.attrs}))

    // Run the function to search for models and handle the selections, this is a separate function, so it can be run independently
    const {selectedModelOptions} = await dispatch(getModels({flowDefinition: flowMetadata.definition.flow})).unwrap()

    // Get the parameters from the selected models and set the default values
    dispatch(getParameters({isModelChange: false, isSettingUpRerun: Boolean(rerun.job !== undefined), selectedModelOptions, updateValidation: false}))

    return {
        // Flow
        flowMetadata
    }
})

/**
 * A function that a flow's metadata and then carries out a series of additional API calls to get lists of
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
    // 'buildRunAFlowJobConfiguration' function, which runs when the user selects a flow and in which case the
    // flow information needs to be passed in as it is not in the store. Second, when the user is
    // refreshing the list of inputs and the function is called from a generic component, in which case the
    // flow details are not passed in but picked up from the store.
    void | {
    flowDefinition: trac.metadata.IFlowDefinition,
    flowAttrs: trac.metadata.ITag["attrs"]
},
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAFlowStore/getInputs', async (payload, {getState, dispatch}) => {

    // Get what we need from the store
    const {cookies: {"trac-tenant": tenant}, tracApi: {searchAsOf}, clientConfig: {searches: {maximumNumberOfOptionsReturned}}} = getState().applicationStore
    const {rerun} = getState().runAFlowStore
    const {showVersions, showObjectId, showUpdatedDate, showCreatedDate} = getState().runAFlowStore.optionLabels.inputs
    const {inputDefinitions: existingInputDefinitions, selectedInputOptions: existingSelectedInputOptions} = getState().runAFlowStore.inputs

    // When there is no payload we are refreshing the list of inputs for a flow that is already configured
    const isRefresh = payload === undefined

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")

    // Getting the flow information depends on whether a new job is being set up (no payload) or we are updating an
    // already set up list of inputs
    const flowDefinition = !isRefresh ? payload?.flowDefinition : getState().runAFlowStore.flow.flowMetadata?.definition?.flow
    const flowAttrs = !isRefresh ? payload?.flowAttrs : getState().runAFlowStore.flow.flowMetadata?.attrs

    if (flowDefinition == undefined) throw new Error("flowDefinition is undefined")
    if (flowAttrs == undefined) throw new Error("flowAttrs is undefined")

    // For each input we need to search for available options and then get the metadata for the default option
    // if one exists.
    const inputOptions = createFlowInputOptionsTemplate(flowDefinition)

    // If the user loads a job to rerun and then changes flow we want the inputs in the "LOADED FOR RERUN" to be still available.
    // This allows the user to pick a job to re-run and apply all the selections to a new (updated flow). Same goes for the
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

        // You can specify a search term to use to find the inputs for a flow, so here we
        // get the nodes in the flow
        const nodes = flowDefinition.nodes

        // Get the search expression for the input if it exists
        let searchExpression: null | undefined | trac.metadata.ISearchExpression = nodes && nodes[inputKey] ? nodes[inputKey].nodeSearch : null

        // This is a default search by the key of the input, use this if the node does not have one set
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

        // If the node in the flow has a search term set to find it then we use that as the search term, otherwise we get it from the key
        // string. There are two types of search terms that can be set, first is a single term (like the default) and the second is a
        // logical expression of multiple terms. In the latter we nest them as a single term within a parent term, which allows us to
        // say A and B, where B is the logical expressions from the flow.
        const terms: trac.metadata.ISearchExpression[] = searchExpression?.logical ? [trac.metadata.SearchExpression.create({logical: searchExpression.logical})] : searchExpression?.term ? [trac.metadata.SearchExpression.create({term: searchExpression.term})] : [defaultSearchExpression]

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
    // If you change the below code you need to make sure that you always key the objects by the key used by the flow.
    // You should never key by the key of the input dataset as there is not always a 1-2-1 mapping, indeed flows
    // can use bespoke searches to define what they need and none of the results necessarily have a key that matches the
    // input keys in the flow.

    // The selected inputs and models to fill in with the default selections
    const selectedInputOptions = createFlowSelectedInputOptionsTemplate(flowDefinition)
    const inputTypes = categoriseFlowInputTypes(flowDefinition)
    let inputDefinitions = setProcessedInputs(selectedInputOptions, inputTypes, flowDefinition, flowAttrs)

    let schemasBatchMetadataRequest: trac.metadata.ITagSelector[] = []

    // We need to keep track of which inputs were nulled because they were not in the refreshed list, if these are
    // not optional then the validation for them should be turned to false
    let inputsThatWereNulled: string[] = []

    // Now go through the search results for each input in the flow
    searchResults.forEach(searchResult => {

        // Results here is an array of tags
        const {key, results} = searchResult

        // Turn the inputs search results into options. tagVersion is not used in setting the option key so that
        // when re-running a job the options from the searches can match the object used in the job that is being
        // re-run even if the tag has been updated since.
        inputOptions[key][2].options = convertSearchResultsIntoOptions(results, showObjectId, showVersions, showUpdatedDate, showCreatedDate, false)

        // First handle the case where we are re-running a job
        if (rerun.job !== undefined && !isRefresh) {

            // Get the tag selector of the object used (by the key in the flow)
            const inputUsed = rerun.job.definition?.job?.runFlow?.inputs?.[key]

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

            // Has the user already selected an option, say for a flow that they have just changed
            const alreadySelectedOption = existingSelectedInputOptions[key]

            // Does the existing option exist in the new search results
            const {index} = alreadySelectedOption != null ? getGroupOptionIndex(alreadySelectedOption, [inputOptions[key][2]]) : {index: -1}

            // If the user has already selected an item, then if this item still exists after changing the flow or hitting the refresh button then
            // retain that as the selected option. If we are building a new flow and the item does not exist then pick the first item in the list,
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

            // We don't want to add an icon to the selected option as a green tick mark will be shown above the widget
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

            // Find the option in the option group, index 2 is the added for search section
            const {tier, index} = getGroupOptionIndex(selectedOption, [inputOptions[inputRequestTypes[i].key][2]])

            // If found then put the option with the tag added back into the list
            if (tier > -1 && index > -1) {
                inputOptions[schemaRequestTypes[i].key][2].options[index] = selectedOption
            }

            // Is the selected option in the re-run section (index 1), if so hide it in the search options (index 2)
            inputOptions[schemaRequestTypes[i].key] = hideDuplicatedGroupOptions(selectedOption, inputOptions[schemaRequestTypes[i].key], 1, 2)
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
        // When building a job the 'buildRunAFlowJobConfiguration' function updates the validation, but when
        // refreshing we need to update it in this function.
        updateValidation: Boolean(payload === undefined),
    }
})

/**
 * A function that a flow's metadata and then carries out a series of additional API calls to get lists
 * of the available options for models. These are processed as arrays that can be directly placed into
 * {@link SelectOption} components. The function also takes the latest option from the list of options
 * and sets that as the default to use in the job. The only case it does not try to assign a default to
 * are the optional data inputs.
 */
export const getModels = createAsyncThunk<// Return type of the payload creator
    {
        // Models
        modelOptions: Models["modelOptions"]
        modelsThatWereNulled: string[]
        selectedModelOptions: Models["selectedModelOptions"]
        modelTypes: Models["modelTypes"]
        updateValidation: boolean
    },
    // First argument to the payload creator, there are two cases when this is run. First, as part of the
    // 'buildRunAFlowJobConfiguration' function, which runs when the user selects a flow and in which case the
    // flow information needs to be passed in as it is not in the store. Second, when the user is
    // refreshing the list of inputs and the function is called from a generic component, in which case the
    // flow details are not passed in but picked up from the store.
    void |
    {
        flowDefinition: trac.metadata.IFlowDefinition
    },
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAFlowStore/getModels', async (payload, {getState, dispatch}) => {

    // Get what we need from the store
    const {cookies: {"trac-tenant": tenant}, tracApi: {searchAsOf}, clientConfig: {searches: {maximumNumberOfOptionsReturned}}} = getState().applicationStore
    const {rerun} = getState().runAFlowStore
    const {showVersions, showObjectId, showUpdatedDate, showCreatedDate} = getState().runAFlowStore.optionLabels.models
    const {modelOptions: existingModelOptions, selectedModelOptions: existingSelectedModelOptions} = getState().runAFlowStore.models

    // When there is no payload we are refreshing the list of inputs for a flow that is already configured
    const isRefresh = payload === undefined

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")

    const flowDefinition = !isRefresh ? payload?.flowDefinition : getState().runAFlowStore.flow.flowMetadata?.definition?.flow

    if (flowDefinition == undefined) throw new Error("flowDefinition is undefined")

    // For each model we need to search for available options and then get the metadata for the default option
    // if one exists.
    const modelOptions = createFlowModelOptionsTemplate(flowDefinition)

    // If the user loads a job to rerun and then changes flow we want the models in the "LOADED FOR RERUN" to be still available.
    // This allows the user to pick a job to re-run and apply all the selections to a new (updated flow). Same goes for the
    // "ADDED BY USER" inputs.
    Object.keys(modelOptions).forEach(key => {

        if (existingModelOptions.hasOwnProperty(key)) {
            const existingOptions = existingModelOptions[key]
            if (existingOptions !== undefined && isGroupOption(existingOptions)) {

                if (isTagOptionArray(existingOptions[0].options)) modelOptions[key][0].options = existingOptions[0].options
                if (isTagOptionArray(existingOptions[1].options)) modelOptions[key][1].options = existingOptions[1].options
            }
        }
    })

    // Define an array of async calls that are searching for models that match the keys defined in the
    // flow definition. Note that the returned value is an array with the objects inside having results as
    // an array of tags.
    const modelSearches = Object.keys(modelOptions).map(async (modelKey) => {

        // You can specify a search term to use to find the models for a flow, so here we
        // get the nodes in the flow
        const nodes = flowDefinition.nodes

        // Get the search expression for the model if it exists
        let searchExpression: null | undefined | trac.metadata.ISearchExpression = nodes && nodes[modelKey] ? nodes[modelKey].nodeSearch : null

        // This is a default search by the key of the model, use this if the node does not have one set
        const defaultSearchExpression: trac.metadata.ISearchExpression = {
            term: {
                attrName: "key",
                attrType: trac.BasicType.STRING,
                operator: trac.SearchOperator.EQ,
                searchValue: {
                    type: {basicType: trac.BasicType.STRING},
                    stringValue: modelKey
                }
            }
        }

        // If the node in the flow has a search term set to find it then we use that as the search term, otherwise we get it from the key
        // string. There are two types of search terms that can be set, first is a single term (like the default) and the second is a
        // logical expression of multiple terms. In the latter we nest them as a single term within a parent term, which allows us to
        // say A and B, where B is the logical expressions from the flow.
        const terms: trac.metadata.ISearchExpression[] = searchExpression?.logical ? [trac.metadata.SearchExpression.create({logical: searchExpression.logical})] : searchExpression?.term ? [trac.metadata.SearchExpression.create({term: searchExpression.term})] : [defaultSearchExpression]

        const results = await searchByMultipleAndClauses({
            includeOnlyShowInSearchResultsIsTrue: true,
            maximumNumberOfOptionsReturned,
            objectType: trac.ObjectType.MODEL,
            searchAsOf,
            tenant,
            terms
        })

        // Update the progress bar
        dispatch(updateCompletedApiCalls())

        // We sort by objectTimestamp rather than the default tagTimestamp so that when tags are changed the
        // order does not change
        return {type: "model", key: modelKey, results: results.sort(sortOptionsByObjectTimeStamp)}
    })

    // Execute the array of searches. Note that the returned value is an array with the objects inside having
    // results as an array of tags.
    const searchResults = await Promise.all(modelSearches)

    // We need to make a set of additional requests to get the metadata for the default options selected.
    // This list and its order needs to be kept in sync with modelsBatchMetadataRequest
    let modelRequestTypes: { type: "model-new" | "model-rerun", key: string }[] = []

    // An array of tag headers that we need to get the full metadata for, for example if we are setting a default option we
    // need to get the definition of it, this list contains the header for the request. This list and its order needs to be
    // kept in sync with 'modelRequestTypes'
    let modelsBatchMetadataRequest: trac.metadata.ITagSelector[] = []

    // We need to keep track of which models were nulled because they were not in the refreshed list, if these are
    // not optional then the validation for them should be turned to false
    let modelsThatWereNulled: string[] = []

    // WARNING
    // If you change the below code you need to make sure that you always key the objects by the key used by the flow.
    // You should never key by the key of the model as there is not always a 1-2-1 mapping, indeed flows can use bespoke
    // searches to define what they need and none of the results necessarily have a key that matches the model keys in the flow.

    // The selected models to fill in with the default selections
    const selectedModelOptions = createFlowSelectedModelOptionsTemplate(flowDefinition)
    const modelTypes = categoriseFlowModelTypes(flowDefinition)

    // Now go through the search results for each model and input in the flow
    searchResults.forEach(searchResult => {

        // Results here is an array of tags
        const {key, results} = searchResult

        // Turn the model search results into options and add them to the job configuration. tagVersion is not used in
        // setting the option key so that when re-running a job the options from the searches can match the object used
        // in the job that is being re-run even if the tag has been updated since.
        modelOptions[key][2].options = convertSearchResultsIntoOptions(results, showObjectId, showVersions, showUpdatedDate, showCreatedDate, false)

        // First handle the case where we are re-running a job
        if (rerun.job !== undefined && !isRefresh) {

            // Get the tag selector of the object used (by the key in the flow
            const modelUsed = rerun.job.definition?.job?.runFlow?.models?.[key]

            if (modelUsed != undefined) {

                // Add the header for the additional tag we need to a list
                modelsBatchMetadataRequest.push(modelUsed)

                // Add the extra request to the list of requests to make
                modelRequestTypes.push({type: "model-rerun", key})
            }

            // The selected option is set when the metadata for it comes back
        }
        // Now handle where is a new run being set up, or a refresh of the options is happening
        else {

            // Has the user already selected an option, say for a flow that they have just changed
            const alreadySelectedOption = existingSelectedModelOptions[key]

            // Does the existing option exist in the new search results
            const {index} = alreadySelectedOption != null ? getGroupOptionIndex(alreadySelectedOption, [modelOptions[key][2]]) : {index: -1}

            // If the user has already selected an item, then if this item still exists after changing the flow or hitting the refresh button then
            // retain that as the selected option. If we are building a new flow and the item does not exist then pick the first item in the list,
            // if we are refreshing the options and the selected item does not exist then null the selection.
            // We need to clone the alreadySelectedOption as it is read only, and we modify it with the tag later on
            const selectedOption = index > -1 ? cloneDeep(alreadySelectedOption) : (!isRefresh ? (modelOptions[key][2].options[0] || null) : null)

            // Save the selected option
            selectedModelOptions[key] = selectedOption

            if (alreadySelectedOption !== null && selectedOption === null) {
                modelsThatWereNulled.push(key)
            }

            // Get the tag for the selected input option, these won't be in the search results
            if (selectedOption?.tag?.header && !selectedOption?.tag?.definition?.model) {

                // Add the header for the additional tag we need to a list
                modelsBatchMetadataRequest.push(selectedOption?.tag?.header)

                // Add the extra request to the list of requests to make
                modelRequestTypes.push({type: "model-new", key})
            }
        }
    })

    // Get the input and model metadata - this is using the client side metadata store, this consists of the metadata
    // we need for the default selected options
    const modelMetadataResults = await dispatch(checkForBatchMetadata(modelsBatchMetadataRequest)).unwrap()

    // Update the progress bar
    dispatch(updateCompletedApiCalls())

    // So now we need to put the metadata into the options for the currently selected models
    modelMetadataResults.forEach((result, i) => {

        if (modelRequestTypes[i].type === "model-rerun") {

            // Turn the search result into an option in a different part of the dropdown, we
            // don't have to add the tag into the tag property as this function will do this for us (we already got the full metadata
            // whereas in the search results we didn't)
            modelOptions[modelRequestTypes[i].key][1].options = convertSearchResultsIntoOptions([result], showObjectId, showVersions, showUpdatedDate, showCreatedDate, false)

            const selectedOption = {...modelOptions[modelRequestTypes[i].key][1].options[0]} || null

            // Add an icon to say that this is the option that matches the job to re-run
            modelOptions[modelRequestTypes[i].key][1].options[0].icon = "bi-check-circle"

            // Set the option as the selected option
            selectedModelOptions[modelRequestTypes[i].key] = {...selectedOption, icon: "bi-check-circle"}

            // If the rerun option exists in the search list then we update the option there too
            if (selectedOption) {

                // Is the selected option in the re-run section (index 1), if so hide it in the search options (index 2)
                modelOptions[modelRequestTypes[i].key] = hideDuplicatedGroupOptions(selectedOption, modelOptions[modelRequestTypes[i].key], 1, 2)
            }

        } else if (modelRequestTypes[i].type === "model-new") {

            // Get the current selected option
            let selectedOption = selectedModelOptions[modelRequestTypes[i].key]

            // If there is an option selected
            if (selectedOption) {

                // Put the metadata tag into the option
                selectedOption.tag = result

                // Find the option in the option group
                const {tier, index} = getGroupOptionIndex(selectedOption, modelOptions[modelRequestTypes[i].key])

                // If found then put the option with the tag added back into the list
                if (tier > -1 && index > -1) {
                    modelOptions[modelRequestTypes[i].key][tier].options[index] = selectedOption
                }
            }
        }
    })

    // Put the options for the input datasets into the input definitions
    Object.keys(modelOptions).forEach(key => {

        // If the user has selected to re-run a job and then refreshed the list of models then the new options here
        // can contain the same option, we want to avoid the duplication
        if (rerun.job && modelOptions[key][1].options?.[0] && isRefresh) {

            // If there is an item in the re-run section (index 1), if so hide it in the search options (index 2)
            modelOptions[key] = hideDuplicatedGroupOptions(modelOptions[key][1].options[0], modelOptions[key], 1, 2)
        }
    })

    // The fulfilled lifecycle hook contains logic to set the validation of any models set to null as a result of a refresh
    // to false (invalid). There is no logic to rebuild the parameter menu or to set the validation of the value to invalid.
    // As it stands when models are refreshed and a model is made null as it is no longer in the list the parameter menu stays
    // as it is. The reason is that the user will HAVE to select a model to run the job (there is no concept of an optional model)
    // Meaning that the parameter menu will need to be rebuilt when the model is changed.

    return {
        modelOptions,
        modelsThatWereNulled,
        modelTypes,
        selectedModelOptions,
        // When the payload is not passed then the user is not building a job but refreshing the models.
        // When building a job the 'buildRunAFlowJobConfiguration' function updates the validation, but when
        // refreshing we need to update it in this function.
        updateValidation: Boolean(payload === undefined)
    }
})

/**
 * A function that adds an option for a model or an input to the list of options. This runs when the user pastes
 * an object ID or key into a SelectOption component for a model and passes the TRAC object information to this
 * function to handle. It gets the metadata for the model and adds that into the option.
 */
export const addModelOrInputOption = createAsyncThunk<// Return type of the payload creator
    { id: string, option: SearchOption },
    // First argument to the payload creator.
    OnCreateNewOptionPayload<trac.metadata.ITag>,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAFlowStore/addModelOrInputOption', async (payload, {getState, dispatch}) => {

    const {id, newOptions} = payload

    if (typeof id !== "string") throw new TypeError("The id passed to the 'addModelOrInputOption' function was not a string")

    if (newOptions.length === 0 || !newOptions[0].tag.header) throw new Error("No new option was passed to the 'addModelOrInputOption' function")

    // Get the variables that determine how to show the labels
    const modelOrInput = newOptions[0].tag.header?.objectType === trac.metadata.ObjectType.MODEL ? "models" : "inputs"
    const {showObjectId, showVersions, showUpdatedDate, showCreatedDate} = getState().runAFlowStore.optionLabels[modelOrInput]

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
    { job: trac.metadata.ITag, flow: trac.metadata.ITag },
    // First argument to the payload creator
    { job: trac.metadata.ITag, flow: trac.metadata.ITag },
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAFlowStore/setJobToRerun', async (payload, {dispatch}) => {

    // Rest the attributes back to the default, if the user has changed the model then we reset the
    // attributes back to the default as they may be no longer valid, this clearing will not be
    // visible to the user as the attributes are on the review page and the widget to change the
    // model is on the setup page.
    dispatch(resetAttributesToDefaults({storeKey: "runAFlow"}))

    // We want to set the name for the job as the name of the job that is being rerun
    const jobName = payload.job?.attrs?.name?.stringValue ?? null
    const businessSegments = extractValueFromTracValueObject(payload.job.attrs?.business_segments)

    // Update the job attribute
    dispatch(setAttributesAutomatically({storeKey: "runAFlow", values: {name: jobName, business_segments: businessSegments.value ?? null}}))

    return payload
})

/**
 * A function that initiates a job in TRAC, it gets the information for the flow, collates all the parameters,
 * inputs and models the user selected and converts them into the format required by the TRAC API.
 */
export const runJob = createAsyncThunk<// Return type of the payload creator
    trac.api.IJobStatus,
    // First argument to the payload creator
    void,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('runAFlowStore/runJob', async (_, {dispatch, getState}) => {

    // Get what we need from the store
    const {"trac-tenant": tenant} = getState().applicationStore.cookies

    // First extract the parameters
    const {values: parameterValues, parameterDefinitions} = getState().runAFlowStore.parameters

    let parameters: Record<string, trac.metadata.IValue> = {}

    Object.entries(parameterDefinitions).forEach(([key, value]) => {

        const tag = createTag(key, value.basicType, parameterValues[key])
        if (tag.value) parameters[key] = tag.value
    })

    // Next extract the inputs
    const {selectedInputOptions} = getState().runAFlowStore.inputs

    let inputs: Record<string, trac.metadata.ITagSelector> = {}

    Object.entries(selectedInputOptions).forEach(([key, value]) => {

        if (value?.tag?.header) inputs[key] = value.tag.header
    })

    // Next extract the models
    const {selectedModelOptions} = getState().runAFlowStore.models

    let models: Record<string, trac.metadata.ITagSelector> = {}

    Object.entries(selectedModelOptions).forEach(([key, value]) => {

        if (value?.tag?.header) models[key] = value.tag.header
    })

    // Next get the flow
    const {selectedFlowOption} = getState().runAFlowStore.flows

    if (selectedFlowOption == null) throw new Error("No flow is selected to run")

    // Next get the attributes
    const {processedAttributes, values} = getState().setAttributesStore.uses.runAFlow.attributes

    const jobAttrs = createTagsFromAttributes(processedAttributes, values)

    // If the user set the job tag then extract that into a separate tag array to set on all the outputs from the job
    const outputAttrs = typeof values.job_tag === "string" ? createTagsFromAttributes({"job_tag": processedAttributes.job_tag}, {"job_tag": values.job_tag}) : undefined

    const jobRequest = {
        tenant: tenant,
        job: {
            jobType: trac.JobType.RUN_FLOW,
            runFlow: {
                flow: selectedFlowOption.tag.header,
                inputs,
                parameters,
                models,
                outputAttrs
            }
        },
        jobAttrs: jobAttrs,
    }

    // This is a bit of hackery or magic not sure which. We want to return the result from the
    // successful submission, but we also want to clear the attributes the user entered.
    return await submitJob(jobRequest).then((result) => {
        dispatch(resetAttributesToDefaults({storeKey: "runAFlow"}))
        return result
    });
})

export const runAFlowStore = createSlice({
    name: 'runAFlowStore',
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
         * A reducer that changes the scene to set up mode, where all the various inputs/models/parameters can be changed.
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
        setShowKeys: (state, action: PayloadAction<{ name: "inputs" | "models" | "parameters" }>) => {

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
         * A reducer that sets the number of API calls are needed to build the flow. This is used to show progress to
         * the user.
         */
        setNumberOfApiCalls: (state, {payload}: PayloadAction<number>) => {

            state.flow.numberOfApiCalls = {toDo: payload, completed: 0}
        },
        /**
         * A reducer that updates how many of the API calls have completed when building the flow. This is used to show
         * progress to the user.
         */
        updateCompletedApiCalls: (state, {payload}: PayloadAction<undefined | number>) => {

            state.flow.numberOfApiCalls.completed = state.flow.numberOfApiCalls.completed + (payload ? payload : 1)
        },
        /**
         * A reducer that sets whether the model chain editor should be seen.
         */
        setShowModelChain: (state) => {

            state.modelChain.showModelChain = !state.modelChain.showModelChain
        },
        /**
         * A reducer that toggles whether to show the summary info for a particular model in the chain.
         */
        setModelInfoToShow: (state, action: PayloadAction<string>) => {

            // If there is no entry for the model key then the info will be shown (added in as true), otherwise flip the value
            if (!state.modelChain.modelInfoToShow.hasOwnProperty(action.payload)) {
                state.modelChain.modelInfoToShow[action.payload] = true
            } else {
                delete state.modelChain.modelInfoToShow[action.payload]
            }
        },
        /**
         * A reducer that sets all models to either have their summary info shown or hidden, in big chains you don't want to have to
         * click on each model separately.
         */
        toggleToShowAllModelInfo: (state) => {

            // If all the models have their summary info shown then clicking hides all, otherwise it shows all
            if (Object.keys(state.modelChain.modelInfoToShow).length === Object.keys(state.models.modelOptions).length) {

                state.modelChain.modelInfoToShow = {}

            } else {

                Object.keys(state.models.modelOptions).forEach(key => {
                    if (state.models.selectedModelOptions[key]) state.modelChain.modelInfoToShow[key] = true
                })
            }
        },
        /**
         * A reducer that runs when the user changes one of the options in the Toolbar component to change how the
         * option labels show (e.g. showing the flow versions). The name and id are used to set the property in the
         * store to update. After the change the labels on all the relevant options in the store and the selected option
         * are updated.
         */
        updateOptionLabels: (state, action: PayloadAction<{ name: "flows" | "inputs" | "models" | "policies", id: "showObjectId" | "showVersions" | "showCreatedDate" | "showUpdatedDate" }>) => {

            const {id, name} = action.payload
            state.optionLabels[name][id] = !state.optionLabels[name][id]

            if (name === "flows") {

                // Get the variables that determine how to show the labels
                const {showObjectId, showVersions, showUpdatedDate, showCreatedDate} = state.optionLabels.flows

                // If the user changes how they want the labels to display we update all the options
                state.flows.flowOptions[0].options = updateObjectOptionLabels(state.flows.flowOptions[0].options, showObjectId, showVersions, showUpdatedDate, showCreatedDate)
                state.flows.flowOptions[1].options = updateObjectOptionLabels(state.flows.flowOptions[1].options, showObjectId, showVersions, showUpdatedDate, showCreatedDate)
                state.flows.flowOptions[2].options = updateObjectOptionLabels(state.flows.flowOptions[2].options, showObjectId, showVersions, showUpdatedDate, showCreatedDate)

                // We also update the label on the selected option if set
                if (state.flows.selectedFlowOption) {
                    state.flows.selectedFlowOption = updateObjectOptionLabels([state.flows.selectedFlowOption], showObjectId, showVersions, showUpdatedDate, showCreatedDate)[0]
                }

            } else if (name === "policies") {

                // Get the variables that determine how to show the labels
                const {showVersions, showObjectId, showUpdatedDate, showCreatedDate} = state.optionLabels.policies

                // If the user changes how they want the labels to display we update all the options
                state.policies.options = updateObjectOptionLabels(state.policies.options, showObjectId, showVersions, showUpdatedDate, showCreatedDate)

                // We also update the label on the selected option if set
                if (state.policies.selectedPolicyOption && state.policies.selectedPolicyOption.value !== noPolicyOption.value) {
                    state.policies.selectedPolicyOption = updateObjectOptionLabels([state.policies.selectedPolicyOption], showObjectId, showVersions, showUpdatedDate, showCreatedDate)[0]
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

            } else if (name === "models") {

                // Get the variables that determine how to show the labels
                const {showVersions, showObjectId, showUpdatedDate, showCreatedDate} = state.optionLabels.models

                // If the user changes how they want the labels to display we update all the options
                Object.values(state.models.modelOptions).forEach(options => {
                    if (options && isGroupOption(options)) {
                        // @ts-ignore
                        options[0].options = updateObjectOptionLabels(options[0].options, showObjectId, showVersions, showUpdatedDate, showCreatedDate)
                        // @ts-ignore
                        options[1].options = updateObjectOptionLabels(options[1].options, showObjectId, showVersions, showUpdatedDate, showCreatedDate)
                        // @ts-ignore
                        options[2].options = updateObjectOptionLabels(options[2].options, showObjectId, showVersions, showUpdatedDate, showCreatedDate)
                    }
                })

                // We also update the label on the selected option if set
                Object.entries(state.models.selectedModelOptions).forEach(([key, selectedOption]) => {
                    if (selectedOption) {
                        state.models.selectedModelOptions[key] = updateObjectOptionLabels([selectedOption], showObjectId, showVersions, showUpdatedDate, showCreatedDate)[0]
                    }
                })
            }
        },
        /**
         * A reducer that adds an option for a flow to the list of options. This runs when the user pastes an object
         * ID or key into a SelectOption component for the flows and passes the TRAC object information to this
         * function to handle.
         */
        addFlowOption: (state, action: PayloadAction<OnCreateNewOptionPayload<trac.metadata.ITag>>) => {

            // Get the variables that determine how to show the labels
            const {showObjectId, showVersions, showUpdatedDate, showCreatedDate} = state.optionLabels.flows

            const {newOptions} = action.payload

            // Update the label to reflect any user changes to what they want
            const optionWithUpdatedLabel = updateObjectOptionLabels(newOptions, showObjectId, showVersions, showUpdatedDate, showCreatedDate)[0]

            // Add the new option to the list, other added items are retained
            state.flows.flowOptions[0].options.push(optionWithUpdatedLabel)

            // Set the option to be the new value
            state.flows.selectedFlowOption = optionWithUpdatedLabel

            // If the selected flow already exists in the list then the SelectOption component will just find it rather than search for it but to be safe
            // we remove any added items from the search results, if the user searches by object key there may be a case that this is needed
            state.flows.flowOptions[2].options = state.flows.flowOptions[2].options.filter(option => option.value !== optionWithUpdatedLabel.value)

            // Remove any info about re-running a job
            state.rerun.job = undefined

            // Tell the application that this job has not been built
            state.flow.jobHasBeenBuilt = false

            showToast("success", "New flow option successfully added", "runAFlowStore/addFlowOption")
        },
        /**
         * A reducer that stores the selected policy option into the store.
         */
        setPolicyOption: (state, action: PayloadAction<SelectOptionPayload<SearchOption, false>>) => {

            state.policies.selectedPolicyOption = action.payload.value

            // If the user has not selected the 'No policy' option then we need to set all options to the ones in
            // policy
            if (action.payload.value && action.payload.value.value !== "NONE") {
            } else {
                // If the user selects the 'No policy' option then we need to clear any details of the job that we
                // were recreating
                state.rerun.job = undefined

                Object.entries(state.models.modelOptions).forEach(([key, groups]) => {

                    state.models.modelOptions[key][2].options = groups[2].options.map(option => {
                        let newOption = {...option};
                        delete newOption.icon;
                        return newOption;
                    })
                })

                Object.entries(state.models.selectedModelOptions).forEach(([key, option]) => {

                    let newOption = !option ? option : {...option};
                    if (newOption) delete newOption.icon;

                    state.models.selectedModelOptions[key] = newOption
                })

                Object.entries(state.inputs.inputDefinitions).forEach(([key, processedInput]) => {
                    if (processedInput) {

                        const groups = processedInput?.options as TracGroup

                        if (groups && groups.length > 0 && state.inputs?.inputDefinitions[key]?.options) {

                            const options = state.inputs?.inputDefinitions[key]?.options

                            if (options) {
                                // @ts-ignore
                                options[2].options = groups[2].options.map(option => {
                                        let newOption = {...option};
                                        delete newOption.icon;
                                        return newOption;
                                    }
                                )
                            }
                        }
                    }
                })

                Object.entries(state.inputs.selectedInputOptions).forEach(([key, value]) => {
                    if (value) {

                        let newOption = !value ? value : {...value};
                        if (newOption) delete newOption.icon;

                        state.inputs.selectedInputOptions[key] = newOption

                    }
                })
            }
        },
        /**
         * A reducer that stores the metadata object for the selected policy into the store.
         */
        setPolicyMetadata: (state, action: PayloadAction<trac.metadata.ITag>) => {

            state.policy.selectedPolicyObject = action.payload
        },
        /**
         * A reducer that takes the selected models for a job and gets the parameters from the model definitions,
         * it handles cases like where there are different labels, basic types and default values across the
         * parameter definitions. It also handles when the user has selected to re-run a job. This is run in
         * two cases, first when building a job after selecting a flow and second, when the user changes a model
         * to run, and we need to revalidate/update the parameters.
         */
        getParameters: (state, action: PayloadAction<{ selectedModelOptions: Models["selectedModelOptions"], updateValidation: boolean, isSettingUpRerun: boolean, isModelChange: boolean }>) => {

            const {isModelChange, isSettingUpRerun, selectedModelOptions, updateValidation} = action.payload

            // Flows don't have to have the parameters defined in them, so they are built from the models in them.
            // First extract the parameter values from the models selected

            // If changing the model means the parameters definitions may have changed, some values already set will be invalid
            // and new parameters may be required. So rebuild the parameters
            let newParametersFromModels = buildParametersAcrossMultipleModels(selectedModelOptions)

            // If a job has been built in response to the user wanting to rerun a job we need to set the parameters
            // to be the same as the job

            // The setProcessedParameters function has all the logic to set the default values of each parameter, including when it is an array.
            // Rather than duplicate the logic here, we modify the default values from the parametersFromModels array. Since we are re-running
            // a job we don't have to check the types as the original values were good to use originally, but we check anyway

            if (isSettingUpRerun) {
                Object.entries(state.rerun.job?.definition?.job?.runFlow?.parameters || {}).forEach(([key, value]) => {

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

            // Note that at this point some parameters can be null

            if (isModelChange) {
                // Go through each new parameter and see if the old value can still be used, if not it will be set as the default
                // value from the new definition
                Object.keys(newParameterValues || {}).forEach((key) => {

                    // Get the parameter index from the model definition
                    const newParameterIndex = newParametersFromModels.findIndex(newParametersFromModel => newParametersFromModel.key === key)
                    const oldBasicType = state.parameters.parameterDefinitions[key]?.basicType ?? trac.BasicType.BASIC_TYPE_NOT_SET

                    // If there was an entry for the parameter with the same key in the old set of models (the user may have set a value that we
                    // may want to carry over). It must be the same type though.
                    // TODO the param type of the parameterDefinition does not include array and map, these are in special type so this
                    //  check may need to be improved
                    if (newParameterIndex > -1 && oldBasicType === newParametersFromModels[newParameterIndex].value.paramType?.basicType && !state.parameters.parameterDefinitions[key]?.useOptions && !newParameterDefinitions[key]?.useOptions) {

                        newParameterValues[key] = state.parameters.values[key]
                    }

                    // TODO, if both are options, of the same type then use them if there are no list that things must come from. If there is a list then make it pick those that are in the list
                })
            }

            // Update the parameter validations, if the user changes from a model with a default value to one without then a
            // valid value will be set but could then be changed to null, null values are invalid. This happens if the type
            // of variable changes. So here we update the validation status of anything changed to null as false.
            Object.entries(newParameterValues).forEach(([key, value]) => {
                if (state.parameters.values.hasOwnProperty(key) && state.parameters.values[key] !== null && value === null) {
                    state.validation.isValid.parameters[key] = false
                }
            })

            state.parameters.parameterDefinitions = newParameterDefinitions
            state.parameters.values = newParameterValues
            state.parameters.lastParameterChanged = null

            // Update the job validation only is not being in isolation and not with 'buildRunAFlowJobConfiguration
            if (updateValidation) runAFlowStore.caseReducers.validateJobSelections(state)
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
                state.flow.userChanged.what.parameter = true
                state.flow.userChanged.something = true

                state.validation.isValid.parameters[action.payload.id] = action.payload.isValid
                state.validation.validationChecked = false
            }
        },
        /**
         * A reducer that checks all the models, parameters and input datasets selected and works out if there are any issues
         * that will mean that the job will error.
         */
        validateJobSelections: (state) => {

            const {flow: {flowMetadata}, models: {selectedModelOptions, modelOptions}, inputs: {selectedInputOptions, inputDefinitions, inputTypes}} = state

            const inputOptions: Record<string, GenericGroup[]> = {}
            Object.entries(inputDefinitions).forEach(([key, inputDefinition]) => {

                if (inputDefinition.options && isGroupOption(inputDefinition.options)) inputOptions[key] = inputDefinition.options
            })

            // Run all the validation tests to check that the selections are valid, the 'review job' button is disabled if a check fails
            state.validation = {
                ...{
                    isValid: state.validation.isValid,
                    validationChecked: state.validation.validationChecked
                }, ...validateFlowSelections(flowMetadata?.definition?.flow, selectedModelOptions, selectedInputOptions, modelOptions, inputOptions, inputTypes)
            }
        }
    },
    extraReducers: (builder) => {

        // A set of lifecycle reducers to run before/after the getFlows function
        builder.addCase(getFlows.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.flows.status = "pending"
            state.flows.message = undefined
        })
        builder.addCase(getFlows.fulfilled, (state, action: PayloadAction<SearchOption[]>) => {

            state.flows.status = "succeeded"
            state.flows.message = undefined
            state.flows.flowOptions[2].options = action.payload

            // If the user has selected to re-run a job and then refreshed the list of flows then the payload here
            // can contain the same option, we want to avoid the duplication
            if (state.flows.flowOptions[1].options?.[0] && state.rerun.job) {

                // If there is an item in the re-run section (index 1), if so hide it in the search options (index 2)
                state.flows.flowOptions = hideDuplicatedGroupOptions(state.flows.flowOptions[1].options[0], state.flows.flowOptions, 1, 2)
            }

            // Now that we have the new options we need to check if the selected flow and policy options ares still
            // in the list if it is we do nothing, if not we need to remove the selected option and also clear the part
            // of the state dealing with the configured job
            if (state.flows.selectedFlowOption && action.payload.findIndex(option => state.flows.selectedFlowOption && state.flows.selectedFlowOption.value === option.value) === -1) {

                state.flows.selectedFlowOption = null
                // WARNING be careful resetting some parts of state can trigger changes that cause hooks to execute
                state.flow = setupFlow()
                //state.policies = setupPolicies()
                //state.policy = setupPolicy()
                state.modelChain = {...initialState.modelChain}
                // We need to hide the model chain which requires the flow status to be reset
                state.models = setupModels()
                state.parameters = setupParameters()
                state.inputs = setupInputs()
                state.validation = setupValidation()
                // Remove the rerun policy items
                state.rerun = setupRerun()
            }
        })
        builder.addCase(getFlows.rejected, (state, action) => {

            state.flows.status = "failed"

            const text = {
                title: "Failed to get a list of flows",
                message: "The search for flows based on your business segments did not complete successfully.",
                details: action.error.message
            }

            showToast("error", text, "runAFlowStore/getFlows/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the buildRunAFlowJobConfiguration function
        builder.addCase(buildRunAFlowJobConfiguration.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.flow.status = "pending"
            state.flow.message = undefined
        })
        builder.addCase(buildRunAFlowJobConfiguration.fulfilled, (state, action: PayloadAction<{
            // Flow
            flowMetadata: Flow["flowMetadata"]
        }>) => {

            // Update the flow details
            state.flow.status = "succeeded"
            state.flow.message = undefined
            state.flow.jobHasBeenBuilt = true
            state.flow.flowMetadata = action.payload.flowMetadata

            // Update the job validation
            runAFlowStore.caseReducers.validateJobSelections(state)
        })
        builder.addCase(buildRunAFlowJobConfiguration.rejected, (state, action) => {

            state.flow.status = "failed"

            const text = {
                title: "Failed to get the flow",
                message: "The request to get the selected flow did not complete successfully.",
                details: action.error.message
            }

            showToast("error", text, "runAFlowStore/buildRunAFlowJobConfiguration/rejected")
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

            // Update the input API status details
            state.inputs.status = "succeeded"
            state.inputs.message = undefined

            // Update the inputs
            state.inputs.inputDefinitions = action.payload.inputDefinitions
            state.inputs.inputTypes = action.payload.inputTypes
            state.inputs.selectedInputOptions = action.payload.selectedInputOptions

            // Any inputs that were nulled need to have their validation set to false if they are non-optional
            action.payload.inputsThatWereNulled.forEach(key => {
                console.log(state.inputs.inputTypes)
                if (!state.inputs.inputTypes.optional.includes(key)) {
                    state.validation.isValid.inputs[key] = false
                }
            })

            // Update the job validation only is not being in isolation and not with 'buildRunAFlowJobConfiguration
            if (action.payload.updateValidation) runAFlowStore.caseReducers.validateJobSelections(state)
        })
        builder.addCase(getInputs.rejected, (state, action) => {

            state.inputs.status = "failed"

            const text = {
                title: "Failed to get the input datasets",
                message: "The request to get the input datasets for the flow did not complete successfully.",
                details: action.error.message
            }

            showToast("error", text, "runAFlowStore/getInputs/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the getModels function
        builder.addCase(getModels.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.models.status = "pending"
            state.models.message = undefined
        })
        builder.addCase(getModels.fulfilled, (state, action: PayloadAction<{
            // Models
            modelOptions: Models["modelOptions"]
            modelsThatWereNulled: string[]
            selectedModelOptions: Models["selectedModelOptions"]
            modelTypes: Models["modelTypes"]
            updateValidation: boolean
        }>) => {

            // Update the flow details
            state.models.status = "succeeded"
            state.models.message = undefined

            // Update the models
            state.models.modelOptions = action.payload.modelOptions
            state.models.selectedModelOptions = action.payload.selectedModelOptions
            state.models.modelTypes = action.payload.modelTypes

            // The SelectOption component for selecting a model is not mounted until the user
            // views the chain and decides to select the model. So validation status will not
            // be set automatically, so here we do this, so we can check if the job is valid.
            // This also serves to update validation if a model selection is nulled during
            // a refresh
            Object.entries(action.payload.selectedModelOptions).forEach(([key, selectedOption]) => {

                state.validation.isValid.models[key] = Boolean(selectedOption !== null)
            })

            // Update the job validation only is not being in isolation and not with 'buildRunAFlowJobConfiguration
            if (action.payload.updateValidation) runAFlowStore.caseReducers.validateJobSelections(state)
        })
        builder.addCase(getModels.rejected, (state, action) => {

            state.models.status = "failed"

            const text = {
                title: "Failed to get the models",
                message: "The request to get the models for the flow did not complete successfully.",
                details: action.error.message
            }

            showToast("error", text, "runAFlowStore/getModels/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the getPolicies function
        builder.addCase(getPolicies.pending, (state) => {

            // Clear all the messages
            //toast.dismiss()
            state.policies.status = "pending"
            state.policies.message = undefined
        })
        builder.addCase(getPolicies.fulfilled, (state, action: PayloadAction<SearchOption[]>) => {

            state.policies.status = "succeeded"
            state.policies.message = undefined
            state.policies.options = action.payload

            // Now that we have the new options we need to check if the selected option is still in the list
            // if it is we do nothing, if not we need to remove the selected option and also clear the part of the
            // state dealing with policy
            if (!state.policies.selectedPolicyOption || (state.policies.selectedPolicyOption && action.payload.findIndex(option => state.policies.selectedPolicyOption && state.policies.selectedPolicyOption.value === option.value) === -1)) {
                state.policies.selectedPolicyOption = noPolicyOption
                // state.flow = setupFlow()
                // state.policies = setupPolicies()
                // state.policy = setupPolicy()
                // state.models = setupModels()
                // state.parameters = setupParameters()
                // state.inputs = setupInputs()
                // state.validation = setupValidation()
            }
        })
        builder.addCase(getPolicies.rejected, (state, action) => {

            state.flows.status = "failed"

            const text = {
                title: "Failed to get a list of policies",
                message: "The search for policies applicable to your flow did not complete successfully.",
                details: action.error.message
            }

            showToast("error", text, "runAFlowStore/getPolicies/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the getPolicy function
        builder.addCase(getPolicy.pending, (state) => {

            // Clear all the messages
            //toast.dismiss()
            state.policy.status = "pending"
            state.policy.message = undefined
        })
        builder.addCase(getPolicy.fulfilled, (state, action: PayloadAction<void | trac.metadata.ITag>) => {

            state.policy.status = "succeeded"
            state.policy.message = undefined

            if (action.payload) {

                state.rerun.job = action.payload

                if (state.models) state.flow.userChanged = {
                    something: false,
                    what: {model: false, input: false, parameter: false}
                }
            }

        })
        builder.addCase(getPolicy.rejected, (state, action) => {

            state.flows.status = "failed"

            const text = {
                title: "Failed to get a the selected policy",
                message: "The request to get the details of the selected policy did not complete successfully.",
                details: action.error.message
            }

            showToast("error", text, "runAFlowStore/getPolicy/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the setInput function
        builder.addCase(setFlow.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.flow.status = "pending"
            state.flow.message = undefined
        })
        builder.addCase(setFlow.fulfilled, (state, action: PayloadAction<SelectOptionPayload<SearchOption, false>>) => {

            state.flow.status = "succeeded"
            state.flow.message = undefined

            state.flows.selectedFlowOption = action.payload.value

            // If the user changes selection then remove any details of the job that needs to be rerun. This is set
            // if the user selects a job to rerun. There is a hook in the UI that runs functions when the
            // selected flow option changes.
            state.rerun = setupRerun()
            state.flow.jobHasBeenBuilt = false
        })
        builder.addCase(setFlow.rejected, (state, action) => {

            state.inputs.status = "failed"

            const text = {
                title: "Failed to set a the selected flow",
                message: "The request to set the selected option did not complete successfully.",
                details: action.error.message
            }

            showToast("error", text, "runAFlowStore/setFlow/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the setModel function
        builder.addCase(setModel.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.models.status = "pending"
            state.models.message = undefined
        })
        builder.addCase(setModel.fulfilled, (state, action: PayloadAction<SelectOptionPayload<SearchOption, false>>) => {

            state.models.status = "succeeded"
            state.models.message = undefined

            const {id, isValid, value} = action.payload

            if (typeof id === "string") {

                state.validation.isValid.models[id] = isValid
                state.validation.validationChecked = false

                // Note value can be null
                state.flow.userChanged.what.model = true
                state.flow.userChanged.something = true
                state.models.selectedModelOptions[id] = value

                if (value) {

                    // Since the selected model now has the metadata for the model we also need to update the option in the group
                    // We need to do this for both the re-run and the search results options
                    const {tier: tier1, index: index1} = getGroupOptionIndex(value, [state.models.modelOptions[id][1]])

                    if (tier1 > -1 && index1 > -1) {
                        state.models.modelOptions[id][1].options[index1] = {
                            ...value,
                            disabled: state.models.modelOptions[id][1].options[index1].disabled,
                            icon: state.models.modelOptions[id][1].options[index1].icon
                        }
                    }

                    const {tier: tier2, index: index2} = getGroupOptionIndex(value, [state.models.modelOptions[id][2]])

                    if (tier2 > -1 && index2 > -1) {
                        state.models.modelOptions[id][2].options[index2] = {
                            ...value,
                            disabled: state.models.modelOptions[id][2].options[index2].disabled,
                            icon: state.models.modelOptions[id][2].options[index2].icon
                        }
                    }
                }

                // Check that all the parameters are valid
                runAFlowStore.caseReducers.getParameters(state, {
                    type: "getParameters",
                    payload: {isModelChange: true, isSettingUpRerun: false, selectedModelOptions: state.models.selectedModelOptions, updateValidation: false}
                })

                // Update the job validation
                runAFlowStore.caseReducers.validateJobSelections(state)
            }
        })
        builder.addCase(setModel.rejected, (state, action) => {

            state.models.status = "failed"

            const text = {
                title: "Failed to set a the selected model",
                message: "The request to set the selected option and get its details did not complete successfully.",
                details: action.error.message
            }

            showToast("error", text, "runAFlowStore/setModel/rejected")
            console.error(action.error)
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
                state.flow.userChanged.what.input = true
                state.flow.userChanged.something = true
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
                runAFlowStore.caseReducers.validateJobSelections(state)
            }
        })
        builder.addCase(setInput.rejected, (state, action) => {

            state.inputs.status = "failed"

            const text = {
                title: "Failed to set a the selected input",
                message: "The request to set the selected option and get its details did not complete successfully.",
                details: action.error.message
            }

            showToast("error", text, "runAFlowStore/setInput/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the addModelOrInputOption function
        builder.addCase(addModelOrInputOption.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.models.status = "pending"
            state.models.message = undefined
            state.inputs.status = "pending"
            state.inputs.message = undefined
        })
        builder.addCase(addModelOrInputOption.fulfilled, (state, action: PayloadAction<{ id: string, option: SearchOption }>) => {

            state.models.status = "succeeded"
            state.models.message = undefined
            state.inputs.status = "succeeded"
            state.inputs.message = undefined

            const {id, option} = action.payload

            // ID is the model key for the flow
            if (hasOwnProperty(state.models.modelOptions, id) && hasOwnProperty(state.models.selectedModelOptions, id)) {

                // The current set of options for the input
                const options = state.models.modelOptions[id]

                // This only works if the options for the inputs are in groups
                if (options && isGroupOption(options)) {

                    // Add the new option to the list, other added items are retained
                    options[0].options.push(option)
                }

                // Set the option to be the new value
                state.models.selectedModelOptions[id] = option

            } else if (hasOwnProperty(state.inputs.inputDefinitions, id) && hasOwnProperty(state.inputs.selectedInputOptions, id)) {

                // The current set of options for the input
                const options = state.inputs.inputDefinitions[id].options

                // This only works if the options for the inputs are in groups
                if (options && isGroupOption(options)) {

                    // Add the new option to the list, other added items are retained
                    options[0].options.push(option)
                }

                // Set the option to be the new value
                state.inputs.selectedInputOptions[id] = option
            }

            // Update the job validation
            runAFlowStore.caseReducers.validateJobSelections(state)

            const modelOrInput = option.tag.header?.objectType === trac.metadata.ObjectType.MODEL ? "model" : "input"

            showToast("success", `New ${modelOrInput} option successfully added`, "runAFlowStore/addModelOrInputOption")

        })
        builder.addCase(addModelOrInputOption.rejected, (state, action) => {

            state.models.status = "failed"
            state.inputs.status = "failed"

            const text = {
                title: "Failed to add the mew option",
                message: "The request to add a new option and get its details did not complete successfully.",
                details: action.error.message
            }

            showToast("error", text, "runAFlowStore/addModelOrInputOption/rejected")
            console.error(action.error)
        })

        // A single lifecycle reducers to run after the setJobToRerun function, it has no async parts so does not need pending and failure functions
        builder.addCase(setJobToRerun.fulfilled, (state, action: PayloadAction<{ job: trac.metadata.ITag, flow: trac.metadata.ITag }>) => {

            const {job, flow} = action.payload

            // Get the variables that determine how to show the labels
            const {
                showObjectId: flowShowObjectId,
                showVersions: flowShowVersions,
                showUpdatedDate: flowShowUpdatedDate,
                showCreatedDate: flowShowCreatedDate
            } = state.optionLabels.flows

            const {
                showObjectId: policyShowObjectId,
                showVersions: policyShowVersions,
                showUpdatedDate: policyShowUpdatedDate,
                showCreatedDate: policyShowCreatedDate
            } = state.optionLabels.policies

            // Is the tag for a run flow
            if (job.definition?.job?.runFlow && flow && job.header?.objectId) {

                state.setUpOrReview = "setup"
                state.rerun.job = job
                state.rerun.jobParameters = {}

                // Extract the parameters used by the job into an object and convert them into the format used by the
                // ParameterMenu component
                Object.entries(job?.definition?.job?.runFlow?.parameters || {}).forEach(([key, value]) => {

                    const extractedValue = extractValueFromTracValueObject(value)

                    let policyValue: null | number | string | boolean | Option | Option[] = null

                    if (Array.isArray(extractedValue.value)) {

                        policyValue = convertArrayToOptions(extractedValue.value, true, extractedValue.subBasicType)

                    } else if (!isObject(extractedValue.value)) {
                        policyValue = extractedValue.value
                    }

                    if (state.rerun.jobParameters) state.rerun.jobParameters[key] = policyValue
                })

                state.rerun.jobModels = {}

                Object.entries(job?.definition?.job?.runFlow?.models || {}).forEach(([key, value]) => {

                    const policyValue: SearchOption[] = convertSearchResultsIntoOptions([{header: value}], false, false, false, false, false)

                    if (state.rerun.jobModels) state.rerun.jobModels[key] = policyValue[0]
                })

                state.rerun.jobInputs = {}

                Object.entries(job?.definition?.job?.runFlow?.inputs || {}).forEach(([key, value]) => {

                    const policyValue: SearchOption[] = convertSearchResultsIntoOptions([{header: value}], false, false, false, false, false)

                    if (state.rerun.jobInputs) state.rerun.jobInputs[key] = policyValue[0]
                })

                // We need to tell the UI to rebuild the flow to get the right options and set the policy icons
                state.flow.jobHasBeenBuilt = false

                // Convert the flow metadata into an option, use the user options for how this should be viewed
                const newFlowOption = convertSearchResultsIntoOptions([flow], flowShowObjectId, flowShowVersions, flowShowUpdatedDate, flowShowCreatedDate, false)

                // Add an icon to say that this is the option that matches the job to re-run
                newFlowOption[0].icon = "bi-check-circle"

                // Note that the whole array is replaced with a single option, index 1 is the "ADDED FOR RERUN" sub list
                state.flows.flowOptions[1].options = [...newFlowOption]
                state.flows.selectedFlowOption = {...newFlowOption[0]}

                // If there is an item in the re-run section (index 1), if so hide it in the search options (index 2)x
                state.flows.flowOptions = hideDuplicatedGroupOptions(newFlowOption[0], state.flows.flowOptions, 1, 2)

                const newPolicyOptions = convertSearchResultsIntoOptions([job], policyShowObjectId, policyShowVersions, policyShowUpdatedDate, policyShowCreatedDate, false)

                // We now add in a policy option for the job, we edit the name of the job to show that it is a re-run
                if (!newPolicyOptions[0].label.startsWith("Re-run of")) {

                    newPolicyOptions[0].label = `Re-run of ${newPolicyOptions[0].label}`
                }

                // Add the re-run policy option to the list of options
                state.policies.options = [noPolicyOption, ...newPolicyOptions]
                // Set the re-run job policy option as the selected option
                state.policies.selectedPolicyOption = newPolicyOptions[0]
                state.policy.selectedPolicyObject = undefined
            }
        })

        // A set of lifecycle reducers to run before/after the runJob function
        builder.addCase(runJob.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.job.status = "pending"
            state.job.message = undefined
        })
        builder.addCase(runJob.fulfilled, (state, action: PayloadAction<trac.api.IJobStatus>) => {

            state.job.status = "succeeded"
            state.job.message = undefined

            const text = `The job to run the flow in TRAC was successfully started with job ID ${action.payload.jobId?.objectId}, you can see its progress in the 'Find a job' pages.`
            showToast("success", text, "runAFlowStore/runJob/fulfilled")
        })
        builder.addCase(runJob.rejected, (state, action) => {

            state.job.status = "failed"

            const text = {
                title: "Failed to start the job",
                message: "The request to start the job did not complete successfully.",
                details: action.error.message
            }

            showToast("error", text, "runAFlowStore/runJob/rejected")
            console.error(action.error)
        })
    }
})

// Action creators are generated for each case reducer function
export const {
    addFlowOption,
    getParameters,
    resetState,
    reviewJob,
    setListsOrTabs,
    setModelInfoToShow,
    setNumberOfApiCalls,
    setParameter,
    setPolicyOption,
    setPolicyMetadata,
    setShowKeys,
    setShowModelChain,
    setUpJob,
    setValidationChecked,
    toggleToShowAllModelInfo,
    updateCompletedApiCalls,
    updateOptionLabels
} = runAFlowStore.actions

export default runAFlowStore.reducer