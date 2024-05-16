/**
 * The main configuration of the Redux store, we import the individual slices and combine them into a single set of
 * root reducers. We also add an interception to all calls to all reducers so that when the user changes the searchAsOf
 * or tenant options all stores are reset. Finally, because we store information about TRAC in the stores we get a
 * warning about them not being serializable items, we tell Redux to ignore actions or parts of the slices that
 * generate these messages.
 */

import {combineReducers, configureStore} from '@reduxjs/toolkit'

/**
 * Import the stores for each individual scene/component. Note that the import is not explicitly
 * exported from the scene store. So if we import RunAModelStoreReducer what is actually
 * exported is batchImportDataStore.reducer. I believe that the magic of redux toolkit is
 * being used here.
 */
import applicationSetupStoreReducer from "./react/scenes/ApplicationSetupScene/store/applicationSetupStore";
import applicationStoreReducer from './react/store/applicationStore';
import batchImportDataStoreReducer from "./react/scenes/BatchImportDataScene/store/batchImportDataStore";
import businessSegmentsStoreReducer from "./react/components/BusinessSegments/businessSegmentsStore";
import dataAnalyticsStoreReducer from "./react/scenes/DataAnalyticsScene/store/dataAnalyticsStore";
import examplesStoreReducer from "./react/scenes/ExamplesScene/store/examplesStore";
import findInTracReducer from "./react/components/FindInTrac/findInTracStore";
import objectSummaryStoreReducer from "./react/scenes/ObjectSummaryScene/store/objectSummaryStore";
import overlayBuilderStoreReducer from "./react/components/OverlayBuilder/overlayBuilderStore";
import queryBuilderStoreReducer from "./react/components/QueryBuilder/queryBuilderStore";
import runAFlowStoreReducer from "./react/scenes/RunAFlowScene/store/runAFlowStore";
import runAModelStoreReducer from './react/scenes/RunAModelScene/store/runAModelStore';
import setAttributesStoreReducer from './react/components/SetAttributes/setAttributesStore';
import updateTagsStoreReducer from "./react/scenes/UpdateTagsScene/store/updateTagsStore";
import uploadADatasetStoreReducer from "./react/scenes/UploadADatasetScene/store/uploadADatasetStore";
import uploadAFlowStoreReducer from "./react/scenes/UploadAFlowScene/store/uploadAFlowStore";
import uploadFromGitHubStoreReducer from "./react/components/UploadFromGitHub/store/uploadFromGitHubStore";
import whereClauseBuilderStoreReducer from "./react/components/WhereClauseBuilder/whereClauseBuilderStore";

// A Typescript interface provided by Redux
import {AnyAction} from "redux";

const ignoredPaths = [
    // applicationSetupStore
    "applicationSetupStore.editor.items.ui_attributes_list.tag",
    "applicationSetupStore.editor.items.ui_attributes_list.fields",
    "applicationSetupStore.editor.items.ui_batch_import_data.tag",
    "applicationSetupStore.editor.items.ui_batch_import_data.fields",
    "applicationSetupStore.editor.items.ui_business_segment_options.tag",
    "applicationSetupStore.editor.items.ui_business_segment_options.fields",
    "applicationSetupStore.editor.items.ui_parameters_list.tag",
    "applicationSetupStore.editor.items.ui_parameters_list.fields",
    "applicationSetupStore.tracItems.items.ui_attributes_list.currentDefinition.tag",
    "applicationSetupStore.tracItems.items.ui_attributes_list.currentDefinition.fields",
    "applicationSetupStore.tracItems.items.ui_batch_import_data.currentDefinition.tag",
    "applicationSetupStore.tracItems.items.ui_batch_import_data.currentDefinition.fields",
    "applicationSetupStore.tracItems.items.ui_business_segment_options.currentDefinition.tag",
    "applicationSetupStore.tracItems.items.ui_business_segment_options.currentDefinition.fields",
    "applicationSetupStore.tracItems.items.ui_parameters_list.currentDefinition.tag",
    "applicationSetupStore.tracItems.items.ui_parameters_list.currentDefinition.fields",

    // findInTracStore
    "findInTracStore.uses.search.selectedTags.searchByAttributes",
    "findInTracStore.uses.search.selectedTags.searchByObjectId",
    "findInTracStore.uses.search.selectedResults.tags",

    // uploadAFlowStore
    "uploadAFlowStore.alreadyInTrac.tag",

    "findInTracStore.uses.findAJob.selectedTags.searchByAttributes",
    "findInTracStore.uses.findAJob.selectedTags.searchByObjectId",
    "findInTracStore.uses.findAJob.selectedResults.tags",

    "findInTracStore.uses.updateTags.selectedTags.searchByAttributes",
    "findInTracStore.uses.updateTags.selectedTags.searchByObjectId",
    "findInTracStore.uses.updateTags.selectedResults.tags",

    "findInTracStore.uses.dataAnalytics.selectedTags.searchByAttributes",
    "findInTracStore.uses.dataAnalytics.selectedTags.searchByObjectId",
    "findInTracStore.uses.dataAnalytics.selectedResults.tags",

    // uploadFromGitHubStore
    "uploadFromGitHubStore.uses.uploadAModel.file.alreadyInTrac.tag",
    "uploadFromGitHubStore.uses.uploadASchema.file.alreadyInTrac.tag",

    // uploadADatasetStore
    "uploadADatasetStore.file.file",
    "uploadADatasetStore.alreadyInTrac.tag",
    "uploadADatasetStore.alreadyInTrac.schema",
    "uploadADatasetStore.existingSchemas.options",
    "uploadADatasetStore.existingSchemas.selectedOption.tag",
    "uploadADatasetStore.priorVersion.options",
    "uploadADatasetStore.priorVersion.selectedOption.tag",

    // runAModelStore
    "runAModelStore.inputs.inputDefinitions",
    "runAModelStore.inputs.selectedInputOptions",
    "runAModelStore.models.modelOptions",
    "runAModelStore.models.selectedModelOption.tag",
    "runAModelStore.model.modelMetadata",
    "runAModelStore.rerun.job",
    "runAModelStore.rerun.jobParameters",
    "runAModelStore.rerun.jobInputs",

    // runAFlowStore
    "runAFlowStore.rerun.job",
    "runAFlowStore.rerun.jobParameters",
    "runAFlowStore.rerun.jobInputs",
    "runAFlowStore.rerun.jobModels",
    "runAFlowStore.flows.flowOptions",
    "runAFlowStore.flows.searchResults",
    "runAFlowStore.flows.selectedFlowOption.tag",
    "runAFlowStore.flow.selectedFlowObject",
    "runAFlowStore.flowSetup.modelOptions",
    "runAFlowStore.flowSetup.selectedModelOptions",
    "applicationStore.metadataStore",
    "runAFlowStore.flowSetup.inputOptions",
    "runAFlowStore.flowSetup.selectedInputOptions",
    "runAFlowStore.inputs.processedInputs",
    "uploadFromGitHubStore.file.alreadyInTrac.tag",
    "runAFlowStore.inputs.values.economic_scenario.tag",
    "runAFlowStore.flow.flowTag",
    "runAFlowStore.policies.options",
    "runAFlowStore.policies.selectedPolicyOption.tag",
    "runAFlowStore.inputs.values",
    "payload.selectedOption.value.tag",

    // updateTagsStore
    "updateTagsStore.tag",

    "objectSummaryStore.flow.searchResult.nodes.definition",
    "objectSummaryStore.flow.searchResult.pdf.pdfChartProps.graphLayout",
    "objectSummaryStore.flow.searchResult.nodes.selectedModelOutputs",
    "objectSummaryStore.flow.searchResult.nodes.selectedModelInputs",
    "objectSummaryStore.flow.searchResult.nodes.selectedDataset",
    "objectSummaryStore.flow.jobViewerRunFlow.nodes.definition",
    "objectSummaryStore.flow.jobViewerRunFlow.pdf.pdfChartProps.graphLayout",
    "objectSummaryStore.flow.jobViewerRunFlow.nodes.selectedModelOutputs",
    "objectSummaryStore.flow.jobViewerRunFlow.nodes.selectedModelInputs",
    "objectSummaryStore.flow.jobViewerRunFlow.nodes.selectedDataset",
    "objectSummaryStore.flow.runAFlow.nodes.definition",
    "objectSummaryStore.flow.runAFlow.pdf.pdfChartProps.graphLayout",
    "objectSummaryStore.flow.runAFlow.nodes.selectedModelOutputs",
    "objectSummaryStore.flow.runAFlow.nodes.selectedModelInputs",
    "objectSummaryStore.flow.runAFlow.nodes.selectedDataset",

    "runAFlowStore.flow.flowMetadata",
    "runAFlowStore.models.modelOptions",
    "runAFlowStore.models.selectedModelOptions",
    "runAFlowStore.inputs.inputDefinitions",
    "runAFlowStore.inputs.selectedInputOptions",
    "queryBuilderStore.uses.dataAnalytics.query",
    "dataAnalyticsStore.selectedData.tags",
    "overlayBuilderStore.uses.runAFlow.change",
    "whereClauseBuilderStore.uses.dataAnalytics.whereClause",
    "whereClauseBuilderStore.uses.runAFlow.whereClause",
    "applicationStore.platformInfo",
]

const ignoredActions = [

    // applicationStore
    "applicationStore/checkForBatchMetadata/rejected",
    "applicationStore/checkForMetadata/fulfilled",

    // applicationSetupStore
    "applicationSetupStore/getSetupItem/pending",
    "applicationSetupStore/getSetupItem/fulfilled",
    "applicationSetupStore/createSetupItem/fulfilled",
    "applicationSetupStore/getSetupItems/fulfilled",

    // uploadAFlowStore
    "uploadAFlowStore/saveFileToStore/fulfilled",
    "uploadAFlowStore/uploadFlowToTrac/fulfilled",

    // uploadADatasetStore
    "uploadADatasetStore/saveFileToStore/pending",
    "uploadADatasetStore/saveFileToStore/fulfilled",
    "uploadADatasetStore/addNewPriorVersionOption",
    "uploadADatasetStore/uploadDataToTrac/fulfilled",
    "uploadADatasetStore/uploadDataToTrac/pending",
    "uploadADatasetStore/setSelectedSchemaOption",

    // runAModelStore
    "runAModelStore/getModels/pending",
    "runAModelStore/getModels/fulfilled",
    "runAModelStore/buildRunAModelJobConfiguration/fulfilled",
    "runAModelStore/getInputs/pending",
    "runAModelStore/getInputs/fulfilled",
    "runAModelStore/getParameters",
    "runAModelStore/setInput/pending",
    "runAModelStore/setInput/fulfilled",
    "runAModelStore/runJob/fulfilled",
    "runAModelStore/setModel/fulfilled",
    "runAModelStore/setModel/pending",
    "runAModelStore/setJobToRerun/pending",
    "runAModelStore/setJobToRerun/fulfilled",

    //  runAFlowStore
    "runAFlowStore/runJob/fulfilled",
    "runAFlowStore/setJobToRerun/pending",
    "runAFlowStore/setJobToRerun/fulfilled",
    "runAFlowStore/getFlows/fulfilled",
    "runAFlowStore/buildRunAFlowJobConfiguration/fulfilled",
    "runAFlowStore/setFlowMetadata",
    "runAFlowStore/setFlow/pending",
    "runAFlowStore/setFlow/fulfilled",
    "runAFlowStore/setSelectedModelOption",
    "runAFlowStore/getInputs/pending",
    "runAFlowStore/getInputs/fulfilled",
    "runAFlowStore/getModels/pending",
    "runAFlowStore/getModels/fulfilled",
    "runAFlowStore/getParameters",
    "runAFlowStore/addModelOrInputOption/pending",
    "runAFlowStore/addModelOrInputOption/fulfilled",
    "runAFlowStore/setInput/rejected",

    // setSAttributesStore
    "setAttributesStore/setAttributesFromTag",

    // updateTagsStore
    "updateTagsStore/setTag",
    "updateTagsStore/updateTagInTrac/fulfilled",

    // uploadFromGitHubStore
    "uploadFromGitHubStore/getFile/fulfilled",
    "uploadFromGitHubStore/uploadSchemaToTrac/fulfilled",

    "applicationStore/checkForMetadata/pending",
    "applicationStore/checkForMetadata/fulfilled",
    "applicationStore/checkForBatchMetadata/pending",
    "applicationStore/checkForBatchMetadata/fulfilled",
    "applicationStore/addToMetadataStore",


    "findInTracStore/addManyObjectIdSearchResultsToTable",
    "findInTracStore/doSearch/fulfilled",
    "uploadFromGitHubStore/uploadModelToTrac/fulfilled",
    "runAFlowStore/setInput",
    "runAFlowStore/setJobToRerun",
    "runAFlowStore/updateObjectOptionLabels",
    "runAFlowStore/getPolicy/fulfilled",
    "objectSummaryStore/setFlowNodeOptions",
    "objectSummaryStore/savePdfVersionOfSvg",
    "objectSummaryStore/setSvgSettings",
    "objectSummaryStore/setSelectedNodeId",
    "findInTracStore/setTags",
    "findInTracStore/getSelectedRows",

    "editTagStore/updateTagInTrac/fulfilled",

    "runAFlowStore/setInput/pending",
    "runAFlowStore/setInput/fulfilled",
    "runAFlowStore/setModel/pending",
    "runAFlowStore/setModel/fulfilled",
    "queryBuilderStore/createQueryEntry",
    "dataAnalyticsStore/setSelectedTags",
    "overlayBuilderStore/createOverlayEntry",
    "whereClauseBuilderStore/createWhereClauseEntry",
    "applicationStore/setPlatformInfo",
    "findInTracStore/saveSelectedRowsAndGetTags/fulfilled"

]

const allReducers = {

    applicationSetupStore: applicationSetupStoreReducer,
    applicationStore: applicationStoreReducer,
    batchImportDataStore: batchImportDataStoreReducer,
    businessSegmentsStore: businessSegmentsStoreReducer,
    dataAnalyticsStore: dataAnalyticsStoreReducer,
    examplesStore: examplesStoreReducer,
    findInTracStore: findInTracReducer,
    objectSummaryStore: objectSummaryStoreReducer,
    overlayBuilderStore: overlayBuilderStoreReducer,
    queryBuilderStore: queryBuilderStoreReducer,
    runAFlowStore: runAFlowStoreReducer,
    runAModelStore: runAModelStoreReducer,
    setAttributesStore: setAttributesStoreReducer,
    updateTagsStore: updateTagsStoreReducer,
    uploadADatasetStore: uploadADatasetStoreReducer,
    uploadAFlowStore: uploadAFlowStoreReducer,
    uploadFromGitHubStore: uploadFromGitHubStoreReducer,
    whereClauseBuilderStore: whereClauseBuilderStoreReducer
};

// The next few steps are allowing us to have a single action that can reset the store
// across all slices See https://stackoverflow.com/questions/35622588/how-to-reset-the-state-of-a-redux-store
export const combinedReducer = combineReducers(allReducers);

const rootReducer = (state: any, action: AnyAction) => {

    // These are actions that reset the store, if the tenant changes we need to intercept the action
    // then rewrite the state and then continue with the action
    if (['applicationStore/setTenantSetting', 'applicationStore/setSearchAsOf'].includes(action.type)) {

        // When the tenant changes we need to keep the applicationStore slice intact as that contains information
        // that should persist across the reset
        const {applicationStore} = state
        // Set state as just the applicationStore, the undefined value for the other slices will reset them
        state = {applicationStore}
        // Run the original action on the reset state
        return combinedReducer(state, action)
    }

    return combinedReducer(state, action)
}

export const store = configureStore({

    reducer: rootReducer,
    // The exclusions are because the TRAC API classes are not treated by redux toolkit as serializable items
    // which causes it to print a warning to the log. Rather than turning off the warning we set paths to ignore.
    // This means any genuine warnings will still be printed to the log.
    // See https://redux-toolkit.js.org/usage/usage-guide#working-with-non-serializable-data

    middleware: (getDefaultMiddleware) => getDefaultMiddleware({
        serializableCheck: {

            // See https://redux-toolkit.js.org/usage/usage-guide#working-with-non-serializable-data
            // Ignore these action types
            ignoredActions: ignoredActions,
            // Ignore these field paths in all actions
            ignoredActionPaths: [],
            // Ignore these paths in the state
            ignoredPaths: ignoredPaths
        }
    }),
    //middleware: (getDefaultMiddleware) => getDefaultMiddleware(),
    // Don't enable redux devtools in production
    devTools: process.env.NODE_ENV !== 'production'
})

// See https://redux-toolkit.js.org/tutorials/typescript
// Infer the `RootState` and `AppDispatch` types from the store itself
export type RootState = ReturnType<typeof store.getState>
export type AppDispatch = typeof store.dispatch