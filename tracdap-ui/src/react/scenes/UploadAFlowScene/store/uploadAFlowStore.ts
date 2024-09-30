import {createAsyncThunk, createSlice} from '@reduxjs/toolkit';
import {createTagsFromAttributes} from "../../../utils/utils_attributes_and_parameters";
import type {FileImportModalPayload, FileSystemInfo, Option, StoreStatus} from "../../../../types/types_general";
import {importFlow, isObjectLoadedByLocalFileDetails,} from "../../../utils/utils_trac_api";
import type {PayloadAction} from '@reduxjs/toolkit';
import type {RootState} from "../../../../storeController";
import {resetAttributesToDefaults, setAttributesAutomatically} from "../../../components/SetAttributes/setAttributesStore";
import {showToast} from "../../../utils/utils_general";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {toast} from "react-toastify";

type UploadAFlowStoreState = {
    import: {
        fileInfo: null | FileSystemInfo
        status: StoreStatus
        message: undefined | string,
    },
    file: {
        data: null | string,
    },
    upload: {
        status: StoreStatus,
        message: undefined | string
    }
    alreadyInTrac: {
        foundInTrac: boolean,
        tag: null | undefined | trac.metadata.ITagHeader
    }
    priorVersion: {
        options: Option<string, trac.metadata.ITag>[]
        selectedOption: null | Option<string, trac.metadata.ITag>
    }
}

// This is the initial state of the store.
const initialState: UploadAFlowStoreState = {
    import: {
        fileInfo: null,
        status: "idle",
        message: undefined,
    },
    file: {
        data: null
    },
    upload: {
        status: "idle",
        message: undefined,
    },
    alreadyInTrac: {
        foundInTrac: false,
        tag: null
    },
    priorVersion: {
        options: [],
        selectedOption: null
    }
}

/**
 * A reducer that receives the details of a json flow loaded from the FileImportModal component
 * and stores the results. It also makes API calls to see if the file is already stored in TRAC and if it is
 * the user will be prevented from loading it again.
 */
export const saveFileToStore = createAsyncThunk<// Return type of the payload creator
    { importedFile: FileImportModalPayload, alreadyInTrac: UploadAFlowStoreState["alreadyInTrac"] },
    // First argument to the payload creator
    FileImportModalPayload,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('uploadAFlowStore/saveFileToStore', async (payload, {getState, dispatch}) => {

    // Clear the store back to it's initial state, this will also clear the attributes object and values - those are
    // regenerated using a useEffect call in the SetMetadata component
    dispatch(deleteFile())

    // Extract the file details, so we can search to see if they are already loaded into TRAC
    const {fileInfo} = payload

    // Get what we need from the store
    const {cookies: {"trac-tenant": tenant}, tracApi: {searchAsOf}} = getState().applicationStore

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")

    // See if the file is already if TRAC, we check the same file name, size and modified date .
    // If it is there then we will not allow the dataset to be loaded again
    const flowSearchResults = await isObjectLoadedByLocalFileDetails({
        fileDetails: fileInfo,
        objectType: trac.ObjectType.FLOW,
        searchAsOf,
        tenant
    })

    // Since we are selecting a new flow file we need to reset the metadata attributes in the UI, this information
    // is owned by the setAttributesStore
    dispatch(resetAttributesToDefaults({storeKey: "uploadAFlow"}))

    // Set the hidden information about the model
    dispatch(setAttributesAutomatically({storeKey: "uploadAFlow",
        values: {
            import_method: ["user_interface"],
            original_file_name: payload.fileInfo.fileName,
            original_file_size: payload.fileInfo.sizeInBytes,
            original_file_modified_date: payload.fileInfo.lastModifiedTracDate
        }
    }))

    // Since we now know that the data is loaded we block the same dataset being loaded again
    return {
        importedFile: payload,
        alreadyInTrac: {
            foundInTrac: Boolean(Array.isArray(flowSearchResults) && flowSearchResults.length > 0),
            tag: flowSearchResults.length > 0 ? flowSearchResults[0].header : undefined
        },
    }
})

/**
 * A function that makes an API call to TRAC to save the flow to TRAC.
 */
export const uploadFlowToTrac = createAsyncThunk<// Return type of the payload creator
    { alreadyInTrac: UploadAFlowStoreState["alreadyInTrac"] },
    // First argument to the payload creator
    void,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('uploadAFlowStore/uploadFlowToTrac', async (_, {getState}) => {

    // Get what we need from the store
    const {cookies: {"trac-tenant": tenant}} = getState().applicationStore
    const {processedAttributes, values} = getState().setAttributesStore.uses.uploadAFlow.attributes

    // data is a string version of the json, but we checked when it was loaded that it can be
    // converted to an object without error
    const {data} = getState().uploadAFlowStore.file
    const {selectedOption: selectedPriorVersion} = getState().uploadAFlowStore.priorVersion

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")

    if (data == null) throw new Error("The selected flow is null, this is not allowed")

    // Create the set of attribute tags for the dataset
    const attrs = createTagsFromAttributes(processedAttributes, values)

    // Upload the data, the response is the metadata header tag for the object, we handle both create and update here
    const tagHeader = await importFlow({
        attrs,
        // The fromObject function converts the flow object from the JSON to a flow definition, with all the right
        // properties etc. It also converts the string versions of the nodeTypes (e.g. "INPUT_NODE") to the TRAC enum
        // equivalent (e.g. 1). It does the same thing for the parameter basicType variables too ("STRING" => 4)
        flow: trac.metadata.FlowDefinition.fromObject(JSON.parse(data)),
        priorVersion: selectedPriorVersion?.tag?.header || undefined,
        tenant
    })

    // Since we now know that the data is loaded we block the same dataset being loaded again
    return {
        alreadyInTrac: {
            foundInTrac: true,
            tag: tagHeader,
            areSchemasTheSame: true
        }
    }
})

export const uploadAFlowStore = createSlice({
    name: 'uploadAFlowStore',
    initialState: initialState,
    reducers: {
        /**
         * A reducer that resets the store to its original state, this runs when the user is partway through loading a
         * dataset and decides to start again.
         */
        deleteFile: () => initialState
    },
    extraReducers: (builder) => {
        // A set of lifecycle reducers to run before/after the saveFileToStore function
        builder.addCase(saveFileToStore.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.import.status = "pending"
            state.import.message = undefined
        })
        builder.addCase(saveFileToStore.fulfilled, (state, action: PayloadAction<{ importedFile: FileImportModalPayload, alreadyInTrac: UploadAFlowStoreState["alreadyInTrac"] }>) => {

            const {alreadyInTrac, importedFile} = action.payload

            // This is a rendering optimisation as we can just use this as meaning do we have the things
            // we need or not. data can be various types from the FileIMportModal, but we should be getting a
            // string version of the json for this use
            if (typeof importedFile.data === "string" && importedFile.fileInfo) {

                state.import.status = "succeeded"
                state.import.message = undefined
                state.import.fileInfo = importedFile.fileInfo
                state.file.data = importedFile.data
            }
            // Update the information about whether the dataset is loaded in TRAC
            state.alreadyInTrac = alreadyInTrac
        })
        builder.addCase(saveFileToStore.rejected, (state, action) => {

            state.import.status = "failed"

            const text = {
                title: "Failed to import the flow",
                message: "The import of the flow was not completed successfully.",
                details: action.error.message
            }

            showToast("error", text, "saveFileToStore/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the uploadDataToTrac function
        builder.addCase(uploadFlowToTrac.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.upload.status = "pending"
            state.upload.message = undefined
        })
        builder.addCase(uploadFlowToTrac.fulfilled, (state, action: PayloadAction<{ alreadyInTrac: UploadAFlowStoreState["alreadyInTrac"] }>) => {

            const {alreadyInTrac} = action.payload

            state.upload.status = "succeeded"
            state.upload.message = undefined

            // Update the information about whether the dataset is loaded in TRAC
            state.alreadyInTrac = alreadyInTrac

            showToast("success", `The flow was loaded into TRAC was successfully with object ID ${alreadyInTrac.tag?.objectId}.`, "uploadFlowToTrac/fulfilled")
        })
        builder.addCase(uploadFlowToTrac.rejected, (state, action) => {

            state.upload.status = "failed"

            const text = {
                title: "Failed to load the flow",
                message: "The loading of the flow into TRAC was not completed successfully.",
                details: action.error.message
            }

            showToast("error", text, "uploadFlowToTrac/rejected")
            console.error(action.error)
        })
    }
})

// Action creators are generated for each case reducer function
export const {
    deleteFile
} = uploadAFlowStore.actions

export default uploadAFlowStore.reducer