import {areSchemasEqual, areSchemasEqualButOneHasExtra, fillSchemaDefaultValues} from "../../../utils/utils_schema";
import {arraysOfPrimitiveValuesASubset, getUniqueObjectIndicesFromArray} from "../../../utils/utils_arrays";
import {convertSearchResultsIntoOptions} from "../../../utils/utils_trac_metadata";
import {createAsyncThunk, createSlice, PayloadAction} from '@reduxjs/toolkit';
import {createTagsFromAttributes} from "../../../utils/utils_attributes_and_parameters";
import {
    DataRow,
    FileImportModalPayload,
    FileSystemInfo,
    GuessedVariableTypes,
    ImportedFileSchema,
    OnCreateNewOptionPayload,
    Option,
    SelectOptionPayload,
    StoreStatus,
    StreamingDataResult,
    StreamingEventFunctions,
    UpdateSchemaPayload
} from "../../../../types/types_general";
import {
    getMatchingSchemasByNumberOfFields,
    importCsvDataFromReference,
    importJsonDataFromStore,
    isObjectLoadedByLocalFileDetails,
    metadataBatchRequest,
    updateTag
} from "../../../utils/utils_trac_api";
import {showToast} from "../../../utils/utils_general";
import type {RootState} from '../../../../storeController';
import {setAttributesAutomatically} from "../../../components/SetAttributes/setAttributesStore";
import {toast} from 'react-toastify';
import {tracdap as trac} from "@finos/tracdap-web-api";
import {isDefined} from "../../../utils/utils_trac_type_chckers";

// Define a type for the slice state
export interface UploadADatasetStoreState {
    import: {
        fileInfo: null | FileSystemInfo
        guessedVariableTypes: undefined | GuessedVariableTypes
        status: StoreStatus
        message: undefined | string,
    }
    file: {
        data: null | DataRow[]
        file: undefined | File
        schema: ImportedFileSchema[]
    }
    existingSchemas: {
        selectedTab: string
        options: Option<string, trac.metadata.ITag>[]
        selectedOption: null | Option<string, trac.metadata.ITag>
    }
    upload: {
        status: StoreStatus,
        message: undefined | string
    }
    alreadyInTrac: {
        foundInTrac: boolean,
        tag: null | undefined | trac.metadata.ITagHeader
        schema?: null | trac.metadata.IFieldSchema[]
        isSuggestedSchemaTheSame?: boolean
        isExistingSchemaTheSame?: boolean
    }
    priorVersion: {
        options: Option<string, trac.metadata.ITag>[]
        selectedOption: null | Option<string, trac.metadata.ITag>
        isPriorVersionSchemaTheSame?: boolean
    }
}

// This is the initial state of the store.
const initialState: UploadADatasetStoreState = {

    import: {
        fileInfo: null,
        guessedVariableTypes: undefined,
        status: "idle",
        message: undefined,
    },
    file: {
        data: null,
        schema: [],
        file: undefined
    },
    existingSchemas: {
        selectedTab: "suggested",
        options: [],
        selectedOption: null
    },
    upload: {
        status: "idle",
        message: undefined,
    },
    alreadyInTrac: {
        foundInTrac: false,
        tag: null,
        schema: undefined,
        isSuggestedSchemaTheSame: undefined,
        isExistingSchemaTheSame: undefined
    },
    priorVersion: {
        options: [],
        selectedOption: null,
        isPriorVersionSchemaTheSame: true
    }
}

/**
 * A reducer that receives the details of a CSV or Excel dataset loaded from the FileImportModal component
 * and stores the results. It also makes API calls to see if the file is already stored in TRAC and if it is
 * the user will be prevented from loading it again.
 */
export const saveFileToStore = createAsyncThunk<// Return type of the payload creator
    { importedFile: FileImportModalPayload, alreadyInTrac: UploadADatasetStoreState["alreadyInTrac"], existingSchemas?: Partial<UploadADatasetStoreState["existingSchemas"]> },
    // First argument to the payload creator
    FileImportModalPayload,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('uploadADatasetStore/saveFileToStore', async (payload, {getState, dispatch}) => {

    // Clear the store back to it's initial state, this will also clear the attributes object and values - those are
    // regenerated using a useEffect call in the SetMetadata component
    dispatch(deleteFile())

    // Extract the file details, so we can search to see if they are already loaded into TRAC
    const {fileInfo, schema, guessedVariableTypes} = payload

    // Get what we need from the store
    const {cookies: {"trac-tenant": tenant}, tracApi: {searchAsOf}} = getState().applicationStore

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")

    // If we find any schema objects in TRAC that match the uploaded file's schema we will use this variable to
    // store options for each schema
    let schemaOptions: Option<string, trac.metadata.ITag>[] = []

    // If we have a guessed schema for the loaded file and a schema (built from the guessed variable types) then we
    // can go and see if we have any schemas that match than can be used instead of the guessed version
    if (guessedVariableTypes && schema) {

        // Find all the schemas that have the same number of fields as the loaded file or less
        const schemaSearchResults = await getMatchingSchemasByNumberOfFields({
            tenant,
            numberOfFields: schema.length,
            searchAsOf,
            canBeFewer: false
        })

        // If we have found any schemas with the same number of fields or fewer as the dataset that we are loading up
        // Then we need to get the schemas and work out which ones match the schema we have guessed for the
        // loaded file. We have to match against the guessed schema as any of the combination of filed types
        // are valid and the user could set any one of them.
        if (schemaSearchResults.length > 0) {

            // A request to get the metadata for an array of objects by their header tag
            // Filter those without the right field names
            const schemaMetadataResults = await metadataBatchRequest({
                tenant,
                tags: schemaSearchResults.map(item => item.header)
            })

            // Now that we have the metadata for each schema we can remove those that have different field names to the
            // loaded file guessed schema, this is a quick filter that would be picked up by the next step but this
            // is a quicker check to do first
            const matchingByFieldNames = schemaMetadataResults.tag.filter(item => item?.definition?.schema?.table?.fields && arraysOfPrimitiveValuesASubset(item?.definition?.schema?.table?.fields.map(variable => variable.fieldName?.toUpperCase()), schema.map(variable => variable.fieldName?.toUpperCase())))

            // Now do the loaded schemas map to the guessed types!!
            const matchingGuessedSchema = matchingByFieldNames.filter(item => {

                // If the search result item has a schema
                if (item?.definition?.schema?.table?.fields) {

                    // Does every field have a type that is at least one option in the guessed types for that field
                    return item.definition.schema.table.fields.every(variable => {

                        // If there are case differences between the schema object definition and the loaded dataset we need a way to relate the two by key
                        const matchingGuessedVariableTypesKey = Object.keys(guessedVariableTypes).find(guessedVariableType => guessedVariableType.toUpperCase() === variable.fieldName?.toUpperCase())

                        return !!(variable.fieldName && variable.fieldType && matchingGuessedVariableTypesKey != undefined && guessedVariableTypes[matchingGuessedVariableTypesKey].types.recommended.includes(variable.fieldType));
                    })

                } else {

                    return false
                }
            })

            // Find the indices of the unique schemas - we could have multiple matching results
            const uniqueSchemaIndices = getUniqueObjectIndicesFromArray(matchingGuessedSchema.map(item => item?.definition?.schema?.table?.fields))

            // TODO organise these into "same number of fields" and "fewer fields" and add some text to the UI
            // Convert the unique schemas into a set of options for the user to pick which one they want to use
            schemaOptions = convertSearchResultsIntoOptions(matchingGuessedSchema.filter((item, i) => uniqueSchemaIndices.includes(i)), false, false, true, false)

            // Set the tab which shows the available schemas as the default if there are options
            if (schemaOptions.length > 0) {
                dispatch(setTab("existing"))
            }
        }
    }

    // See if the file is already if TRAC, we check the same file name, size and modified date .
    // If it is there then we will not allow the dataset to be loaded again
    const dataSearchResults = await isObjectLoadedByLocalFileDetails({
        fileDetails: fileInfo,
        objectType: trac.ObjectType.DATA,
        searchAsOf,
        tenant
    })

    // If we find a matching dataset then this will store its metadata
    let resultWithMatchingSchema: undefined | trac.metadata.ITag

    // If we find any datasets with the same filename, size and modified date
    if (dataSearchResults.length > 0) {

        // A request to get the metadata for the data by its header tag
        const dataMetadataResults = await metadataBatchRequest({
            tenant,
            tags: dataSearchResults.map(item => item.header)
        })

        if (schema && dataMetadataResults.tag.length > 0) {

            resultWithMatchingSchema = dataMetadataResults.tag.find(item => item.definition?.data?.schema?.table?.fields && areSchemasEqual(schema, item.definition?.data?.schema?.table?.fields, true))
        }
    }

    // Add in the hidden attribute values, these are valid attributes, but they are set to be hidden, instead
    // the application adds in the data. Attributes are managed in a different component with its own store
    // in order to keep the application DRY (don't repeat yourself)
    dispatch(setAttributesAutomatically({
        storeKey: "uploadADataset",
        values: {
            original_file_name: payload.fileInfo.fileName,
            original_file_size: payload.fileInfo.sizeInBytes,
            original_file_modified_date: payload.fileInfo.lastModifiedTracDate,
            // An array value signifies that the attribute needs to be converted into a set of options
            import_method: ["user_interface"]
        }
    }))

    // Since we now know that the data is loaded we block the same dataset being loaded again
    return {
        importedFile: payload,
        alreadyInTrac: {
            foundInTrac: Boolean(Array.isArray(dataSearchResults) && dataSearchResults.length > 0),
            tag: dataSearchResults.length > 0 ? dataSearchResults[0].header : undefined,
            schema: resultWithMatchingSchema?.definition?.data?.schema?.table?.fields,
            isSuggestedSchemaTheSame: Boolean(resultWithMatchingSchema !== undefined),
            isExistingSchemaTheSame: false
        },
        existingSchemas: {options: schemaOptions, selectedOption: null}
    }
})

/**
 * A function that makes an API call to TRAC to save the dataset to TRAC.
 */
export const uploadDataToTrac = createAsyncThunk<// Return type of the payload creator
    { alreadyInTrac: UploadADatasetStoreState["alreadyInTrac"] },
    // First argument to the payload creator
    void | StreamingEventFunctions,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        serializedErrorType: Error | ({ message: string, row: number })[],
        state: RootState
    }>('uploadADatasetStore/uploadDataToTrac', async (eventFunctions, {getState}) => {

    // Get what we need from the store
    const {cookies: {"trac-tenant": tenant}, clientConfig: {uploading: {data: {processing: {messageSize}}}}} = getState().applicationStore
    const {processedAttributes, values} = getState().setAttributesStore.uses.uploadADataset.attributes

    // File here is the reference to the file selected by a form input, we can load the file from it
    const {
        existingSchemas: {selectedOption, selectedTab},
        file: {data, schema, file},
        import: {fileInfo},
        priorVersion: {selectedOption: selectedPriorVersion}
    } = getState().uploadADatasetStore

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")

    // Create the set of attribute tags for the dataset
    const attrs = createTagsFromAttributes(processedAttributes, values)

    let result: StreamingDataResult

    // Either the user must have the suggested tab selected or the existing one AND an option selected.
    if ((selectedTab === "existing" && selectedOption) || selectedTab === "suggested") {

        // Loading the data from the reference to the selected file
        // We have to do this before we load from JSON as the data
        // will always be populated as a sample of the file in the
        // reference is taken.
        if (fileInfo?.fileExtension === "csv" && file !== undefined) {

            // The response is the metadata header tag for the object, we handle only handle update here
            // TODO What happens when you try and load into a priorVersion
            result = await importCsvDataFromReference({
                attrs,
                file,
                numberOfRows: fileInfo?.numberOfRows,
                priorVersion: selectedPriorVersion?.tag?.header || undefined,
                schemaFields: selectedTab === "existing" && selectedOption?.tag.definition?.schema?.table?.fields ? selectedOption?.tag.definition?.schema?.table?.fields : schema,
                schemaHeader: selectedOption?.tag.header ?? undefined,
                tenant
            }, eventFunctions, messageSize)

        } else if (fileInfo?.fileExtension === "xlsx" && data != null) {

            result = await importJsonDataFromStore({
                attrs,
                data,
                // TODO fix the below so that string can be passed - this comes when parsing fewer rows to guess schema than in data
                numberOfRows: typeof fileInfo?.numberOfRows !== "string" ? fileInfo?.numberOfRows : undefined,
                priorVersion: selectedPriorVersion?.tag?.header || undefined,
                schemaFields: selectedTab === "existing" && selectedOption?.tag.definition?.schema?.table?.fields ? selectedOption?.tag.definition?.schema?.table?.fields : schema,
                schemaHeader: selectedOption?.tag.header ?? undefined,
                tenant
            }, eventFunctions, messageSize)

        } else {

            throw new Error("The selected dataset is null or there is no reference to load it from, this is not allowed")
        }

    } else {
        throw new Error("You have selected to use an existing schema but have not selected a schema to use")
    }

    // Now that the dataset has been uploaded we know some more information
    // that we did not know at the start of the load, we know how long the load took and
    // also the number of rows. These are not critical, but it is useful to have them, for
    // example we can build a dashboard on load times. So we make an API call to update the
    // tag of the newly loaded dataset.
    const tagUpdates: trac.metadata.ITagUpdate[] = [
        {
            "attrName": "number_of_rows",
            "value": {
                "integerValue": result.numberOfRows,
                "type": {
                    "basicType": trac.INTEGER
                }
            }
        },
        {
            "attrName": "duration_of_load",
            "value": {
                "integerValue": result.duration,
                "type": {
                    "basicType": trac.INTEGER
                }
            }
        }]

    // Add a tag update to put the number of rows and duration into the tags, we can only know this after the load completes
    const updatedTag = await updateTag({
        priorVersion: result.tag,
        tagUpdates,
        tenant
    })

    // Since we now know that the data is loaded we block the same dataset being loaded again
    return {
        alreadyInTrac: {
            foundInTrac: true,
            tag: updatedTag,
            schema: selectedPriorVersion?.tag?.definition?.data?.schema?.table?.fields || schema,
            areSchemasTheSame: true
        }
    }
})

export const uploadADatasetStore = createSlice({
    name: 'uploadA' +
        '' +
        'DatasetStore',
    initialState,
    reducers: {
        /**
         * A reducer that resets the store to its original state, this runs when the user is partway through loading a
         * dataset and decides to start again.
         */
        deleteFile: () => initialState,
        /**
         * A reducer that is passed to the SchemaEditor component which allows the user edited schema
         * to be saved in the store.
         */
        updateSchema: (state, action: PayloadAction<UpdateSchemaPayload>) => {

            const {index, fieldSchema: newFieldSchema, fieldOrder} = action.payload

            // Update the other field schema affected, if the change was a change in field order
            if (fieldOrder) {

                const indexToSwapWith = state.file.schema.findIndex(fieldSchema => fieldSchema.fieldOrder === newFieldSchema.fieldOrder)
                if (indexToSwapWith > -1) state.file.schema[indexToSwapWith]["fieldOrder"] = fieldOrder.oldFieldOrder
            }

            // Put the new field schema definition into the array
            state.file.schema[index] = newFieldSchema

            // See if the new schema matches the schema of the dataset loaded into TRAC if one was found
            if (state.alreadyInTrac.schema) {
                state.alreadyInTrac.isSuggestedSchemaTheSame = areSchemasEqual(state.alreadyInTrac.schema, state.file.schema)
            }

            // Check if the newly edited schema is still OK to update the prior version dataset selected
            uploadADatasetStore.caseReducers.areSchemasTheSame(state)
        },
        /**
         * A reducer that stores the user selected tab from the ChooseSchema component. This is needed to be able to
         * know whether to use the guessed or existing schema when uploading the data.
         */
        setTab: (state, action: PayloadAction<string>) => {

            if (!(action.payload === "existing" || action.payload === "suggested")) return

            state.existingSchemas.selectedTab = action.payload

            // If the user changes tab we need to reassess if the schema relating to which ever tab they have selected
            // is ok to update the existing dataset with
            uploadADatasetStore.caseReducers.areSchemasTheSame(state)
        },
        /**
         * A reducer that takes the user selected schema from the list of existing schemas in TRAC and stores the
         * option in the store.
         */
        setSelectedSchemaOption: (state, action: PayloadAction<SelectOptionPayload<Option<string, trac.metadata.ITag>, false>>) => {

            state.existingSchemas.selectedOption = action.payload.value

            const {schema: guessedSchema} = state.file

            // Check if the dataset found in TRAC has the same schema as the selected schema object, if it does then we
            // do not allow the dataset to be loaded
            if (action.payload.value?.tag?.definition?.schema?.table?.fields && state.alreadyInTrac.schema) {
                state.alreadyInTrac.isExistingSchemaTheSame = areSchemasEqual(action.payload.value.tag.definition.schema.table.fields, state.alreadyInTrac.schema)
            }

            // If the dataset has a different schema to the one selected to apply but only by the capitalisation of the field names
            // then change the schema object definition to map to the new names. Otherwise, when we load the data we get undefined
            // values as the keys don't work.
            if (state.existingSchemas.selectedOption?.tag?.definition?.schema?.table?.fields) {

                let fields = state.existingSchemas.selectedOption.tag.definition.schema.table.fields

                if (fields) {

                    // If the user has selected to use an existing schema for a dataset then since TRAC is case-insensitive the chosen
                    // schema field names could not match the case of the loaded dataset. If we don't do the next step then the table
                    // will show empty rows as it can't match the schema to the data. To make it match we add a property to the schema
                    // using the guessed schema (which will match the column names) to provide the field name case.
                    const fieldNamesInData = guessedSchema.map(guessedField => guessedField.fieldName).filter(isDefined)

                    state.existingSchemas.selectedOption.tag.definition.schema.table.fields = fields.map(field => {

                        let newField: ImportedFileSchema = {...field}

                        if (field.fieldName) {
                            newField.jsonName = fieldNamesInData.find(fieldName => fieldName.toUpperCase() === field.fieldName?.toUpperCase())
                        }

                        // If we found that the data was in a specified format when we parsed it and guessed the schema then copy that info across
                        // for example if a date field was found to be in "dd/mm/yyyy" format then we need that info to save it as ISO format on the server
                        if (guessedSchema) {
                            const positionOfFieldSchema = guessedSchema.findIndex(fieldSchema => fieldSchema.fieldName != undefined && fieldSchema.fieldName.toUpperCase() === field.fieldName?.toUpperCase())
                            if (positionOfFieldSchema > -1) {
                                newField.inFormat = guessedSchema[positionOfFieldSchema].inFormat
                            }
                        }

                        // Add in default values as the API does not return them
                        return fillSchemaDefaultValues(newField)
                    })
                }
            }

            // Check if the newly selected schema is still OK to update the prior version dataset selected
            uploadADatasetStore.caseReducers.areSchemasTheSame(state)
        },
        /**
         * A reducer that adds an option for the datasets that can be used as prior versions to replace with the new
         * file. This is passed to the SelectOption component which actually gets the TRAC object requested by the user
         * and then passes this as a new option to this function.
         */
        addNewPriorVersionOption: (state, action: PayloadAction<OnCreateNewOptionPayload<trac.metadata.ITag>>) => {

            const {newOptions} = action.payload

            // The check on tag is just a Typescript check as the onCreateOption prop to the SelectOption contains
            // two types of payloads
            if (newOptions.length > 0) {

                // Add the option to the list
                state.priorVersion.options.push(action.payload.newOptions[0])

                // To be able to call the setPriorVersionOption reducer from here we have to repackage the
                // payload from this function to a payload that the setPriorVersionOption so that Typescript
                // does not complain
                uploadADatasetStore.caseReducers.setPriorVersionOption(state, {
                    payload: {
                        basicType: action.payload.basicType,
                        isValid: true,
                        value: action.payload.newOptions[0]
                    }, type: "setPriorVersionOption"
                })

                showToast("success", "New dataset option successfully added", "addNewPriorVersionOption/fulfilled")
            }
        },
        /**
         * A reducer that checks whether the loaded file schema, either from the guessed one or one set by a previously
         * loaded schema object, matches the schema of the dataset that the user has selected to try and add the file
         * as a new version. This runs in multiple places, when ever any of the three schemas change.
         */
        areSchemasTheSame: (state) => {

            if (state.priorVersion.selectedOption === null) {
                // When priorVersion.selectedOption is null there is no attempt being made to update a dataset to a
                // new version
                state.priorVersion.isPriorVersionSchemaTheSame = true

            } else if (state.priorVersion.selectedOption?.tag?.definition?.data?.schema?.table?.fields) {

                // This is the 'new schema' we want to set for the new version of the dataset, either from the
                // existing schema object or the one in guessed and edited by the user
                const schemaToCompareTo = state.existingSchemas.selectedTab === "existing" ? state.existingSchemas.selectedOption?.tag?.definition?.schema?.table?.fields : state.file.schema

                if (schemaToCompareTo) {

                    //action.payload.value.tag.definition.data.schema.table.fields is the schema of the dataset that we
                    // want to use as the prior version - the 'current schema'
                    state.priorVersion.isPriorVersionSchemaTheSame = areSchemasEqualButOneHasExtra(state.priorVersion.selectedOption?.tag.definition.data.schema.table.fields, schemaToCompareTo)

                } else {
                    state.priorVersion.isPriorVersionSchemaTheSame = true
                }
            }
        },
        /**
         * A reducer that sets the selected dataset to use as the prior version to save on top of.
         */
        setPriorVersionOption: (state, action: PayloadAction<SelectOptionPayload<Option<string, trac.metadata.ITag>, false>>) => {

            state.priorVersion.selectedOption = action.payload.value
            // Check if the dataset found in TRAC has the same schema as the selected dataset to overlay, if it does then we
            // do not allow the dataset to be loaded
            uploadADatasetStore.caseReducers.areSchemasTheSame(state)
        }
    },
    extraReducers: (builder) => {

        // A set of lifecycle reducers to run before/after the saveFileToStore function
        builder.addCase(saveFileToStore.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.import.status = "pending"
            state.import.message = undefined
        })
        builder.addCase(saveFileToStore.fulfilled, (state, action: PayloadAction<{ importedFile: FileImportModalPayload, alreadyInTrac: UploadADatasetStoreState["alreadyInTrac"], existingSchemas?: Partial<UploadADatasetStoreState["existingSchemas"]> }>) => {

            const {alreadyInTrac, importedFile, existingSchemas} = action.payload

            // This is a rendering optimisation as we can just use this as meaning do we have the things
            // we need or not. data can be various types from the FileIMportModal, but we should be getting an array for this use
            if (Array.isArray(importedFile.schema) && (Array.isArray(importedFile.data) || importedFile.file !== undefined) && importedFile.guessedVariableTypes && importedFile.fileInfo) {

                state.import.status = "succeeded"
                state.import.message = undefined
                state.import.fileInfo = importedFile.fileInfo
                state.import.guessedVariableTypes = importedFile.guessedVariableTypes
                state.file.data = Array.isArray(importedFile.data) ? importedFile.data : null
                state.file.schema = importedFile.schema
                state.file.file = importedFile.file

                state.existingSchemas.options = existingSchemas?.options || []
                state.existingSchemas.selectedOption = existingSchemas?.selectedOption || null
            }
            // Update the information about whether the dataset is loaded in TRAC
            state.alreadyInTrac = alreadyInTrac
        })
        builder.addCase(saveFileToStore.rejected, (state, action) => {

            state.import.status = "failed"

            const text = {
                title: "Failed to import the dataset",
                message: "The import of the dataset was not completed successfully.",
                details: action.error.message
            }

            showToast("error", text, "saveFileToStore/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the uploadDataToTrac function
        builder.addCase(uploadDataToTrac.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.upload.status = "pending"
            state.upload.message = undefined
        })
        builder.addCase(uploadDataToTrac.fulfilled, (state, action: PayloadAction<{ alreadyInTrac: UploadADatasetStoreState["alreadyInTrac"] }>) => {

            const {alreadyInTrac} = action.payload

            state.upload.status = "succeeded"
            state.upload.message = undefined

            // Update the information about whether the dataset is loaded in TRAC
            state.alreadyInTrac = alreadyInTrac

            showToast("success", `The dataset was loaded into TRAC was successfully with object ID ${alreadyInTrac.tag?.objectId}.`, "uploadDataToTrac/fulfilled")
        })
        builder.addCase(uploadDataToTrac.rejected, (state, action) => {

            state.upload.status = "failed"

            const text = {
                title: "Failed to load the dataset",
                message: "The loading of the dataset into TRAC was not completed successfully.",
                details: Array.isArray(action.error) ? action.error.map(error => error.message + "(" + error.row + ")") : action.error.message
            }

            showToast("error", text, "uploadDataToTrac/rejected")
            console.error(action.error)
        })
    }
})

// Action creators are generated for each case reducer function
export const {
    addNewPriorVersionOption,
    deleteFile,
    setPriorVersionOption,
    setSelectedSchemaOption,
    setTab,
    updateSchema
} = uploadADatasetStore.actions

export default uploadADatasetStore.reducer