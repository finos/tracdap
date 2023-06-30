/**
 * This slice acts as the store for datasets created and edited by the {@link ApplicationSetupScene}.
 *
 * @category Redux store
 * @module applicationSetupStore
 */

import {arraysOfObjectsEqual, sortArrayBy} from "../../../utils/utils_arrays";
import cloneDeep from "lodash.clonedeep";
import {createAsyncThunk, createSlice, PayloadAction} from '@reduxjs/toolkit';
import {
    ButtonPayload,
    ConfirmButtonPayload,
    CreateDetails,
    GetDatasetByTagResult,
    OnCreateNewOptionPayload,
    Option,
    SelectValuePayload,
    StoreStatus,
    UiBatchImportDataRow,
    UiBusinessSegmentsDataRow,
    UiEditableDatasetKeys,
    UiEditableRow
} from "../../../../types/types_general";
import {createEmptyRow} from "../../../utils/utils_schema";
import {defaultUiAttributesList, uiAttributesListAttrs, uiAttributesListSchema} from "./defaultUiAttributesList";
import {defaultUiBatchImportData, uiBatchImportDataAttrs, uiBatchImportDataSchema} from "./defaultUiBatchImportData";
import {defaultUiBusinessSegmentOptions, uiBusinessSegmentOptionsAttrs, uiBusinessSegmentOptionsSchema} from "./defaultUiBusinessSegmentOptions";
import {defaultUiParametersList, uiParametersListAttrs, uiParametersListSchema} from "./defaultUiParametersList";
import {getSmallDatasetByTag, importDataFromJson, multipleSearchesByMultipleClauses, readSmallDataset} from "../../../utils/utils_trac_api";
import {isDefined, isKeyOf} from "../../../utils/utils_trac_type_chckers";
import {processAttributesDataForAllUses, setAllProcessedAttributes} from "../../../components/SetAttributes/setAttributesStore";
import {processAttributes} from "../../../utils/utils_attributes_and_parameters";
import {RootState} from '../../../../storeController';
import {showToast} from "../../../utils/utils_general";
import {toast} from "react-toastify";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {UiAttributesListRow, UiParametersListRow} from "../../../../types/types_attributes_and_parameters";

/**
 * An interface of common properties of the default definitions of the UI datasets, we use this interface to stop
 * having to repeat ourselves in the store interface.
 */
interface DefaultDatasetItem {
    attrs: trac.metadata.ITagUpdate[]
    fields: trac.metadata.IFieldSchema[]
}

/**
 * An interface of common properties of the current definitions of the UI datasets, we use this interface to stop
 * having to repeat ourselves in the store interface.
 */
interface CurrentDatasetItem {
    tag: trac.metadata.ITag,
    fields: trac.metadata.IFieldSchema[]
    foundInTrac: boolean
}

/**
 * An interface of common properties of the editor definitions of the UI datasets, we use this interface to stop
 * having to repeat ourselves in the store interface.
 */
interface EditorDatasetItem {

    /**
     * The TRAC schema for the dataset.
     */
    fields: trac.metadata.IFieldSchema[]
    /**
     * The index of the row being edited.
     */
    index: null | number
    /**
     * Whether the edited data has been validated and what rows have been checked. The number key
     * is the row number in the dataset and the string key is the property in the row.
     */
    validation: { isValid: Record<number, Record<string, boolean>>, validationChecked: boolean }
}

/**
 * An interface for the applicationSetupStore.ts Redux store.
 */
export interface applicationSetupStoreState {
    /**
     * The editor is where copies of the data downloaded from TRAC are stored if the user selects to edit them. We copy
     * the data so that the changes can be discarded.
     */
    editor: {
        control: {
            /**
             * Whether to show the editor in the user interface.
             */
            show: boolean,
            /**
             * The key of the dataset being edited.
             */
            key: null | UiEditableDatasetKeys
            /**
             * Has the user made a change to a dataset loaded into the editor.
             */
            userChangedSomething: boolean
        }
        items: {
            /**
             * Details about the batch import data loaded into the editor.
             */
            ui_attributes_list: EditorDatasetItem & {
                data: | UiAttributesListRow[]
                row: UiAttributesListRow | null
                /**
                 * For attributes the user can manually add a category, this stores the manually added values.
                 */
                userAddedCategoryOptions: Option<string>[]
            },
            /**
             * Details about the batch import data loaded into the editor.
             */
            ui_batch_import_data: EditorDatasetItem & {
                /**
                 * A copy of the downloaded data that can be edited.
                 */
                data: UiBatchImportDataRow[]
                /**
                 * The row of data being edited.
                 */
                row: UiBatchImportDataRow | null
            },
            ui_business_segment_options: EditorDatasetItem & {
                data: UiBusinessSegmentsDataRow[]
                row: UiBusinessSegmentsDataRow | null
                /**
                 * Which business segment is being edited, only one variable can be edited at a time.
                 */
                variable: "GROUP_01" | "GROUP_02" | "GROUP_03" | "GROUP_04"
            },
            ui_parameters_list: EditorDatasetItem & {
                data: | UiAttributesListRow[]
                row: UiAttributesListRow | null
                /**
                 * For attributes the user can manually add a category, this stores the manually added values.
                 */
                userAddedCategoryOptions: Option<string>[]
            }
        }
    },
    /**
     * The status of various API calls to TRAC and the information returned from those calls.
     */
    tracItems: {
        /**
         * The initial call to search and download any of the user interface owned datasets already stored in TRAC.
         */
        getSetupItems: { status: StoreStatus },
        /**
         * The call to a new user interface owned dataset in TRAC.
         */
        createSetupItem: { status: StoreStatus },
        /**
         * The call to download a new user interface owned dataset that has just been created.
         */
        getSetupItem: { status: StoreStatus },
        /**
         * The call to update a user interface owned dataset that has been edited.
         */
        updateSetupItem: { status: StoreStatus },
        /**
         * The store of information for the user interface owned datasets. The default definition is for when the dataset
         * does not exist in TRAC then this defines what will be created. The current definition is fir when the dataset
         * already exists in TRAC then this is where what is downloaded information is stored.
         */
        items: {
            ui_attributes_list: {
                defaultDefinition: DefaultDatasetItem & { data: UiAttributesListRow[] }
                currentDefinition: CurrentDatasetItem & { data: UiAttributesListRow[] }
            },
            ui_batch_import_data: {
                defaultDefinition: DefaultDatasetItem & { data: UiBatchImportDataRow[] }
                currentDefinition: CurrentDatasetItem & { data: UiBatchImportDataRow[] }
            },
            ui_business_segment_options: {
                defaultDefinition: DefaultDatasetItem & { data: UiBusinessSegmentsDataRow[] }
                currentDefinition: CurrentDatasetItem & { data: UiBusinessSegmentsDataRow[] }
            },
            ui_parameters_list: {
                defaultDefinition: DefaultDatasetItem & { data: UiParametersListRow[] }
                currentDefinition: CurrentDatasetItem & { data: UiParametersListRow[] }
            },
        }
    }
}

// This is the initial state of the store.
const initialState: applicationSetupStoreState = {
    editor: {
        control: {
            show: false,
            key: null,
            userChangedSomething: false
        },
        items: {
            ui_attributes_list: {
                data: [],
                fields: [],
                validation: {isValid: {}, validationChecked: false},
                row: null,
                index: null,
                userAddedCategoryOptions: []
            },
            ui_batch_import_data: {
                data: [],
                fields: [],
                validation: {isValid: {}, validationChecked: false},
                row: null,
                index: null,
            },
            ui_business_segment_options: {
                data: [],
                fields: [],
                validation: {isValid: {}, validationChecked: false},
                row: null,
                index: null,
                variable: "GROUP_01"
            },
            ui_parameters_list: {
                data: [],
                fields: [],
                validation: {isValid: {}, validationChecked: false},
                row: null,
                index: null,
                userAddedCategoryOptions: []
            }
        }
    },
    tracItems: {
        getSetupItems: {status: "idle"},
        createSetupItem: {status: "idle"},
        getSetupItem: {status: "idle"},
        updateSetupItem: {status: "idle"},
        // Note that we import the default definitions from other files rather than hard code them here and
        // make this code very long
        items: {
            ui_attributes_list: {
                defaultDefinition: {
                    attrs: uiAttributesListAttrs,
                    fields: uiAttributesListSchema,
                    data: defaultUiAttributesList
                },
                currentDefinition: {
                    tag: {},
                    data: [],
                    fields: [],
                    foundInTrac: false
                }
            },
            ui_batch_import_data: {
                defaultDefinition: {
                    attrs: uiBatchImportDataAttrs,
                    fields: uiBatchImportDataSchema,
                    data: defaultUiBatchImportData
                },
                currentDefinition: {
                    tag: {},
                    data: [],
                    fields: [],
                    foundInTrac: false
                }
            },
            ui_business_segment_options: {
                defaultDefinition: {
                    attrs: uiBusinessSegmentOptionsAttrs,
                    fields: uiBusinessSegmentOptionsSchema,
                    data: defaultUiBusinessSegmentOptions
                },
                currentDefinition: {
                    tag: {},
                    data: [],
                    fields: [],
                    foundInTrac: false
                }
            },
            ui_parameters_list: {
                defaultDefinition: {
                    attrs: uiParametersListAttrs,
                    fields: uiParametersListSchema,
                    data: defaultUiParametersList
                },
                currentDefinition: {
                    tag: {},
                    data: [],
                    fields: [],
                    foundInTrac: false
                }
            }
        }
    }
    // processedAttributes: {},
    // processedParameters: {}
}

/**
 * A function that searches TRAC for the items required by the user interface to function.
 * If these are found then the items themselves are downloaded. This function is run when
 * the application loads in the browser and is called by the {@link OnLoad} component.
 */
export const getSetupItems = createAsyncThunk<// Return type of the payload creator
    { key?: string, data: UiEditableRow[], schema: trac.metadata.IFieldSchema[], tag?: trac.metadata.ITag }[] | undefined,
    // First argument to the payload creator
    void,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('applicationSetupStore/getSetupItems', async (_, {dispatch, getState}) => {

    // Get the parameters we need from the store
    const {searchAsOf} = getState().applicationStore.tracApi
    const {"trac-tenant": tenant} = getState().applicationStore.cookies

    // Get the details of the items we need to search for, each should have a schema and a dataset search, then we need
    // to get the metadata and the data itself if an entry exists.
    const {items} = getState().applicationSetupStore.tracItems

    // If there is no tenant (during set up there may not be) then exit
    if (tenant === undefined) return

    const termArray: trac.metadata.ISearchExpression[][] = Object.keys(items).map(key => [{
        term: {
            attrName: "key",
            attrType: trac.STRING,
            operator: trac.SearchOperator.EQ,
            searchValue: {stringValue: key}
        }
    }])

    // Complete all the searches, note if one of these returns a 404 the below code is not executed but the
    // promise is rejected
    const searchResponses = await multipleSearchesByMultipleClauses({
        includeOnlyShowInSearchResultsIsTrue: false,
        keys: Object.keys(items),
        objectType: trac.ObjectType.DATA,
        searchAsOf,
        tenant,
        terms: termArray
    })

    // Remove any searches that yielded no results and take the first result where multiple options exist. The latest one
    // be the most recent.
    const tags = searchResponses.filter(response => response.results.length > 0 && response.results[0].header != null).map(response => response.results[0]).filter(isDefined)

    // Get the keys of the datasets that returned results
    const dataKeys = searchResponses.filter(response => response.results.length > 0 && response.results[0].header != null).map(response => response.key).filter(isDefined)

    // The map and filter to header is not needed as we know it is not null from the way tags was created but Typescript complains if we don't check here too
    const resultsPromiseArray = tags.map(tag => tag.header).filter(isDefined).map((header, i) => readSmallDataset<UiEditableRow>({key: dataKeys[i], tenant, tagSelector: header}))

    // Get the datasets from TRAC
    const results = (await Promise.all(resultsPromiseArray)).map(response => ({...response, tag: tags.find(tag => tag?.attrs?.key.stringValue === response.key)}))

    // Now we have an attributes store in the SetMetadata component, this manages all the attributes across the application using a common set of
    // functions and storage. There may be times (such as in the RunAFlowScene) where we need to set an attribute in the background, that's fine there
    // is a function for that but the store needs to be populated for the function to work. If the user has never mounted the SetMetadata component
    // it's store will not have had the default attributes for each use case set. So what we do is do that update here as soon as the data is downloaded.
    const uiAttributesListData = results.find(result => result.key === "ui_attributes_list")?.data as UiAttributesListRow[]
    const uiBusinessSegmentOptionsData = results.find(result => result.key === "ui_business_segment_options")?.data as UiBusinessSegmentsDataRow[]
    const uiBusinessSegmentOptionsSchema = results.find(result => result.key === "ui_business_segment_options")?.schema

    // Do we have what we need to pre-populate the setAttributesStore?
    if (uiAttributesListData && uiBusinessSegmentOptionsData && uiBusinessSegmentOptionsSchema) {

        const allProcessedAttributes = processAttributes(uiAttributesListData, uiBusinessSegmentOptionsData, uiBusinessSegmentOptionsSchema)
        // This stores the master attributes list in the setAttributesStore
        dispatch(setAllProcessedAttributes(allProcessedAttributes))
        // This updates all menus to be just for the new attributes for their use case
        dispatch(processAttributesDataForAllUses())
    }

    return results
})

/**
 * A function that creates a dataset in TRAC for one of the items required by the user interface to function.
 */
export const createSetupItem = createAsyncThunk<// Return type of the payload creator
    { key: UiEditableDatasetKeys, response: trac.metadata.ITagHeader } | undefined,
    // First argument to the payload creator
    ConfirmButtonPayload,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('applicationSetupStore/createSetupItem', async ({id: key}, {getState, dispatch}) => {

    // Get the parameters we need from the store
    const {"trac-tenant": tenant} = getState().applicationStore.cookies

    // If there is no tenant (during set up there may not be) then exit
    if (tenant === undefined) return

    // Some Typescript housekeeping
    if (key == null || !isKeyOf(getState().applicationSetupStore.tracItems.items, key)) return

    // Get the details of the items, one of which we are going to create, each should have a default schema and dataset
    const {attrs, data, fields} = getState().applicationSetupStore.tracItems.items[key].defaultDefinition

    // Send the data to TRAC, the importDataFromJson function can cope with both creates and updates
    const response = await importDataFromJson({attrs, tenant, data, schema: fields})

    // Now that the dataset has been created the dataset is retrieved. This is not strictly necessary if you think about
    // it, the version in the store should be the version in TRAC. However, we download it to sync the store with TRAC
    // as a precaution.
    await dispatch(getSetupItem(response))

    return {key, response}
})

/**
 * A function that updates a dataset in TRAC for one of the items required by the user interface to function.
 */
export const updateSetupItem = createAsyncThunk<// Return type of the payload creator
    void | true,
    // First argument to the payload creator
    void,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('applicationSetupStore/updateSetupItem', async (_, {getState, dispatch}) => {

    // Get the parameters we need from the store
    const {"trac-tenant": tenant} = getState().applicationStore.cookies
    const {key} = getState().applicationSetupStore.editor.control

    if (!key || !tenant) return

    const {data, fields} = getState().applicationSetupStore.editor.items[key]
    const {tag} = getState().applicationSetupStore.tracItems.items[key].currentDefinition

    // Update the data, importData can cope with both creates and updates, we provide the header of the current
    // definition as the version of the dataset to update.
    const response = await importDataFromJson({tenant, data, priorVersion: tag.header || undefined, schema: fields})

    // Now that the dataset has been updated the dataset is retrieved. This is not strictly necessary if you think about
    // it, the version in the store should be the version in TRAC. However, we download it to sync the store with TRAC
    // as a precaution.
    await dispatch(getSetupItem(response))

    // Sync the editor with the new version of the dataset that was retrieved
    dispatch(addDatasetToEditor({id: key}))

    return true
})

/**
 * A function that gets the metadata tag and data for one of the setup datasets. This is run when a dataset is created or
 * updated. It saves the latest version to the store.
 */
export const getSetupItem = createAsyncThunk<// Return type of the payload creator
    GetDatasetByTagResult<UiEditableDatasetKeys, UiEditableRow>,
    // First argument to the payload creator
    trac.metadata.ITagHeader,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('applicationSetupStore/getSetupItem', async (header, {getState}) => {

    // Get the parameters we need from the store
    const {"trac-tenant": tenant} = getState().applicationStore.cookies

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")

    return await getSmallDatasetByTag<UiEditableDatasetKeys, UiEditableRow>(header, {tenant})
})

export const applicationSetupStore = createSlice({
    name: 'applicationSetupStore',
    initialState,
    reducers: {
        /**
         * A reducer that copies the dataset to the editor ready for editing. This is
         * used when the user selected to edit a dataset in the {@link DatasetSummaryTable}
         * component.
         */
        addDatasetToEditor: (state, action: PayloadAction<ConfirmButtonPayload>) => {

            const {id: key} = action.payload

            if (isKeyOf(state.editor.items, key)) {

                state.editor.control.key = key
                state.editor.control.show = true
                state.editor.items[key].row = null

                if (key === "ui_attributes_list") {

                    state.editor.items[key].data = cloneDeep(state.tracItems.items[key].currentDefinition.data) as UiAttributesListRow[]

                } else if (key === "ui_batch_import_data") {

                    state.editor.items[key].data = cloneDeep(state.tracItems.items[key].currentDefinition.data) as UiBatchImportDataRow[]

                } else if (key === "ui_business_segment_options") {

                    state.editor.items[key].data = cloneDeep(state.tracItems.items[key].currentDefinition.data) as UiBusinessSegmentsDataRow[]

                } else if (key === "ui_parameters_list") {

                    state.editor.items[key].data = cloneDeep(state.tracItems.items[key].currentDefinition.data) as UiParametersListRow[]
                }

                // Copy the schema across, the business segment editor allows you to edit the schema labels as a way of editing the
                // names of the tiers of segments
                state.editor.items[key].fields = state.tracItems.items[key].currentDefinition.fields
                // Reset the validation information
                state.editor.items[key].validation = {isValid: {}, validationChecked: false}
                // If the user copied a dataset then reset the change flag
                state.editor.control.userChangedSomething = false
            }
        },
        /**
         * A reducer that adds a new row to the end of the dataset. It can also be used to copy a row but this is
         * only available for adding business segments.
         */
        addRowToData: (state, action: PayloadAction<ButtonPayload>) => {

            const {index, name} = action.payload

            const {key} = state.editor.control

            const isCopy = name === "copy"

            // Some Typescript housekeeping
            if (key === null) return

            let emptyRowIndex: number = -1
            let schemaForRow: trac.metadata.IFieldSchema[] = state.editor.items[key].fields

            if (key === "ui_attributes_list") {

                emptyRowIndex = state.editor.items[key].data.findIndex(row => (row.ID == null || row.ID.replace(' ', '').length === 0 || row.CATEGORY == null || row.BASIC_TYPE == null))

            } else if (key === "ui_batch_import_data") {

                // TODO when properly defined check what the rules should be for detecting an empty row
                emptyRowIndex = state.editor.items[key].data.findIndex(row => (row.DATASET_ID == null || row.DATASET_ID.replace(' ', '').length === 0))

            } else if (key === "ui_business_segment_options") {

                emptyRowIndex = state.editor.items[key].data.findIndex(row => ([1, 2, 3, 4].every(level => {

                    const variable = `GROUP_0${level}_ID`;
                    return isKeyOf(row, variable) && (row[variable] == null || (row[variable] || "").replace(' ', '').length === 0)

                })))

            } else if (key === "ui_parameters_list") {

                emptyRowIndex = state.editor.items[key].data.findIndex(row => (row.ID == null || row.ID.replace(' ', '').length === 0 || row.CATEGORY == null || row.BASIC_TYPE == null))
            }

            if (emptyRowIndex > -1 && !isCopy) {

                showToast("warning", "There is already an empty entry, a new one can not be added.", "handleAddRowToData/warning")

            } else if (emptyRowIndex > -1 && isCopy && emptyRowIndex === index) {

                showToast("warning", "You can not copy an empty row.", "handleAddRowToData/warning")

            } else if (!schemaForRow) {

                showToast("error", "There is no schema for the new row in the store, no new row can be added.", "handleAddRowToData/rejected")

            } else if (!isCopy) {

                if (key === "ui_attributes_list") {

                    const emptyRow = createEmptyRow<UiAttributesListRow>(schemaForRow)

                    emptyRow.RESERVED_FOR_APPLICATION = false
                    emptyRow.NUMBER_OF_ROWS = 1
                    emptyRow.WIDTH_MULTIPLIER = 1
                    emptyRow.HIDDEN = false
                    emptyRow.MUST_VALIDATE = false

                    state.editor.items[key].data.push(emptyRow as UiAttributesListRow);

                } else if (key === "ui_batch_import_data") {

                    const emptyRow = createEmptyRow<UiBatchImportDataRow>(schemaForRow)

                    emptyRow.BUSINESS_SEGMENTS = "IMPAIRMENT||SECURED||UK"
                    emptyRow.DATASET_ID = "experian_scores"
                    emptyRow.DATASET_NAME = "Experian monthly scores"
                    emptyRow.DATASET_DESCRIPTION = "Weekly snapshot of customer level credit risk scores"
                    emptyRow.DATASET_FREQUENCY = "WEEK"
                    emptyRow.DATASET_SOURCE_SYSTEM = "Racetrack"
                    emptyRow.DATASET_DATE_REGEX = "_'week'_I_yyyy"
                    emptyRow.RECONCILIATION_FIELDS = "name=ROW_COUNT;fieldType=INTEGER;fieldIndex=3;aggregationType=COUNT:name=Count of distinct concatenation of PRMACC & APPNUM;fieldType=INTEGER;fieldIndex=4;aggregationType=COUNT_DISTINCT;dataFields=PRMACC,APPPNUM"
                    emptyRow.RECONCILIATION_ITEM_SUFFIX = "_ok"
                    emptyRow.RECONCILIATION_ITEM_FORMAT = "CSV"
                    emptyRow.RECONCILIATION_ITEM_FORMAT_MODIFIER = "NO_HEADER"
                    emptyRow.NEW_FILES_ONLY = true
                    emptyRow.DISABLED = false

                    state.editor.items[key].data.push(emptyRow);

                } else if (key === "ui_business_segment_options") {

                    const emptyRow = createEmptyRow<UiBusinessSegmentsDataRow>(schemaForRow)

                    state.editor.items[key].data.push(emptyRow);

                } else if (key === "ui_parameters_list") {

                    const emptyRow = createEmptyRow<UiParametersListRow>(schemaForRow)

                    state.editor.items[key].data.push(emptyRow);
                }

                state.editor.control.userChangedSomething = true

            } else if (isCopy && index != null) {

                const newRow = {...state.editor.items[key].data[index]}

                if (key === "ui_attributes_list") {

                    state.editor.items[key].data.push(newRow as UiAttributesListRow);

                } else if (key === "ui_batch_import_data") {

                    state.editor.items[key].data.push(newRow as UiBatchImportDataRow);

                } else if (key === "ui_business_segment_options") {


                    state.editor.items[key].data.push(newRow as UiBusinessSegmentsDataRow);

                } else if (key === "ui_parameters_list") {

                    state.editor.items[key].data.push(newRow as UiParametersListRow);
                }

                state.editor.control.userChangedSomething = true
            }
        },
        /**
         * A reducer that updates business segment information when the user edits the values.
         */
        editBusinessSegment: (state, action: PayloadAction<SelectValuePayload>) => {

            const {id: variable, name, isValid, value} = action.payload
            const {index, row} = state.editor.items.ui_business_segment_options

            // Keep Typescript happy
            if (row != null && index != null && typeof value !== "number" && isKeyOf(row, variable)) {

                // Store the new user value
                row[variable] = value

                // If the validation is not yet set add the property for the row to the validation store
                if (!state.editor.items.ui_business_segment_options.validation.isValid.hasOwnProperty(index)) {
                    state.editor.items.ui_business_segment_options.validation.isValid[index] = {}
                }

                // There is a weird thing here where the function is looking across the two inputs,
                // the ID and the name. So either both are OK or both are invalid. So when the validity
                // is saved in the store we need to update the validity of both. Otherwise, one value
                // that was invalid before the second value was changed will still be listed as invalid.
                // To help we have the name property, this is the name of the business segment group
                // without the _ID or _NAME part. i.e. id can be "GROUP_1_ID" and name will be "GROUP_1"

                state.editor.items.ui_business_segment_options.validation.isValid[index][`${name}`] = isValid
                state.editor.items.ui_business_segment_options.validation.validationChecked = false

                // Check if the net results of the updates means a change has been made
                applicationSetupStore.caseReducers.hasChangeBeenMade(state)
            }
        },
        /**
         * A reducer that updates the title of a business segment, this is stored in the dataset's schema.
         */
        editBusinessSegmentTitle: (state, action: PayloadAction<{ id: keyof UiBusinessSegmentsDataRow, value: string | null, isValid: boolean }>) => {

            const {id: variable, isValid, value} = action.payload

            const variableIndex = state.editor.items.ui_business_segment_options.fields.findIndex(field => field.fieldName === variable)

            if (variableIndex > -1) {

                state.editor.items.ui_business_segment_options.fields[variableIndex] = {
                    ...state.editor.items.ui_business_segment_options.fields[variableIndex],
                    // This is a bit of a hack, if the user edits the label to nothing a null value
                    // will cause the fallback to be used, if we intercept this null value and set it
                    // as a string then it will show in nothing in the interface.
                    label: value === null ? "" : value
                }

                // Update the validation information, the number in the isValid object is normally the row number
                // Here we add an index of -1, which can never be a valid row number, to store the title validation
                if (!state.editor.items.ui_business_segment_options.validation.isValid.hasOwnProperty(-1)) {
                    state.editor.items.ui_business_segment_options.validation.isValid[-1] = {}
                }
                state.editor.items.ui_business_segment_options.validation.isValid[-1][`${variable}-title`] = isValid
                state.editor.items.ui_business_segment_options.validation.validationChecked = false

                // Check if the net results of the updates means a change has been made
                applicationSetupStore.caseReducers.hasChangeBeenMade(state)
            }
        },
        /**
         * A reducer that deletes a batch import/parameter/attribute/business segment from the dataset. Note that the payload
         * comes from the {@link ConfirmButton} component so the type is the general type of the onClick prop.
         */
        deleteItem: (state, action: PayloadAction<ConfirmButtonPayload>) => {

            const {index} = action.payload
            const {key} = state.editor.control

            if (key && index != null) {

                state.editor.items[key].data.splice(index, 1);

                // If the user deletes a row the validation info is deleted.
                delete state.editor.items[key].validation.isValid[index]

                // Check if the net results of the updates means a change has been made
                applicationSetupStore.caseReducers.hasChangeBeenMade(state)
            }
        },
        /**
         * A reducer that resets the edited dataset back to the last downloaded version. This calls the
         * reducer that copies the dataset to the editor
         */
        resetEditor: (state, _: PayloadAction<ConfirmButtonPayload>) => {

            const {key} = state.editor.control

            if (key != null) {
                applicationSetupStore.caseReducers.addDatasetToEditor(state, {
                    payload: {id: key},
                    type: "uploadADatasetStore/addDatasetToEditor"
                })
            }
        },
        /**
         * A reducer that shows/hides the validation messages about the user edited values.
         */
        toggleValidationMessages: (state, action: PayloadAction<boolean>) => {

            const {key} = state.editor.control

            if (key != null) {
                state.editor.items[key].validation.validationChecked = action.payload
            }
        },
        /**
         * A reducer that copies the dataset row to the editor ready for editing. A shallow copy is made because we know
         * that the properties are all basic types e.g. a string/boolean rather than objects/arrays. The name property
         * is optional because the same function is used for all datasets not just the business segment options. This is
         * used when the user wants to edit a particular row in the dataset.
         */
        addRowToEditor: (state, action: PayloadAction<{ id: number, name?: "GROUP_01" | "GROUP_02" | "GROUP_03" | "GROUP_04" }>) => {

            const {id: index, name: variable} = action.payload
            const {key} = state.editor.control

            if (key != null) {

                state.editor.items[key].row = {...state.editor.items[key].data[index]}
                state.editor.items[key].index = index

                // variable is only used for business segment editing
                if (key === "ui_business_segment_options" && variable != null) {
                    state.editor.items[key].variable = variable
                }
            }
        },
        /**
         * A reducer that copies the edited row to the dataset ready for saving in TRAC. A shallow copy is made because we know
         * that the properties are all basic types e.g. a string/boolean rather than objects/arrays.
         */
        saveEditedRowToDataset: (state, action: PayloadAction<UiEditableRow>) => {

            const {payload: row} = action
            const {key} = state.editor.control

            if (key != null) {

                const {index} = state.editor.items[key]

                if (index != null) {
                    state.editor.items[key].data[index] = {...row}
                    state.editor.items[key].index = null

                    // Check if the net results of the updates means a change has been made
                    applicationSetupStore.caseReducers.hasChangeBeenMade(state)
                }
            }
        },
        /**
         * This checks whether the user has made a change to the dataset in the editor or the schema (business segments only)
         * this is used mainly in messaging the user with confirmation messages etc.
         */
        hasChangeBeenMade: (state) => {

            const {key} = state.editor.control

            if (key != null) {

                const {data: editorData, fields: editorFields} = state.editor.items[key]
                const {data: currentData, fields: currentFields} = state.tracItems.items[key].currentDefinition

                state.editor.control.userChangedSomething = !(arraysOfObjectsEqual(editorData, currentData) && arraysOfObjectsEqual(editorFields, currentFields))
            }
        },
        /**
         * A reducer that stores any user added category options. This is for the UiAttributesListData editing. The
         * user can add categories using the SelectOption component in creatable mode. We store these options here so
         * that they are available to apply to other attributes they go on to edit.
         */
        addUserSetCategory: (state, action: PayloadAction<OnCreateNewOptionPayload<CreateDetails>>) => {

            state.editor.items.ui_attributes_list.userAddedCategoryOptions.push(action.payload.newOptions)
        },
    },
    extraReducers: (builder) => {

        // A set of lifecycle reducers to run before/after the getSetupItems function
        builder.addCase(getSetupItems.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.tracItems.getSetupItems.status = "pending"
        })
        builder.addCase(getSetupItems.fulfilled, (state, action: PayloadAction<{ key?: string, data: UiEditableRow[], schema: trac.metadata.IFieldSchema[], tag?: trac.metadata.ITag }[] | undefined>) => {

            state.tracItems.getSetupItems.status = "succeeded"

            // payload can be undefined when no tenant is set
            action.payload !== undefined && action.payload.forEach(item => {

                const {key, data, tag, schema} = item

                // key is the dataset key attribute
                if (key !== undefined && tag && isKeyOf(state.tracItems.items, key)) {

                    state.tracItems.items[key].currentDefinition.tag = tag

                    if (key === "ui_attributes_list") {

                        state.tracItems.items[key].currentDefinition.data = sortArrayBy(data as UiAttributesListRow[], "NAME")

                    } else if (key === "ui_batch_import_data") {

                        // Sort the items, this is just a visual aide but helps when the user is editing similar rows
                        // to see them in a hierarchy
                        state.tracItems.items[key].currentDefinition.data = data as UiBatchImportDataRow[]

                    } else if (key === "ui_business_segment_options") {

                        // Sort the items, this is just a visual aide but helps when the user is editing similar rows
                        // to see them in a hierarchy
                        state.tracItems.items[key].currentDefinition.data = sortArrayBy(sortArrayBy(sortArrayBy(sortArrayBy(data as UiBusinessSegmentsDataRow[], "GROUP_01_NAME"), "GROUP_02_NAME"), "GROUP_03_NAME"), "GROUP_04_NAME")

                    } else if (key === "ui_parameters_list") {

                        state.tracItems.items[key].currentDefinition.data = data as UiParametersListRow[]

                    }
                    state.tracItems.items[key].currentDefinition.fields = schema
                    state.tracItems.items[key].currentDefinition.foundInTrac = true
                }
            })
        })
        builder.addCase(getSetupItems.rejected, (state, action) => {

            state.tracItems.getSetupItems.status = "failed"

            const text = {
                title: "Failed to get datasets",
                message: "The existing versions of the application datasets could not be retrieved because of an error.",
                details: action.error.message
            }

            showToast("error", text, "applicationSetupStore/getSetupItems/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the createSetupItem function
        builder.addCase(createSetupItem.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.tracItems.createSetupItem.status = "pending"
        })
        builder.addCase(createSetupItem.fulfilled, (state, action) => {

            if (!action.payload) return

            const {key, response} = action.payload
            state.tracItems.createSetupItem.status = "succeeded"
            showToast("success", `Created dataset ${response.objectId} with version ${response?.objectVersion}`, key)
        })
        builder.addCase(createSetupItem.rejected, (state, action) => {

            state.tracItems.createSetupItem.status = "failed"

            const text = {
                title: "Failed to create dataset",
                message: "The dataset was not created because of an error, this is usually because of an issue with the schema not matching the data.",
                details: action.error.message
            }

            showToast("error", text, "applicationSetupStore/createSetupItem/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the updateSetupItem function
        builder.addCase(updateSetupItem.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.tracItems.updateSetupItem.status = "pending"
        })
        builder.addCase(updateSetupItem.fulfilled, (state, action: PayloadAction<void | true>) => {

            if (action.payload) {
                showToast("success", "The dataset was updated in TRAC", "applicationSetupStore/updateSetupItem/fulfilled")
            }
        })
        builder.addCase(updateSetupItem.rejected, (state, action) => {

            state.tracItems.updateSetupItem.status = "failed"

            const text = {
                title: "Failed to save the dataset",
                message: "The dataset was not saved in TRAC.",
                details: action.error.message
            }

            showToast("error", text, "applicationSetupStore/updateSetupItem/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the getSetupItem function
        builder.addCase(getSetupItem.pending, (state) => {

            // Clear all the messages
            toast.dismiss()
            state.tracItems.getSetupItem.status = "pending"
        })
        builder.addCase(getSetupItem.fulfilled, (state, action) => {

            // key is the dataset key attribute
            const {key, data, tag, schema} = action.payload
            state.tracItems.getSetupItem.status = "succeeded"

            if (key != null) {

                state.tracItems.items[key].currentDefinition.tag = tag

                if (key === "ui_attributes_list") {

                    state.tracItems.items[key].currentDefinition.data = data as UiAttributesListRow[]

                } else if (key === "ui_batch_import_data") {

                    state.tracItems.items[key].currentDefinition.data = data as UiBatchImportDataRow[]

                } else if (key === "ui_business_segment_options") {

                    state.tracItems.items[key].currentDefinition.data = data as UiBusinessSegmentsDataRow[]

                } else if (key === "ui_parameters_list") {

                    state.tracItems.items[key].currentDefinition.data = data as UiParametersListRow[]
                }

                state.tracItems.items[key].currentDefinition.fields = schema
                state.tracItems.items[key].currentDefinition.foundInTrac = true
            }
        })
        builder.addCase(getSetupItem.rejected, (state, action) => {

            state.tracItems.getSetupItem.status = "failed"

            const text = {
                title: "Failed to get the dataset",
                message: "The dataset was not downloaded from TRAC.",
                details: action.error.message
            }

            showToast("error", text, "applicationSetupStore/getSetupItem/rejected")
            console.error(action.error)
        })
    }
})

// Action creators are generated for each case reducer function
export const {
    addDatasetToEditor,
    addRowToEditor,
    addRowToData,
    addUserSetCategory,
    deleteItem,
    editBusinessSegment,
    editBusinessSegmentTitle,
    resetEditor,
    saveEditedRowToDataset,
    toggleValidationMessages
} = applicationSetupStore.actions

export default applicationSetupStore.reducer