import addMonths from "date-fns/addMonths";
import {checkForBatchMetadata} from "../../store/applicationStore";
import {createAsyncThunk, createSlice, PayloadAction} from '@reduxjs/toolkit'
import {
    DataValues,
    DateFormat,
    DatetimeFormat,
    DeepWritable,
    FullTableState,
    GetTagsFromValuePayload,
    OnCreateNewOptionPayload,
    Option,
    SelectDatePayload,
    SelectOptionPayload,
    SelectValuePayload,
    StoreStatus,
    TableRow
} from "../../../types/types_general";
import endOfDay from "date-fns/endOfDay";
import {convertObjectTypeToString, enrichMetadataAndSetDefault} from "../../utils/utils_trac_metadata";
import {MultiValue} from "react-select";
import {isKeyOf, isOption} from "../../utils/utils_trac_type_chckers";
import {RootState} from "../../../storeController";
import {searchByMultipleAndClauses} from "../../utils/utils_trac_api";
import {showToast, sOrNot} from "../../utils/utils_general";
import startOfDay from "date-fns/startOfDay";
import {toast} from "react-toastify";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {Types} from "../../../config/config_trac_classifications";
import {capitaliseString, convertKeyToText} from "../../utils/utils_string";
import {getUniqueKeyFromObject} from "../../utils/utils_object";

// A list of the filter names
type Filters =
    "objectType" |
    "businessSegments" |
    "updatedDate" |
    "dateRange" |
    "jobType" |
    "jobStatus" |
    "modelPath" |
    "modelRepository" |
    "showInSearchResults" |
    "user"

// A type for the all users of this component, entry point in the store state
type StoreUser = {

    options: {
        // Whether to show a button that allows the user to click through to see the selected object
        allowClickThrough: boolean
    }
    // We separate out the selected results as an optimisation, we store them when the user makes a selection,
    // but they shouldn't cause a rerender, so to avoid this we need to store the information out of the
    // searchByAttributes and searchByAttributes properties so the destructuring of the store does not detect a
    // new object
    selectedResults: {
        searchByAttributes: TableRow[],
        searchByObjectId: TableRow[]
    },
    selectedTags: {
        searchByAttributes: trac.metadata.ITag[],
        searchByObjectId: trac.metadata.ITag[],
        status: StoreStatus
    },
    // What tab for what type or search the user is doing
    selectedTab: "searchByAttributes" | "searchByObjectId"
    // Whether the filters should be shown when the component mounts
    showFiltersOnLoad: boolean,
    searchByAttributes: {
        table: {
            // The results of the search
            results: TableRow[]
            // The state of the results table, we store this when the table unmounts so that we
            // can set it up with the same state if the user goes back to the page
            tableState: FullTableState
        }
        // The status of the API request making the search
        status: StoreStatus
        // Any associated with the API request making the search
        message: undefined | string
        // Whether the search that happens as the component loads has completed
        initialSearchComplete: boolean
        // This is the object type of the results, this is needed to set the right schema on the table. You can not
        // reply on the value of the select as the user can change this after a search has been run without setting
        // off a new search
        objectTypeResultsAreFor: trac.ObjectType
    }
    searchByObjectId: {
        table: {
            // The results of the search
            results: TableRow[]
            // The state of the results table, we store this when the table unmounts so that we
            // can set it up with the same state if the user goes back to the page
            tableState: FullTableState
        }
        // The status of the API request making the search
        status: StoreStatus
        // Any associated with the API request making the search
        message: undefined | string
        // Whether to allow searched by one object ID at a time or many
        oneOrMany: "one" | "many"
        // Whether the search had any results
        // Whether the search that happens as the component loads has completed
        initialSearchComplete: boolean
        // The value of the search string, either from the SelectOption component (using one search) or the SelectValue
        // component (using many search))
        searchValue: string | null | Option<string>
        // What object type they are searching for
        objectType: Option<string, trac.ObjectType>
        // This is the object type of the results, this is needed to set the right schema on the table. You can not
        // reply on the value of the select as the user can change this after a search has been run without setting
        // off a new search
        objectTypeResultsAreFor: trac.ObjectType
    }
    filters: {
        // This list defines which filters will show
        filtersToShow: Filters[]
        // Filters that will be shown for all object types
        general: {
            // The values set for the filters below will be the default values used when the
            // component mounts
            dateRange: {
                startDate: string
                endDate: string
                dayOrMonth: DateFormat | DatetimeFormat
                createOrUpdate: "CREATE" | "UPDATE"
            }
            objectType: Option<string, trac.ObjectType>
            businessSegments: MultiValue<Option<string>>
            user: {
                createOrUpdate: "CREATE" | "UPDATE"
                selectedUserOption: Option<string>
            },
            showInSearchResults: Option<boolean | null>
        }
        // Extra filters when the user is searching for a job
        job: {
            jobStatus: Option<string>
            jobType: Option<string>
        }
        // Extra filters when the user is searching for a model
        model: {
            modelPath: null | string
            modelRepository: Option<string>
        }
    }
}

/**
 * A function that creates the basic setup for a user of the component, this function or a modified version of it
 * is needed when adding new users.
 */
function setUpStoreUser(objectType: trac.ObjectType, allowClickThrough: boolean, filtersToShow: Filters[]): StoreUser {

    return ({
            // See note in the interface
            options: {
                allowClickThrough: allowClickThrough
            },
            selectedResults: {
                searchByAttributes: [],
                searchByObjectId: []
            },
            selectedTags: {
                searchByAttributes: [],
                searchByObjectId: [],
                status: "idle"
            },
            selectedTab: "searchByAttributes",
            showFiltersOnLoad: true,
            searchByAttributes: {
                table: {
                    results: [],
                    tableState: {}
                },
                status: "idle",
                message: undefined,
                initialSearchComplete: false,
                objectTypeResultsAreFor: trac.ObjectType.JOB,
            },
            searchByObjectId: {
                table: {
                    results: [],
                    tableState: {}
                },
                status: "idle",
                oneOrMany: "many",
                message: undefined,
                initialSearchComplete: false,
                searchValue: null,
                objectType: Types.tracObjectTypes.find(option => option.type === objectType) || {
                    value: "JOB",
                    label: "Job",
                    type: trac.ObjectType.JOB
                },
                objectTypeResultsAreFor: trac.ObjectType.JOB
            },
            filters: {
                filtersToShow: filtersToShow,
                general: {
                    dateRange: {
                        // We default to 4 months of history
                        startDate: (startOfDay(addMonths(new Date(), -4))).toISOString(),
                        endDate: (endOfDay(new Date())).toISOString(),
                        dayOrMonth: "DAY",
                        createOrUpdate: "UPDATE"
                    },
                    // Pick an option matching the required type, default to JOB if not found
                    objectType: Types.tracObjectTypes.find(option => option.type === objectType) || {
                        value: "JOB",
                        label: "Job",
                        type: trac.ObjectType.JOB
                    },
                    businessSegments: [{value: "ALL", label: "All"}],
                    user: {
                        createOrUpdate: "UPDATE",
                        selectedUserOption: {value: "ALL", label: "All"}
                    },
                    showInSearchResults: {value: true, label: "True"}
                },
                job: {
                    jobStatus: {value: "ALL", label: "All"},
                    jobType: {value: "ALL", label: "All"},
                },
                model: {
                    modelPath: null,
                    modelRepository: {value: "ALL", label: "All"}
                }
            }
        }
    )
}

// Define a type for the slice state
export interface FindInTracStoreState {
    resultsSettings: {
        schema: trac.metadata.IFieldSchema[]
        tooltips: { [key in Filters]?: string }
    }
    uses: {
        findAJob: StoreUser
        search: StoreUser
        updateTags: StoreUser
        dataAnalytics: StoreUser
    }
}

// If you need to extend the application to use the search component then simply copy a store user
// to a new property and load that property in the new scene.
const initialState: FindInTracStoreState = {

    resultsSettings: {
        schema: [
            {
                fieldName: "key", label: "Key",
                fieldType: trac.STRING, fieldOrder: 0, categorical: true
            },
            {
                fieldName: "objectType", label: "Object type ID",
                fieldType: trac.INTEGER, fieldOrder: 1, categorical: false
            },
            {
                fieldName: "objectTypeAsString", label: "Object type",
                fieldType: trac.STRING, fieldOrder: 2, categorical: true
            },
            {fieldName: "objectId", label: "Object ID", fieldType: trac.STRING, fieldOrder: 3},
            {fieldName: "objectVersion", label: "Object version", fieldType: trac.INTEGER, fieldOrder: 4},
            {fieldName: "tagVersion", label: "Tag version", fieldType: trac.INTEGER, fieldOrder: 5},
            {fieldName: "name", label: "Name", fieldType: trac.STRING, fieldOrder: 6},
            {fieldName: "description", label: "Description", fieldType: trac.STRING, fieldOrder: 7},
            {fieldName: "createdBy", label: "Created by", fieldType: trac.STRING, fieldOrder: 8, categorical: true},
            {fieldName: "updatedBy", label: "Updated by", fieldType: trac.STRING, fieldOrder: 9, categorical: true},
            {
                fieldName: "businessSegments",
                label: "Business segments",
                fieldType: trac.STRING,
                fieldOrder: 10,
                categorical: true
            },
            {
                fieldName: "creationTime",
                label: "Creation time",
                fieldType: trac.DATETIME,
                formatCode: "DATETIME",
                fieldOrder: 11
            },
            {
                fieldName: "updatedTime",
                label: "Updated time",
                fieldType: trac.DATETIME,
                formatCode: "DATETIME",
                fieldOrder: 12
            },

            {fieldName: "jobType", label: "Job type", fieldType: trac.STRING, fieldOrder: 13, categorical: true},
            {fieldName: "jobStatus", label: "Job status", fieldType: trac.STRING, fieldOrder: 14, categorical: true},
            {
                fieldName: "modelRepository",
                label: "Model repository",
                fieldType: trac.STRING,
                fieldOrder: 15,
                categorical: true
            },
            {fieldName: "modelPath", label: "Model path", fieldType: trac.STRING, fieldOrder: 16},
        ],
        tooltips: {
            dateRange: "The date that the object was created or updated, it is not possible to search by when the tags were updated.",
            showInSearchResults: "Objects can be set to be removed from search results when running models or flows, you can filter by this attribute."
        }
    },
    uses: {
        findAJob: setUpStoreUser(trac.ObjectType.JOB, true, ["updatedDate", "businessSegments", "user", "jobStatus", "jobType", "modelPath", "modelRepository", "showInSearchResults"]),
        search: setUpStoreUser(trac.ObjectType.JOB, true, ["objectType", "updatedDate", "businessSegments", "user", "jobStatus", "jobType", "modelPath", "modelRepository", "showInSearchResults"]),
        updateTags: setUpStoreUser(trac.ObjectType.DATA, false, ["objectType", "updatedDate", "businessSegments", "user", "jobStatus", "jobType", "modelPath", "modelRepository", "showInSearchResults"]),
        dataAnalytics: setUpStoreUser(trac.ObjectType.DATA, false, ["updatedDate", "businessSegments", "user", "showInSearchResults"]),
    }
}

/**
 * A function that searches TRAC for objects using the search terms set by the user.
 */
export const doSearch = createAsyncThunk<// Return type of the payload creator
    { storeKey: keyof FindInTracStoreState["uses"], results: trac.metadata.ITag[] },
    // First argument to the payload creator
    { storeKey: keyof FindInTracStoreState["uses"] },
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('findInTracStore/doSearch', async (payload, {getState}) => {

    // The store user to do the search for
    const {storeKey} = payload

    // Get what we need from the store
    const {searchAsOf} = getState().applicationStore.tracApi
    const {"trac-tenant": tenant} = getState().applicationStore.cookies

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")

    // The object type to search for, this has to be set to be able to do an attributes based search
    const selectedObjectType = getState().findInTracStore.uses[storeKey].filters.general.objectType?.type

    const startDate = getState().findInTracStore.uses[storeKey].filters.general.dateRange.startDate
    const endDate = getState().findInTracStore.uses[storeKey].filters.general.dateRange.endDate

    const createOrUpdateDateRange = getState().findInTracStore.uses[storeKey].filters.general.dateRange.createOrUpdate

    const showInSearchResults = getState().findInTracStore.uses[storeKey].filters.general.showInSearchResults.value

    const businessSegments = getState().findInTracStore.uses[storeKey].filters.general.businessSegments

    const user = getState().findInTracStore.uses[storeKey].filters.general.user.selectedUserOption.value
    const createOrUpdateUser = getState().findInTracStore.uses[storeKey].filters.general.user.createOrUpdate

    const jobStatus = getState().findInTracStore.uses[storeKey].filters.job.jobStatus.value
    const jobType = getState().findInTracStore.uses[storeKey].filters.job.jobType.value

    const modelPath = getState().findInTracStore.uses[storeKey].filters.model.modelPath
    const modelRepository = getState().findInTracStore.uses[storeKey].filters.model.modelRepository.value

    // Create/update datetime search - this is always set
    const startDateCriteria: trac.metadata.ISearchExpression = {
        term: {
            attrName: createOrUpdateDateRange === "CREATE" ? "trac_create_time" : "trac_update_time",
            attrType: trac.DATETIME,
            operator: trac.SearchOperator.GE,
            searchValue: {datetimeValue: {isoDatetime: startDate}}
        }
    }

    const endDateCriteria: trac.metadata.ISearchExpression = {
        term: {
            attrName: createOrUpdateDateRange === "CREATE" ? "trac_create_time" : "trac_update_time",
            attrType: trac.DATETIME,
            operator: trac.SearchOperator.LE,
            searchValue: {datetimeValue: {isoDatetime: endDate}}
        }
    }

    // Business segment searches
    const businessSegmentSearchCriteria: trac.metadata.ISearchExpression[] = businessSegments.filter(option => option.value.toUpperCase() !== "ALL").map(option => {

        return (
            {
                term: {
                    attrName: "business_segments",
                    attrType: trac.BasicType.STRING,
                    operator: trac.SearchOperator.EQ,
                    searchValue: {
                        type: {
                            basicType: trac.BasicType.STRING
                        },
                        stringValue: option.value
                    }
                }
            }
        )
    })

    // Create/updated by user search
    const userCriteria: trac.metadata.ISearchExpression = {
        term: {
            attrName: createOrUpdateUser === "CREATE" ? "trac_create_user_id" : "trac_update_user_id",
            attrType: trac.STRING,
            operator: trac.SearchOperator.EQ,
            searchValue: {stringValue: user}
        }
    }

    // Job only searches
    const jobStatusCriteria: trac.metadata.ISearchExpression = {
        term: {
            attrName: "trac_job_status",
            attrType: trac.STRING,
            operator: trac.SearchOperator.EQ,
            searchValue: {stringValue: jobStatus}
        }
    }

    const jobTypeCriteria: trac.metadata.ISearchExpression = {
        term: {
            attrName: "trac_job_type",
            attrType: trac.STRING,
            operator: trac.SearchOperator.EQ,
            searchValue: {stringValue: jobType}
        }
    }

    // Model only searches
    const modelRepositoryCriteria: trac.metadata.ISearchExpression = {
        term: {
            attrName: "trac_model_repository",
            attrType: trac.STRING,
            operator: trac.SearchOperator.EQ,
            searchValue: {stringValue: modelRepository}
        }
    }

    // TODO this should get a contains option at some point
    const modelEntryPointCriteria: trac.metadata.ISearchExpression = {
        term: {
            attrName: "trac_model_entry_point",
            attrType: trac.STRING,
            operator: trac.SearchOperator.EQ,
            searchValue: {stringValue: modelPath}
        }
    }

    // TODO is there a way to check for it not being set?
    const hiddenCriteria: trac.metadata.ISearchExpression = {
        term: {
            attrName: "show_in_search_results",
            attrType: trac.BOOLEAN,
            operator: trac.SearchOperator.EQ,
            searchValue: {booleanValue: showInSearchResults}
        }
    }

    // The date searches are always set, the others get added if the user is not asking for 'ALL'
    let expr: trac.metadata.ISearchExpression[] = [startDateCriteria, endDateCriteria]

    if (user !== "ALL") expr.push(userCriteria)

    if (businessSegmentSearchCriteria.length > 0) expr = expr.concat(businessSegmentSearchCriteria)

    if (selectedObjectType === trac.ObjectType.JOB && jobType !== "ALL") expr.push(jobTypeCriteria)
    if (selectedObjectType === trac.ObjectType.JOB && jobStatus !== "ALL") expr.push(jobStatusCriteria)

    if (selectedObjectType === trac.ObjectType.MODEL && modelRepository !== "ALL") expr.push(modelRepositoryCriteria)
    if (selectedObjectType === trac.ObjectType.MODEL && (modelPath != null && modelPath.toUpperCase() !== "ALL")) expr.push(modelEntryPointCriteria)

    if (showInSearchResults != null) expr.push(hiddenCriteria)

    // Complete all the searches
    // Note that the searchByMultipleAndClauses function has its own logic to search for items with 'show_in_search_results' set to true. Here
    // we disable this and pass in our own search expression (hiddenCriteria) which is more flexible due to the nature of this component. Note
    // that we do not limit the number of search results this search can bring back
    const results = await searchByMultipleAndClauses({
        includeOnlyShowInSearchResultsIsTrue: false,
        maximumNumberOfOptionsReturned: 9999999,
        objectType: selectedObjectType,
        searchAsOf,
        tenant,
        terms: expr
    })

    return {storeKey, results}
})

/**
 * A function that stores the rows of data selected by the user and gets the metadata for the objects
 * selected.
 */
export const saveSelectedRowsAndGetTags = createAsyncThunk<// Return type of the payload creator
    void | { storeKey?: string, tags?: trac.metadata.ITag[], id?: string },
    // First argument to the payload creator
    { storeKey?: string, selectedRows?: TableRow[], id?: string },
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('findInTracStore/saveSelectedRowsAndGetTags', async (payload, {dispatch, getState}) => {

    // The store user to do the search for
    const {id, storeKey, selectedRows} = payload

    // Do not run if the required variables are not set
    if (!selectedRows || !isKeyOf(getState().findInTracStore.uses, storeKey) || !(id === "searchByAttributes" || id === "searchByObjectId")) return

    // Convert the selected table rows into a tag selector that we can use to get
    // the object metadata
    const tagSelectors: trac.metadata.ITagHeader[] = selectedRows.map(row => {
        return ({
            objectId: typeof row.objectId === "string" ? row.objectId : undefined,
            tagVersion: typeof row.tagVersion === "number" ? row.tagVersion : undefined,
            objectVersion: typeof row.objectVersion === "number" ? row.objectVersion : undefined,
            objectType: typeof row.objectType === "number" ? row.objectType : undefined
        })
    })

    // Get the metadata, we use a store to prevent getting the same metadata from the API multiple times
    const tags = await dispatch(checkForBatchMetadata(tagSelectors)).unwrap()

    return {id, storeKey, tags}
})

/**
 * A function that runs when a search completes and the results need to be converted to be shown in a table.
 * @param searchResults - The results from the doSearch function.
 */
const convertSearchResultsToTable = (searchResults: trac.metadata.ITag[]): Record<string, DataValues | (DataValues)[] | Record<string, DataValues>>[] => {

    return searchResults.map(item => {

        let row: Record<string, DataValues | (DataValues)[] | { [key: string]: DataValues }> = {}

        row.key = item.attrs?.key?.stringValue || "Not set"
        row.objectType = item.header?.objectType || null
        row.objectTypeAsString = capitaliseString(convertObjectTypeToString(item.header?.objectType || trac.ObjectType.OBJECT_TYPE_NOT_SET))
        row.objectId = item.header?.objectId || null
        row.objectVersion = item.header?.objectVersion || null
        row.tagVersion = item.header?.tagVersion || null
        row.createdBy = `${item?.attrs?.trac_create_user_name.stringValue} (${item?.attrs?.trac_create_user_id.stringValue})`
        row.updatedBy = `${item?.attrs?.trac_update_user_name.stringValue} (${item?.attrs?.trac_update_user_id.stringValue})`
        row.name = enrichMetadataAndSetDefault(item?.attrs?.name, {}, "Not set")
        row.description = enrichMetadataAndSetDefault(item?.attrs?.description, {}, "Not set")
        row.businessSegments = enrichMetadataAndSetDefault(item?.attrs?.business_segments, {})
        row.creationTime = item.attrs?.trac_create_time.datetimeValue?.isoDatetime || null
        row.updatedTime = item.attrs?.trac_update_time.datetimeValue?.isoDatetime || null

        if (item.header?.objectType === trac.ObjectType.JOB) {
            row.jobType = convertKeyToText(item.attrs?.trac_job_type?.stringValue || "Not set")
            row.jobStatus = item.attrs?.trac_job_status?.stringValue || "Not set"
        } else if (item.header?.objectType === trac.ObjectType.MODEL) {
            row.modelRepository = item.attrs?.trac_model_repository?.stringValue || "Not set"
            row.modelPath = item.attrs?.trac_model_entry_point?.stringValue || "Not set"
        }

        return row
    })
}

export const FindInTracStore = createSlice({
    name: 'findInTracStore',
    initialState: initialState,
    reducers: {

        /**
         * A reducer that sets whether the search by attributes or by object ID/key tab is bring used. The selectedTab
         * variable should not be used during any async operation because the user may change tab during the async call.
         */
        setTab: (state, action: PayloadAction<{ storeKey: string, selectedTab: string }>) => {

            const {storeKey, selectedTab} = action.payload

            if (isKeyOf(state.uses, storeKey) && (selectedTab === "searchByAttributes" || selectedTab === "searchByObjectId")) {
                state.uses[storeKey].selectedTab = selectedTab
            }
        },
        /**
         * A reducer that sets what object type the search is for. This is for when searching by attributes.
         */
        setAttributeObjectTypeFilter: (state, action: PayloadAction<DeepWritable<SelectOptionPayload<Option<string, trac.metadata.ObjectType>, false>>>) => {

            const {storeKey} = action.payload

            if (isKeyOf(state.uses, storeKey) && action.payload && action.payload.value) {
                state.uses[storeKey].filters.general.objectType = action.payload.value
            }
        },
        /**
         * A reducer that sets a what object type the search is for. This is for when searching by object ID/key.
         */
        setObjectIdObjectTypeFilter: (state, action: PayloadAction<DeepWritable<SelectOptionPayload<Option<string, trac.metadata.ObjectType>, false>>>) => {

            const {storeKey} = action.payload

            if (isKeyOf(state.uses, storeKey) && action.payload && action.payload.value) {
                state.uses[storeKey].searchByObjectId.objectType = action.payload.value
            }
        },
        /**
         * A reducer that sets the value of the SelectValue component text area when searching by object ID/key and
         * the user is allowed to search for many items at a time. This is used when 'oneOrMany' is set to 'many' and the
         * SelectValue component is supplying the results. When only one search at a time is allowed we store we do not
         * need the equivalent function as we handle storing fetching and storing the entered text differently.
         */
        setManyObjectIdSearchValue: (state, action: PayloadAction<SelectValuePayload>) => {

            const {storeKey, value} = action.payload

            if (isKeyOf(state.uses, storeKey) && typeof value !== "number") {
                state.uses[storeKey].searchByObjectId.searchValue = value
            }
        },
        /**
         * A reducer that sets whether a search is running by object ID/key. This
         * just means the text area or select component will be disabled while the search is running.
         */
        setObjectIdSearchStatus: (state, action: PayloadAction<{ storeKey: undefined | string, running: boolean }>) => {

            const {storeKey, running} = action.payload

            if (isKeyOf(state.uses, storeKey)) {

                // If a search goes from true (pending) to false assume a completed search occurred
                if (state.uses[storeKey].searchByObjectId.status === "pending" && !running) state.uses[storeKey].searchByObjectId.initialSearchComplete = true

                state.uses[storeKey].searchByObjectId.status = running ? "pending" : "succeeded"
            }
        },
        /**
         * A reducer that gets the results for the objects searched for by object ID/key and then converts them to
         * table data for the user to select from. This is used when 'oneOrMany' is set to 'one' and the SelectOption
         * component is supplying the results. When multiple searches at a time is allowed we store the value using the
         * addManyObjectIdSearchResultsToTable reducer.
         */
        addOneObjectIdSearchResultToTable: (state, action: PayloadAction<OnCreateNewOptionPayload<trac.metadata.ITag>>) => {

            const {storeKey} = action.payload

            // Add the option as the value to show in the SelectOption component
            if (isKeyOf(state.uses, storeKey)) {

                // Update the variables that indicate that the process was running
                state.uses[storeKey].searchByObjectId.status = "succeeded"
                state.uses[storeKey].searchByObjectId.message = undefined

                // We record that a search has been done, this is done to make the table of results permanently
                // visible after the first search
                state.uses[storeKey].searchByObjectId.initialSearchComplete = true

                // We get returned a set of options, but we need to get the tag from each to be able to put it into the table
                state.uses[storeKey].searchByObjectId.table.results = convertSearchResultsToTable(action.payload.newOptions.map(option => option.tag)) as TableRow[]

                // Set the selected option as the new one added, this is just a UI thing, we show the user what they pasted
                state.uses[storeKey].searchByObjectId.searchValue = {
                    value: action.payload.inputValue,
                    label: action.payload.inputValue
                }
            }

            showToast("success", "New search results were added successfully", "findInTracStore/addOneObjectIdSearchResultToTable/success")
        },
        /**
         * A reducer that gets the results for the objects searched for by object ID/key and then converts them to
         * table data for the user to select from. This is used when 'oneOrMany' is set to 'many' and the SelectValue
         * component is supplying the results. The payload is empty except for the storeKey to update when no search
         * results are found for a variety of reasons e.g. object IDs don't exist.
         */
        addManyObjectIdSearchResultsToTable: (state, action: PayloadAction<GetTagsFromValuePayload | string>) => {

            if (typeof action.payload !== "string") {

                const {storeKey} = action.payload

                // Add the option as the value to show in the SelectOption component
                if (isKeyOf(state.uses, storeKey)) {

                    // Update the variables that indicate that the process was running
                    state.uses[storeKey].searchByObjectId.status = "succeeded"
                    state.uses[storeKey].searchByObjectId.message = undefined

                    // If the user has changed the search object type then clear the selected rows, there is a useEffect hook in the Table component
                    // to sync the selected rows to this value
                    if (state.uses[storeKey].searchByObjectId.objectTypeResultsAreFor !== state.uses[storeKey].searchByObjectId.objectType.type && state.uses[storeKey].selectedResults.searchByObjectId.length > 0) {
                        // Clear the selected rows
                        state.uses[storeKey].selectedResults.searchByObjectId = []
                        // Clear the selected tags
                        state.uses[storeKey].selectedTags.searchByObjectId = []
                    }

                    // If the user has not changed the search object type then we need to update the selected rows, some selected rows may not be
                    // in the new search results or in different positions
                    if (state.uses[storeKey].selectedResults.searchByObjectId.length > 0 && state.uses[storeKey].searchByObjectId.objectTypeResultsAreFor === state.uses[storeKey].searchByObjectId.objectType.type) {

                        const newRowKeys = state.uses[storeKey].searchByObjectId.table.results.map(row => getUniqueKeyFromObject(row, ["objectId", "objectVersion", "tagVersion"]))

                        state.uses[storeKey].selectedResults.searchByObjectId = state.uses[storeKey].selectedResults.searchByObjectId.filter(selectedResult => {
                            return newRowKeys.includes(getUniqueKeyFromObject(selectedResult, ["objectId", "objectVersion", "tagVersion"]))
                        })

                        state.uses[storeKey].selectedTags.searchByObjectId = state.uses[storeKey].selectedTags.searchByObjectId.filter(selectedTag => {
                            return selectedTag?.header && newRowKeys.includes(getUniqueKeyFromObject(selectedTag?.header, ["objectId", "objectVersion", "tagVersion"]))
                        })
                    }

                    // Store what type of object was searched for, this isn't precise because we only do it at the end
                    state.uses[storeKey].searchByObjectId.objectTypeResultsAreFor = state.uses[storeKey].searchByObjectId.objectType.type

                    // We record that a search has been done, this is done to make the table of results permanently
                    // visible after the first search
                    state.uses[storeKey].searchByObjectId.initialSearchComplete = true

                    // We get returned a set of options, but we need to get the tag from each to be able to put it into the table
                    state.uses[storeKey].searchByObjectId.table.results = convertSearchResultsToTable(action.payload.tags) as TableRow[]

                    // The value of the SelectValue component already matches the search value as the search is triggered
                    // by hitting enter - so there is no need to update the value stored in th
                    showToast("success", `${action.payload.tags.length} new search result${sOrNot(action.payload.tags)} ${action.payload.tags.length === 1 ? "was" : "were"} added successfully`, "findInTracStore/addManyObjectIdSearchResultsToTable/success")
                }

            } else {

                const storeKey = action.payload

                if (isKeyOf(state.uses, storeKey)) {

                    state.uses[storeKey].searchByObjectId.status = "succeeded"
                    state.uses[storeKey].searchByObjectId.message = undefined

                    // Store what type of object was searched for, this isn't precise because we only do it at the end
                    state.uses[storeKey].searchByObjectId.objectTypeResultsAreFor = state.uses[storeKey].searchByObjectId.objectType.type

                    state.uses[storeKey].searchByObjectId.initialSearchComplete = true
                    state.uses[storeKey].searchByObjectId.table.results = []
                }
            }
        },
        /**
         * A reducer that sets the values of the date range filter.
         */
        setDateRangeFilter: (state, action: PayloadAction<SelectDatePayload>) => {

            const {storeKey, value, name} = action.payload

            if (isKeyOf(state.uses, storeKey) && name === "startDate" && typeof value === "string") {
                state.uses[storeKey].filters.general.dateRange[name] = value
            } else if (isKeyOf(state.uses, storeKey) && name === "endDate" && typeof value === "string") {
                state.uses[storeKey].filters.general.dateRange[name] = value
            }
        },
        /**
         * A reducer that sets whether the user can use a day range or a month range for searching for objects in TRAC.
         */
        setDayOrMonth: (state, action: PayloadAction<{ storeKey: string, dayOrMonth: DateFormat | DatetimeFormat, startDate: string, endDate: string }>) => {

            const {storeKey, dayOrMonth, startDate, endDate} = action.payload

            if (isKeyOf(state.uses, storeKey)) {
                state.uses[storeKey].filters.general.dateRange.dayOrMonth = dayOrMonth
                state.uses[storeKey].filters.general.dateRange.startDate = startDate
                state.uses[storeKey].filters.general.dateRange.endDate = endDate
            }
        },
        /**
         * A reducer that sets whether to search by the created or updated information for both the date and user
         * filters.
         */
        setCreateOrUpdate: (state, action: PayloadAction<{ storeKey: string, createOrUpdate: "CREATE" | "UPDATE", name: string }>) => {

            const {storeKey, createOrUpdate, name} = action.payload

            if (isKeyOf(state.uses, storeKey) && (name === "user" || name === "dateRange")) {
                state.uses[storeKey].filters.general[name].createOrUpdate = createOrUpdate
            }
        },
        /**
         * A reducer that sets what business segments they want to search for in TRAC. If they have selected an option
         * other than the 'All' option then we remove the 'All' option from the list. The list can not be set empty.
         */
        setBusinessSegment: (state, action: PayloadAction<DeepWritable<SelectOptionPayload<Option<string>, true>>>) => {

            const {storeKey, value} = action.payload

            if (isKeyOf(state.uses, storeKey)) {

                // Is 'All' already selected
                const wasAllSelected = Boolean(state.uses[storeKey].filters.general.businessSegments.findIndex(option => option.value === "ALL") > -1)
                // Has 'All' just been selected
                const isAllSelected = Boolean(value.findIndex(option => option.value === "ALL") > -1)

                if (value.length === 0) {
                    state.uses[storeKey].filters.general.businessSegments = [{value: "ALL", label: "All"}]
                } else if (value.length > 1 && wasAllSelected) {
                    // The user had 'All'  selected and then went to change this to one of the subsegments, so we
                    // remove the 'All' option.
                    state.uses[storeKey].filters.general.businessSegments = value.filter(option => option.value !== "ALL")
                } else if (value.length > 1 && !wasAllSelected && isAllSelected) {
                    // The user had subsegments selected and then clicked 'All' so we remove the subsegments and leave
                    // only the 'All' options.
                    state.uses[storeKey].filters.general.businessSegments = value.filter(option => option.value === "ALL")
                } else {
                    state.uses[storeKey].filters.general.businessSegments = value
                }
            }
        },
        /**
         * A reducer that sets the option of the user filter
         */
        setUserFilter: (state, action: PayloadAction<SelectOptionPayload<Option<string>, false>>) => {

            const {storeKey, value} = action.payload

            if (isKeyOf(state.uses, storeKey) && value != null) {

                state.uses[storeKey].filters.general.user.selectedUserOption = value
            }
        },
        /**
         * A reducer that sets a filter option for job searches.
         */
        setJobFilter: (state, action: PayloadAction<SelectOptionPayload<Option<string>, false>>) => {

            const {storeKey, value, name} = action.payload

            if (value != null && isKeyOf(state.uses, storeKey) && isKeyOf(state.uses[storeKey].filters.job, name)) state.uses[storeKey].filters.job[name] = value
        },
        /**
         * A reducer that sets a filter option for model searches.
         */
        setModelFilter: (state, action: PayloadAction<SelectOptionPayload<Option<string>, false> | SelectValuePayload<string>>) => {

            const {storeKey, value, name} = action.payload

            if (isKeyOf(state.uses, storeKey) && isKeyOf(state.uses[storeKey].filters.model, name)) {

                if (name === "modelRepository" && isOption(value)) {
                    state.uses[storeKey].filters.model[name] = value
                } else if (name === "modelPath" && !isOption(value)) {
                    state.uses[storeKey].filters.model[name] = value
                }
            }
        },
        /**
         * A reducer that sets a filter option for the show in search results attribute.
         */
        setShowInSearchResultsFilter: (state, action: PayloadAction<SelectOptionPayload<Option<null | boolean>, false>>) => {

            const {storeKey, value} = action.payload

            if (isKeyOf(state.uses, storeKey) && value != null) {

                state.uses[storeKey].filters.general.showInSearchResults = value
            }
        },
        /**
         * A reducer that stores the state of the search result table so that if the user navigates away and then comes
         * back to the page.
         */
        getTableState: (state, action: PayloadAction<{ storeKey?: string, tableState: FullTableState, id?: string }>) => {

            const {storeKey, tableState, id: selectedTab} = action.payload

            if (isKeyOf(state.uses, storeKey) && (selectedTab === "searchByObjectId" || selectedTab === "searchByAttributes") && isKeyOf(state.uses[storeKey], selectedTab)) state.uses[storeKey][selectedTab].table.tableState = tableState
        }
    },
    extraReducers: (builder) => {

        // A set of lifecycle reducers to run before/after the doSearch function
        builder.addCase(doSearch.pending, (state, action) => {

            // Clear all the messages
            toast.dismiss()

            const {storeKey} = action.meta.arg

            state.uses[storeKey].searchByAttributes.status = "pending"

            // If the user has changed the search object type then clear the selected rows, there is a useEffect hook in the Table component
            // to sync the selected rows to this value
            if (state.uses[storeKey].searchByAttributes.objectTypeResultsAreFor !== state.uses[storeKey].filters.general.objectType.type && state.uses[storeKey].selectedResults.searchByAttributes.length > 0) {
                // Clear the selected rows
                state.uses[storeKey].selectedResults.searchByAttributes = []
                // Clear the selected tags
                state.uses[storeKey].selectedTags.searchByAttributes = []
            }

        })
        builder.addCase(doSearch.fulfilled, (state, action: PayloadAction<{ storeKey: keyof FindInTracStoreState["uses"], results: trac.metadata.ITag[] }>) => {

            const {storeKey, results} = action.payload
            state.uses[storeKey].searchByAttributes.status = "succeeded"
            state.uses[storeKey].searchByAttributes.message = undefined
            state.uses[storeKey].searchByAttributes.initialSearchComplete = true
            state.uses[storeKey].searchByAttributes.table.results = convertSearchResultsToTable(results) as TableRow[]

            // If the user has not changed the search object type then we need to update the selected rows, some selected rows may not be
            // in the new search results or in different positions
            if (state.uses[storeKey].selectedResults.searchByAttributes.length > 0 && state.uses[storeKey].searchByAttributes.objectTypeResultsAreFor === state.uses[storeKey].filters.general.objectType.type) {

                const newRowKeys = state.uses[storeKey].searchByAttributes.table.results.map(row => getUniqueKeyFromObject(row, ["objectId", "objectVersion", "tagVersion"]))

                state.uses[storeKey].selectedResults.searchByAttributes = state.uses[storeKey].selectedResults.searchByAttributes.filter(selectedResult => {
                    return newRowKeys.includes(getUniqueKeyFromObject(selectedResult, ["objectId", "objectVersion", "tagVersion"]))
                })

                state.uses[storeKey].selectedTags.searchByAttributes = state.uses[storeKey].selectedTags.searchByAttributes.filter(selectedTag => {
                    return selectedTag?.header && newRowKeys.includes(getUniqueKeyFromObject(selectedTag.header, ["objectId", "objectVersion", "tagVersion"]))
                })
            }

            // Store what type of object was searched for
            state.uses[storeKey].searchByAttributes.objectTypeResultsAreFor = state.uses[storeKey].filters.general.objectType.type

            if (results.length === 0) {
                // If the search has no results the user gets a blank table, here we give them some feedback that there
                // were no issues
                showToast("success", "Your search ran successfully but there were no matching items", "findInTracSTore/deSearch/fulfilled")
            }
        })
        builder.addCase(doSearch.rejected, (state, action) => {

            const {storeKey} = action.meta.arg

            state.uses[storeKey].searchByAttributes.status = "failed"
            state.uses[storeKey].searchByAttributes.initialSearchComplete = true

            const text = {
                title: "Failed to complete search",
                message: `The search for ${convertObjectTypeToString(state.uses[storeKey].filters.general.objectType.type, true, true)}s based on your criteria did not complete successfully.`,
                details: action.error.message
            }

            showToast("error", text, "findInTrac/doSearch/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the saveSelectedRowsAndGetTags function
        builder.addCase(saveSelectedRowsAndGetTags.pending, (state, action) => {

            // Clear all the messages
            toast.dismiss()

            const {storeKey, id: selectedTab, selectedRows} = action.meta.arg

            // We need to put the selected rows into the store in the pending stage of the async call
            // to get the metadata for the rows. This is because we have a hook in the table that checks
            // that the selected rows in this store and the table state are in sync, if we let the FindInTrac
            // component render without the selected rows up to date (as this is what happens in the pending
            // phase) then the table and the store would be out of sync and additional calls would be made to
            // align them.

            // The length check is an optimization to make sure that you don't replace an empty array with
            // an empty array and cause a rerender
            if (selectedTab !== null && isKeyOf(state.uses, storeKey) && isKeyOf(state.uses[storeKey].selectedResults, selectedTab)) {

                state.uses[storeKey].selectedTags.status = "pending"

                if (selectedRows && !(state.uses[storeKey].selectedResults[selectedTab].length === 0 && selectedRows.length === 0)) {

                    state.uses[storeKey].selectedResults[selectedTab] = selectedRows
                }
            }
        })
        builder.addCase(saveSelectedRowsAndGetTags.fulfilled, (state, action: PayloadAction<void | { storeKey?: string, tags?: trac.metadata.ITag[], id?: string }>) => {

            if (!action.payload) return

            const {storeKey, tags, id: selectedTab} = action.payload

            if (selectedTab !== null && isKeyOf(state.uses, storeKey) && isKeyOf(state.uses[storeKey].selectedResults, selectedTab) && tags) {

                // The length checks are an optimization to make sure that you don't replace an empty array with
                // an empty array and cause a rerender
                if (!(state.uses[storeKey].selectedTags[selectedTab].length === 0 && tags.length === 0)) {
                    state.uses[storeKey].selectedTags[selectedTab] = tags
                }

                state.uses[storeKey].selectedTags.status = "succeeded"
            }
        })
        builder.addCase(saveSelectedRowsAndGetTags.rejected, (state, action) => {

            const {id: selectedTab, storeKey} = action.meta.arg

            if (selectedTab !== null && isKeyOf(state.uses, storeKey) && isKeyOf(state.uses[storeKey].selectedResults, selectedTab)) {
                state.uses[storeKey].selectedTags.status = "failed"
                // Set the tags to empty to avoid showing the wrong info for the selected rows
                state.uses[storeKey].selectedTags[selectedTab] = []
            }

            const text = {
                title: "Failed to update application",
                message: `The call to save the selected rows and get the metadata did not complete successfully.`,
                details: action.error.message
            }

            showToast("error", text, "findInTrac/saveSelectedRowsAndGetTags/rejected")
            console.error(action.error)
        })
    }
})

// Action creators are generated for each case reducer function
export const {
    addManyObjectIdSearchResultsToTable,
    addOneObjectIdSearchResultToTable,
    setAttributeObjectTypeFilter,
    getTableState,
    setBusinessSegment,
    setCreateOrUpdate,
    setDateRangeFilter,
    setDayOrMonth,
    setJobFilter,
    setManyObjectIdSearchValue,
    setModelFilter,
    setObjectIdObjectTypeFilter,
    setObjectIdSearchStatus,
    setShowInSearchResultsFilter,
    setTab,
    setUserFilter
} = FindInTracStore.actions

export default FindInTracStore.reducer