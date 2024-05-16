/**
 * This slice acts as the root store for the application holding data that is not specific to an individual scene
 * in the application. For example, it holds the user set parameters that do not apply to a single scene such as
 * the tenant in use. This store also holds metadata that has been requested from the API, storing the
 * metadata in a Redux store means that we reduce the number of API calls if it is requested again.
 * @module
 */

import {Application, CodeRepositories, Images, ManagementTools, Option} from "../../types/types_general";
import {createAsyncThunk, createSlice, PayloadAction} from '@reduxjs/toolkit';
import {createUniqueObjectKey} from "../utils/utils_trac_metadata";
import {General} from "../../config/config_general";
import {getCookie, setCookie} from "../utils/utils_general";
import {hasOwnProperty, isDefined} from "../utils/utils_trac_type_chckers";
import {metadataBatchRequest, metadataReadRequest} from "../utils/utils_trac_api";
import {RootState} from "../../storeController";
import {tracdap as trac} from "@finos/tracdap-web-api";

/**
 * An interface for the applicationStore.ts Redux store.
 */
export interface ApplicationStoreState {
    /**
     * Whether the side menu should be shown (open) or hidden (closed). There is a listener set in {@link App} to
     * check if the user clicks outside the side menu which will close the menu.
     */
    sideMenu: {
        show: boolean
    }
    /**
     * Information about the user, including when their JWT authorization expires. This is provided by TRAC cookies.
     */
    login: {
        userName: string
        userId: string
        expiryDatetime: string | null
    }
    /**
     * Cookie values set by TRAC-UI, these allow use settings to carry across sessions. The language and theme
     * cookies have default values set in client-config.json. These are set by selecting options in the
     * {@link SettingsModal}.
     */
    cookies: {
        "trac-language": string
        "trac-theme": string
        "trac-tenant"?: string
    },
    /**
     * The datetime to use in TRAC API requests, this is used in time travel mode. This is set in {@link SettingsModal}.
     */
    tracApi: {
        searchAsOf: trac.metadata.IDatetimeValue | null
    },
    /**
     * A set of options for the users who have saved items in TRAC. The list is provided by the getUsers TRAC API
     * endpoint which is called by the {@link OnLoad} component.
     */
    userOptions: Option<string>[]
    /**
     * A set of options for the tenants available for the current user. The list is provided by the listTenants TRAC API
     * endpoint which is called by the {@link OnLoad} component.
     */
    tenantOptions: Option<string>[]
    /**
     * A store of metadata responses from the TRAC API. The key for each metadata record needs to be unique across object ID,
     * object version and tag version.
     */
    metadataStore: Record<string, trac.metadata.ITag>
    /**
     * A set of options for the tenants available for the current user, the information is provided by the getPlatformInfo
     * TRAC API endpoint which is called by the {@link OnLoad} component.
     */
    platformInfo: trac.api.IPlatformInfoResponse
    /**
     * Information about the client specific config for TRAC-UI. The client-config.json file is in the public/static folder
     * and loaded from outside the TRAC-UI bundle by the {@link OnLoad} component before the information is added to the
     * applicationStore for use by the application. It contains config that the client may want to change without having to
     * rebuild the application source code.
     */
    clientConfig: {
        application: Application,
        codeRepositories: CodeRepositories,
        images: Images,
        options: {
            onlyAllowCookiesOnSsl: boolean
        },
        externalLinks: {
            modelCode: { label: string, application: null | ManagementTools, url: null | string }
            uiCode: { label: string, application: null | ManagementTools, url: null | string }
            tracCode: { label: string, application: null | ManagementTools, url: null | string }
            projectDocumentation: { label: string, application: null | ManagementTools, url: null | string },
            projectIssues: { label: string, application: null | ManagementTools, url: null | string }
        },
        searches: {
            /**
             * The limit of the results to return, this is set when the search results are being converted into options, and you want to
             * limit the number that come back due to performance issues.
             */
            maximumNumberOfOptionsReturned: number
        },
        /**
         * Maximum file sizes for uploads to TRAC and settings for guessing the variable types and
         * showing data loads.
         */
        uploading: {
            data: {
                sizeLimit: Record<"default" | string, number>,
                processing: {
                    maximumRowsToUseToGuessSchema: number,
                    maximumRowsToShowInSample: number,
                    messageSize: number
                }
            },
            file: {
                sizeLimit: Record<"default" | string, number>
            }
        }
    }
}

// This is the initial state of the store
const initialState: ApplicationStoreState = {

    sideMenu: {
        show: false
    },
    login: {
        userName: "Unknown",
        userId: "Unknown",
        expiryDatetime: null
    },
    cookies: {
        "trac-language": getCookie("trac-language") || General.defaultLanguage,
        "trac-theme": getCookie("trac-theme") || General.defaultTheme,
        "trac-tenant": getCookie("trac-tenant")
    },
    tracApi: {
        searchAsOf: null
    },
    userOptions: [],
    tenantOptions: [],
    platformInfo: {},
    metadataStore: {},
    clientConfig: {
        application: {
            name: "", tagline: "", maskColour: "purple", webpageTitle: "TRAC data & analytics platform"
        },
        codeRepositories: [],
        uploading: {
            data: {
                sizeLimit: {
                    default: 20971520,
                    // 53687091200 = 1024 * 1024 * 1024 * 50 (50Gb)
                    csv: 53687091200,
                    xlsx: 20971520
                },
                processing: {
                    maximumRowsToUseToGuessSchema: 1000000,
                    maximumRowsToShowInSample: 1000,
                    messageSize: 60000
                }
            },
            file: {
                sizeLimit: {
                    default: 20971520
                }
            }
        },
        images: {
            client: {
                darkBackground: {
                    src: "",
                    naturalWidth: 246,
                    naturalHeight: 66,
                    displayWidth: 200,
                    displayHeight: 57,
                    alt: "Accenture",
                    style: {
                        marginBottom: "2rem"
                    }
                },
                lightBackground: {
                    src: "",
                    naturalWidth: 246,
                    naturalHeight: 66,
                    displayWidth: 200,
                    displayHeight: 57,
                    alt: "Accenture",
                    style: {
                        marginBottom: "2rem"
                    }
                }
            },
            application: {
                darkBackground: {
                    src: "",
                    naturalWidth: 280,
                    naturalHeight: 280,
                    displayWidth: 90,
                    displayHeight: 90,
                    alt: "TRAC logo",
                    style: {
                        marginTop: "0.5rem"
                    }
                },
                lightBackground: {
                    src: "",
                    naturalWidth: 280,
                    naturalHeight: 280,
                    displayWidth: 30,
                    displayHeight: 30,
                    alt: "TRAC logo",
                    style: {
                        marginTop: "0.5rem"
                    }
                }
            }
        },
        options: {
            onlyAllowCookiesOnSsl: false
        },
        "searches": {
            "maximumNumberOfOptionsReturned": 1
        },
        "externalLinks": {
            "modelCode": {
                "label": "model code",
                "application": "github",
                "url": null
            },
            "uiCode": {
                "label": "UI code",
                "application": null,
                "url": null
            },
            "tracCode": {
                "label": "TRAC code",
                "application": "github",
                "url": "https://github.com/finos/tracdap"
            },
            "projectDocumentation": {
                "label": "TRAC docs",
                "application": "confluence",
                "url": "https://tracdap.finos.org/en/stable/"
            },
            "projectIssues": {
                "label": "issues/tickets",
                "application": null,
                "url": null
            }
        }
    }
}

/**
 * A function that gets the metadata for an object from the metadata store if it exists or makes the API call is needed.
 * If an API call is needed the metadata is added to the store in case it is requested again. There aren't
 * pending/fulfilled/failed lifecycle methods as this function is unwrapped in the getMetadata util function, so they
 * are never executed.
 */
export const checkForMetadata = createAsyncThunk<// Return type of the payload creator
    trac.metadata.ITag,
    // First argument to the payload creator
    trac.metadata.ITagSelector,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('applicationStore/checkForMetadata', async (tagSelector, {getState, dispatch}) => {

    // Get what we need from the store
    const {metadataStore, cookies: {"trac-tenant": tenant}} = getState().applicationStore

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")

    // This is not the TRAC object key, this has tag version included and does not support 'latest'
    const objectKey = createUniqueObjectKey(tagSelector)

    // Don't use or set content in the store if version numbers are not set
    // This is because if you want the 'latest' of one of the versions then the
    // key will include 'undefined' which is an invalid key - so we don't want
    // to use the store in this case.
    const useStore = !(tagSelector.latestTag === true || tagSelector.latestObject == true || tagSelector.objectAsOf != null || tagSelector.tagAsOf != null)

    if (useStore && hasOwnProperty(metadataStore, objectKey)) {

        console.log("LOG :: Object found in metadata store")
        return metadataStore[objectKey]

    } else {

        if (useStore) {
            console.log("LOG :: Object not found in metadata store")
        } else {
            console.log("LOG :: Not using metadata store, request included special items")
        }

        const metadataResult = await metadataReadRequest({tagSelector, tenant})

        // Put the new metadata into the store
        if (useStore) {
            dispatch(addToMetadataStore(metadataResult))
        }

        return metadataResult
    }
})

/**
 * A function that gets the metadata for an array of objects from the metadata store if it exists or makes the API call
 * is needed. If an API call is needed the metadata is added to the store. There aren't pending/fulfilled/failed
 * lifecycle methods as this function is unwrapped in the getBatchMetadata util function, so they are never executed.
 */
export const checkForBatchMetadata = createAsyncThunk<// Return type of the payload creator
    trac.metadata.ITag[],
    // First argument to the payload creator
    trac.metadata.ITagHeader[],
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('applicationStore/checkForBatchMetadata', async (tagHeaders, {getState, dispatch}) => {

    // Get what we need from the store
    const {metadataStore, cookies: {"trac-tenant": tenant}} = getState().applicationStore

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")

    // This is an array listing the indices of items that are not in the metadata store, we need to do this
    // because we need to return the array of results in the same order as requested
    let indicesOfMissingItems: number[] = []

    // This is the array that we are going to return to the user
    const results: (null | trac.metadata.ITag)[] = tagHeaders.map((tagHeader, i) => {

        const objectKey = createUniqueObjectKey(tagHeader)

        // If the object metadata already exists we put that in the array
        if (hasOwnProperty(metadataStore, objectKey)) {

            return metadataStore[objectKey]

        } else {

            // Add the index to the list of what's missing
            indicesOfMissingItems.push(i)
            // Put the null in the array
            return null
        }
    })

    // Show a message - this might not be needed in production, but it is useful to be able to see if the cache is
    // working
    if (indicesOfMissingItems.length !== 0 && indicesOfMissingItems.length < tagHeaders.length) {
        console.log(`LOG :: ${tagHeaders.length - indicesOfMissingItems.length}/${tagHeaders.length} objects found in metadata store`)
    } else {
        console.log(`LOG :: All objects${indicesOfMissingItems.length === 0 ? " " : " not "}found in metadata store`)
    }

    // Make a batch request for the missing items
    const tags: trac.metadata.ITagHeader[] = tagHeaders.filter((tagHeader, i) => indicesOfMissingItems.includes(i))

    // TRAC will error if the array of requests is empty
    if (tags.length > 0) {

        // Make the call for the missing results
        const metadataResults = await metadataBatchRequest({tenant, tags})

        // Put the new metadata into the store
        dispatch(addToMetadataStore(metadataResults.tag))

        // Put the new results into the array to return
        metadataResults.tag.forEach((item, i) => {
            results[indicesOfMissingItems[i]] = item
        })
    }

    // We filter the nulls although there should not be any in order to make Typescript happy
    return results.filter(isDefined)
})

export const applicationStore = createSlice({
    name: 'applicationStore',
    initialState: initialState,
    reducers: {
        /**
         * A reducer that shows or hides the side menu, this can either be used to toggle the state
         * or to set it ti a particular value.
         */
        toggleSideMenu: (state, action: PayloadAction<boolean | undefined>) => {

            const show = action.payload

            // If a value for show is explicitly set use that, otherwise toggle
            if ((typeof show === "boolean" && state.sideMenu.show !== show) || typeof show !== "boolean") {

                state.sideMenu.show = typeof show === "boolean" ? show : !state.sideMenu.show
            }
        },
        /**
         * A reducer that sets the information about who has logged in to use the application. The information
         * is shown in the TopMenu component and is alo used in exports to include who exported the file.
         */
        setLogin: (state) => {

            state.login.userId = getCookie("trac_user_id") || "unknown"
            state.login.userName = (getCookie("trac_user_name") || "unknown").replace("+", " ")
            state.login.expiryDatetime = getCookie("trac_session_expiry_utc") || null
        },
        /**
         * A reducer that sets the tenant options that the user can select, these come from TRAC via an API call.
         */
        setTenantOptions: (state, action: PayloadAction<Option<string>[]>) => {

            state.tenantOptions = action.payload
        },
        /**
         * A reducer that sets the platform information, this is shown in the about modal and come from TRAC via an API call.
         */
        setPlatformInfo: (state, action: PayloadAction<trac.api.IPlatformInfoResponse>) => {

            state.platformInfo = action.payload
        },
        /**
         * A reducer that sets a cookie for the selected tenant and saves the value in the store. The src/storeController.ts
         * file contains logic that running this also resets the state of the entire application.
         */
        setTenantSetting: (state, action: PayloadAction<string>) => {

            const {onlyAllowCookiesOnSsl} = state.clientConfig.options

            // Clear the metadata store before we change the searchAsOf, the store won't reflect the tenant
            applicationStore.caseReducers.resetMetadataStore(state)
            state.cookies["trac-tenant"] = action.payload
            setCookie("trac-tenant", action.payload, onlyAllowCookiesOnSsl)
        },
        /**
         * A reducer that sets a cookie for the language setting, themes and
         * tenants need their own functions due to the specific types or functionality needed.
         */
        setLanguageSetting: (state, action: PayloadAction<string>) => {

            const {onlyAllowCookiesOnSsl} = state.clientConfig.options

            state.cookies["trac-language"] = action.payload
            setCookie("trac-language", action.payload, onlyAllowCookiesOnSsl)
        },
        /**
         * A reducer that sets a cookie for the theme setting, because this has specific types set a separate function is
         * used to reduce the need for type checking functions.
         */
        setThemeSetting: (state, action: PayloadAction<string>) => {

            const {onlyAllowCookiesOnSsl} = state.clientConfig.options

            state.cookies["trac-theme"] = action.payload
            setCookie("trac-theme", action.payload, onlyAllowCookiesOnSsl)
        },
        /**
         * A reducer that sets the time travel mode datetime, this then becomes the searchAsOf date (or similar TRAC
         * properties) in TRAC API calls.The src/storeController.ts file contains logic that running this also resets
         * the state of the entire application.
         */
        setSearchAsOf: (state, action: PayloadAction<string | undefined>) => {

            // Clear the metadata store before we change the searchAsOf, the store won't reflect the position at the new datetime
            applicationStore.caseReducers.resetMetadataStore(state)
            state.tracApi.searchAsOf = action.payload !== undefined ? {isoDatetime: action.payload} : null
        },
        // TODO Add documentation and types checking
        setClientConfig: (state, action) => {

            // TODO We should do deep merges and set the defaults from the config local file
            console.log(action.payload)
            // @ts-ignore
            state.clientConfig.codeRepositories = action.payload.codeRepositories
            state.clientConfig.images = action.payload.images
            state.clientConfig.application = action.payload.application
            state.clientConfig.options = action.payload.options
            state.clientConfig.externalLinks = action.payload.externalLinks
            state.clientConfig.searches = action.payload.searches
            if (action.payload.hasOwnProperty("uploading")) state.clientConfig.uploading = action.payload.uploading
            document.title = action.payload.application.webpageTitle
        },
        /**
         * A reducer that sets an array of options for users who have created items in TRAC, used for enabling user ID
         * searches in the {@link FindInTrac} component.
         */
        setUserNames: (state, action: PayloadAction<Option<string>[]>) => {

            state.userOptions = action.payload
        },
        /**
         * A reducer that adds metadata to the metadata store, this is an area is used as a cache of object metadata.
         */
        addToMetadataStore: (state, action: PayloadAction<trac.metadata.ITag | trac.metadata.ITag[]>) => {

            // Add thew new metadata to the store
            if (!Array.isArray(action.payload) && action.payload.header) {

                const objectKey = createUniqueObjectKey(action.payload.header)
                state.metadataStore[objectKey] = action.payload

            } else if (Array.isArray(action.payload)) {

                action.payload.forEach(item => {

                    if (item.header) {
                        const objectKey = createUniqueObjectKey(item.header)
                        state.metadataStore[objectKey] = item
                    }
                })
            }
        },
        /**
         * A reducer that removes all information from the metadata store, used when the user changes tenant or searchAsOf.
         */
        resetMetadataStore : (state) => {
            state.metadataStore = {}
        }
    }
})

// Action creators are generated for each case reducer function
export const {
    addToMetadataStore,
    resetMetadataStore,
    setClientConfig,
    setLogin,
    setLanguageSetting,
    setPlatformInfo,
    setSearchAsOf,
    setTenantOptions,
    setTenantSetting,
    setThemeSetting,
    setUserNames,
    toggleSideMenu
} = applicationStore.actions

export default applicationStore.reducer