/**
 * This slice acts as the store for the {@link UploadFromGitHub} component.
 * @module uploadFromGitHubStore
 * @category Redux store
 */

import type {
    ArrayElement,
    ButtonPayload,
    CodeRepositories,
    CommitDetails,
    FileTree,
    FolderTree,
    GitHubMethods,
    GitRequestInit,
    Group,
    Option,
    RepoDetails,
    SelectedFileDetails,
    SelectOptionPayload,
    SelectValuePayload,
    StoreStatus
} from "../../../../types/types_general";
import {type ExtractedTracValue, FileSystemInfo} from "../../../../types/types_general";
import {
    calculateModelEntryPointAndPath,
    calculatePathsAndPackages,
    commasAndAnds,
    enrichApiCodes,
    enrichErrorMessages,
    getFileDetails,
    getFileExtension,
    getModelRepositories,
    showToast
} from "../../../utils/utils_general";
import {checkForMetadata} from "../../../store/applicationStore";
import {checkUrlEndsRight, convertStringToSchema, getAllRegexMatches} from "../../../utils/utils_string";
import {convertArrayToOptions, sortArrayBy} from "../../../utils/utils_arrays";
import {convertIsoDateStringToFormatCode} from "../../../utils/utils_formats";
import {createAsyncThunk, createSlice, type PayloadAction} from '@reduxjs/toolkit';
import {createTagsFromAttributes} from "../../../utils/utils_attributes_and_parameters";
import {type Endpoints} from "@octokit/types";
import {hasOwnProperty, isDefined, isKeyOf} from "../../../utils/utils_trac_type_chckers";
import {importModel, importSchema, isModelLoaded, isObjectLoadedByLocalFileDetails} from "../../../utils/utils_trac_api";
import {resetAttributesToDefaults, setAttributesAutomatically} from "../../SetAttributes/setAttributesStore";
import type {RootState} from '../../../../storeController';
import type {SingleValue} from "react-select";
import {toast} from "react-toastify";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {wait} from "../../../utils/utils_async";

/**
 * An interface for the 'user' entry point in the store state.
 */
export interface User {
    status: StoreStatus
    details: null | Endpoints["GET /user"]["response"]["data"]
    message: undefined | string
}

/**
 * A function that initialises the 'user' entry point in the store state, having it in a separate function means we can
 * use it to reset parts of the state later on.
 */
function setupUser(): User {

    return {
        status: "idle",
        details: null,
        message: undefined
    }
}

/**
 * An interface for the 'authorisation' entry point in the store state.
 */
export interface Authorisation {
    userName: {
        value: string | null
        isValid: boolean
    }
    token: {
        value: string | null
        isValid: boolean
    }
    validationChecked: boolean
}

/**
 * A function that initialises the 'authorisation' entry point in the store state, having it in a separate function means we can
 * use it to reset parts of the state later on.
 */
function setupAuthorisation(): Authorisation {

    return {
        userName: {value: null, isValid: false},
        token: {value: null, isValid: false},
        validationChecked: false
    }
}

/**
 * An interface for the 'repository' entry point in the store state.
 */
export interface Repository {

    status: StoreStatus
    message: undefined | string
    details: null | Endpoints["GET /repos/{owner}/{repo}"]["response"]["data"]
    contributors: Endpoints["GET /repos/{owner}/{repo}/contributors"]["response"]["data"]
    branches: {
        options: Option<string>[]
        selectedOption: SingleValue<Option<string>>
    },
    pulls: Endpoints["GET /repos/{owner}/{repo}/pulls"]["response"]["data"]
    modelFileExtensions: undefined | string[]
    tracConfigName: undefined | string
}

/**
 * A function that initialises the 'repository' entry point in the store state, having it in a separate function means we can
 * use it to reset parts of the state later on.
 */
function setupRepository(): Repository {

    return {
        status: "idle",
        message: undefined,
        details: null,
        contributors: [],
        branches: {
            options: [],
            selectedOption: null
        },
        pulls: [],
        modelFileExtensions: [],
        tracConfigName: undefined
    }
}

/**
 * An interface for the 'repositories' entry point in the store state.
 */
export interface Repositories {

    status: StoreStatus
    message: undefined | string
    options: Group<string, RepoDetails>[]
    selectedOption: SingleValue<Option<string, RepoDetails>>
}

/**
 * A function that initialises the 'repositories' entry point in the store state, having it in a separate function means we can
 * use it to reset parts of the state later on.
 */
function setupRepositories(): Repositories {

    return {
        status: "idle",
        message: undefined,
        options: [],
        selectedOption: null
    }
}

/**
 * An interface for the 'branch' entry point in the store state.
 */
export interface Branch {
    status: StoreStatus
    message: undefined | string
    commit: {
        options: Group<string, CommitDetails>[],
        selectedOption: null | Option<string, CommitDetails>,
    }
}

/**
 * A function that initialises the 'branch' entry point in the store state, having it in a separate function means we can
 * use it to reset parts of the state later on.
 */
function setupBranch(): Branch {
    return {
        status: "idle",
        message: undefined,
        commit: {
            options: [],
            selectedOption: null,
        }
    }
}

/**
 * An interface for the 'tree' entry point in the store state.
 */
export interface Tree {
    status: StoreStatus
    message: undefined | string
    fileTree: null | FileTree
    folderTree: null | FolderTree
    selectedOption: null | SelectedFileDetails
    showUnselectableFiles: boolean
    treeHasSelectableItem: boolean
    treeHasUnselectableItem: boolean
}

/**
 * A function that initialises the 'tree' entry point in the store state, having it in a separate function means we can
 * use it to reset parts of the state later on.
 */
function setupTree(): Tree {
    return {
        status: "idle",
        message: undefined,
        fileTree: null,
        folderTree: null,
        selectedOption: null,
        showUnselectableFiles: false,
        treeHasSelectableItem: false,
        treeHasUnselectableItem: false
    }
}

/**
 * An interface for the 'file' entry point in the store state.
 */
export interface File {
    status: StoreStatus
    message: undefined | string
    details: null | Endpoints["GET /repos/{owner}/{repo}/contents/{path}"]["response"]["data"]
    fileNameWithoutExtension: null | string
    metadata: { hasMetadata: boolean, hasValidMetadata: boolean, json: null | Record<string, any>, filename: null | string, url: null | string }
    path: trac.metadata.IImportModelJob["path"]
    alreadyInTrac: {
        foundInTrac: boolean
        // We get the Tag when loading a model and a TagHeader when loading a schema
        // but make it look like we have a tag
        tag: null | trac.metadata.ITag
    }
    // Depending on the use case we have object specific information for the file
    model: {
        entryPoint: trac.metadata.IImportModelJob["entryPoint"]
        tracModelClassOptions: Option<string>[]
        selectedTracModelClassOption: SingleValue<Option<string>>
    }
    schema: {
        fields: null | trac.metadata.IFieldSchema[]
        errorMessages: string[]
    }
}

/**
 * A function that initialises the 'file' entry point in the store state, having it in a separate function means we can
 * use it to reset parts of the state later on.
 */
function setupFile(): File {
    return {
        status: "idle",
        message: undefined,
        details: null,
        fileNameWithoutExtension: null,
        path: null,
        metadata: {
            json: null,
            hasMetadata: false,
            filename: null,
            hasValidMetadata: false,
            url: null
        },
        alreadyInTrac: {foundInTrac: false, tag: null},
        model: {
            entryPoint: null,
            tracModelClassOptions: [],
            selectedTracModelClassOption: null
        },
        schema: {
            fields: null,
            errorMessages: []
        }
    }
}

/**
 * An interface for the 'upload' entry point in the store state.
 */
export interface Upload {
    status: StoreStatus,
    message: undefined,
}

/**
 * A function that initialises the 'upload' entry point in the store state, having it in a separate function means we can
 * use it to reset parts of the state later on.
 */
function setupUpload(): Upload {
    return {
        status: "idle",
        message: undefined
    }
}

/**
 * An interface for the uploadFromGitHubStore Redux store.
 */
export interface UploadFromGitHubStoreState {
    uses: {
        uploadAModel: {
            authorisation: Authorisation
            user: User
            repositories: Repositories
            repository: Repository
            branch: Branch
            tree: Tree
            file: File
            upload: Upload
        },
        uploadASchema: {
            authorisation: Authorisation
            user: User
            repositories: Repositories
            repository: Repository
            branch: Branch
            tree: Tree
            file: File
            upload: Upload
        }
    }
}

// This is the initial state of the store. If you want to add a new user add it in here
// and also in the UploadFromGitHubStoreState definition. You will also need to add in whatever
// additional logic you need to the reducer functions (typically starting with the 'getFile'
// function usually).
const initialState: UploadFromGitHubStoreState = {
    uses: {
        uploadAModel: {
            authorisation: setupAuthorisation(),
            user: setupUser(),
            repositories: setupRepositories(),
            repository: setupRepository(),
            branch: setupBranch(),
            tree: setupTree(),
            file: setupFile(),
            upload: setupUpload()
        },
        uploadASchema: {
            authorisation: setupAuthorisation(),
            user: setupUser(),
            repositories: setupRepositories(),
            repository: setupRepository(),
            branch: setupBranch(),
            tree: setupTree(),
            file: setupFile(),
            upload: setupUpload()
        }
    }
}

/**
 * A function that sets the RequestInit object for the GitHub fetch requests. This includes the authorization
 * information provided. This is needed for all API requests in order to authenticate with the API.
 *
 * @param payload - The username and token.
 * @param body - The body for POST and PATCH requests.
 * @param method - The method e.g. POST.
 * @returns A GitHub request initialisation object.
 */
const getGitRequestInit = (payload: Pick<Authorisation, "userName" | "token">, body: {} = {}, method: GitHubMethods = "GET"): GitRequestInit => {

    if (payload.userName.value == null) throw new TypeError("The user name value is null")
    if (payload.token.value == null) throw new TypeError("The token value is null")

    let init: GitRequestInit = {
        method: method,
        headers: {
            'Accept': 'application/vnd.github.v3+json',
            'Content-Type': 'application/json',
            'User-Agent': payload.userName.value,
            'Authorization': `Bearer ${payload.token.value}`
        }
    }

    if (method !== "GET") {
        init.body = JSON.stringify(body)
    }

    return init
}

/**
 * A function that sends an API request to GitHub. It can handle appending multiple requests together if the number
 * of results exceeds the limit per request.
 *
 * @param codeRepositories - The details of the code repositories, including the model repositories the application has been configured to use.
 * @param getAll - Whether to get all the results by appending multiple requests together.
 * @param init - The authentication information from the getGitHeaders function.
 * @param perPage - The maximum number of results to get from the API.
 * @param parameters - Additional query parameters to add to the URL.
 * @param tenant - The tenant that the user is using, used to identify the model repository to make calls to.
 * @param url - The url to send the request to.
 */
export const getGitResource = async <T>(codeRepositories: CodeRepositories, tenant: string, init: GitRequestInit, url: string, getAll: boolean = false, perPage: number = 100, parameters: string[] = []): Promise<T> => {

    // The Git API only lets us get a max of 100 results per API call, so we are going to have to collect all the
    // results via multiple calls. We are going to collect them into the results array. It's likely that
    // there will never be more than 100 results for most types of calls, but we are being safe.

    // The aggregated array of results from the GitHUb API
    let results = null

    // Git has a maximum of 100 results, so we max at that
    const finalPerPage = Math.min(100, perPage)

    // The number of results in the request. When this hits less than the perPage value we know that we need to stop the
    // requests. Defaults to 100 per request if perPage is not set
    let i = finalPerPage;

    // The page of results to request
    let page = 1

    // Check the current tenant has model repositories defined in the config
    if (getModelRepositories(codeRepositories, tenant).length === 0) {
        throw new Error(`There are no GitHub repository models defined for the ${tenant} tenant in the application config`)
    }

    // The user could have multiple GitHub repositories that are for a single tenant, however they are not allowed to
    // have different URLs, if there are more than one value in gitHubApiUrls then we don't know which API URL to use.
    // API URLS should be unique for an Enterprise or GitHub installation.
    // See https://stackoverflow.com/questions/36503800/whats-my-github-appliances-rest-api-endpoint
    const gitHubApiUrls = [...new Set(codeRepositories.filter(repo => repo.tenants.includes(tenant) && repo.type === "gitHub").map(repo => repo.apiUrl))].filter(isDefined)

    if (gitHubApiUrls.length > 1) {
        throw new Error(`There multiple GitHub URLs defined for the ${tenant}, only a single endpoint can be defined per tenant. The following endpoints were found: ${commasAndAnds(gitHubApiUrls)}.`)
    }

    const apiUrl = gitHubApiUrls?.[0]

    // Check the current tenant has a gitHub API endpoint defined
    if (apiUrl === undefined) {
        throw new Error(`There is no GitHub API endpoint defined for the ${tenant} tenant in the application config`)
    }

    // Set the full URL for the request
    let endPoint = `${checkUrlEndsRight(apiUrl)}${url}`

    // The loop continues until all the results are aggregated into one list
    do {

        // Work out what query parameters to add to the URL
        let query = ""

        if (getAll) {
            query = `?per_page=${finalPerPage}&page=${page}`
        } else {
            // Get the first page only
            query = `?per_page=${finalPerPage}`
        }

        if (parameters.length > 0) {
            query = `${query === "" ? "?" : query}&${parameters.join('&')}`
        }

        // Make the actual API call
        let response = await fetch(`${endPoint}${query}`, init)

        if (!response.ok) {
            const errorBody = await response.json()
            const message = enrichApiCodes(response.status) || (errorBody && errorBody.hasOwnProperty("message") && errorBody.message)
            throw new Error(message);
        }

        // Extract the result
        const result = await response.json()

        // Add the repository list to our collection
        results = Array.isArray(results) && Array.isArray(result) ? results.concat(result) : result
        // The -1 is to set i to the exit condition for requests that don't return an array
        i = Array.isArray(result) && getAll ? result.length : -1;
        page++
    }

        // Keep on going until we find the last page
    while (Boolean(i === finalPerPage))

    return results as T
}

/**
 * A function that runs when the user clicks to log in to GitHub. It authenticates the user and then
 * gets a list of repositories for the organisation defined in the Config.
 */
export const gitLogin = createAsyncThunk<// Return type of the payload creator
    { userDetails: User["details"], storeKey: keyof UploadFromGitHubStoreState["uses"] },
    // First argument to the payload creator
    { storeKey: keyof UploadFromGitHubStoreState["uses"] },
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('uploadFromGitHubStore/gitLogin', async (payload, {getState, dispatch}) => {

    // The user of the store
    const {storeKey} = payload

    // Get the parameters we need from the store
    const authorisation = getState().uploadFromGitHubStore.uses[storeKey].authorisation
    const {cookies: {"trac-tenant": tenant}} = getState().applicationStore
    const {codeRepositories} = getState().applicationStore.clientConfig

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")

    // First generate the request init
    const init = getGitRequestInit(authorisation)

    // Set the url for user login, this returns the user associated with the token
    let url = `user`

    // Make the GitHub API call
    const userDetails: Endpoints["GET /user"]["response"]["data"] = await getGitResource(codeRepositories, tenant, init, url)

    // If the token is not for the given login username then we error
    if (userDetails.login !== authorisation.userName.value) {
        throw new Error("The token is not registered to that username")
    }

    // Get the repository list if we authenticate
    if (getModelRepositories(codeRepositories, tenant).length > 0) dispatch(getGitRepositories({storeKey}))

    // This payload is passed to the fulfilled hook.
    return {storeKey, userDetails}
})

/**
 * A function that runs when the user successfully logs onto GitHub. It uses the authentication details of the user to
 * get a list of repositories for the organisations defined in the Config.
 */
export const getGitRepositories = createAsyncThunk<// Return type of the payload creator
    { repositories?: Repositories["options"], storeKey: keyof UploadFromGitHubStoreState["uses"] },
    // First argument to the payload creator
    { storeKey: keyof UploadFromGitHubStoreState["uses"] } | ButtonPayload,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('uploadFromGitHubStore/getGitRepositories', async (payload, {getState}) => {

    // To get the user for the store we have to cope with the two different payloads, one where this function is called
    // from the gitLogin function and one where the user clicks on a button to refresh the repositories list
    const storeKey = hasOwnProperty(payload, "storeKey") ? payload.storeKey : payload.name

    // We need to make sure that when clicking a button to refresh the repositories that the name string is a user in the store
    if (typeof storeKey !== "string" || !isKeyOf(getState().uploadFromGitHubStore.uses, storeKey)) {
        throw new Error(`Store user '${storeKey}' is not set or is not a valid user of the uploadFromGitHub store`)
    }

    // Get the parameters we need from the store
    const authorisation = getState().uploadFromGitHubStore.uses[storeKey].authorisation
    const {cookies: {"trac-tenant": tenant}} = getState().applicationStore
    const {codeRepositories} = getState().applicationStore.clientConfig

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")

    // First generate the request init
    const init = getGitRequestInit(authorisation)

    // Get a list of all the unique owners listed in the config, these are either organisations or owners
    const owners = [...new Set(getModelRepositories(codeRepositories, tenant).map(repo => repo.owner))]

    // Get the repositories for each organisation and process them into a set of options, then add them to a set of
    // grouped options
    let repositoryGroups: Repositories["options"] = await Promise.all(owners.map(async owner => {

        // The names of the model repositories that are valid for the user to select
        const organisationRepositories = getModelRepositories(codeRepositories, tenant).filter(repo => repo.owner === owner)

        // The names of the model repositories that are valid for the user to select
        const validRepositoryNames = organisationRepositories.map(repo => repo.name)

        // The owner types of the model repository owners, this can be either "organisation" or "user", so the max
        // length of this array is two, however if there is actually a mix of values one set will always fail. It should
        // always be just one ownerType per organisation.
        const ownerTypes = [...new Set(organisationRepositories.map(repo => repo.ownerType))]

        if (ownerTypes.length > 1) {
            throw new Error(`The model repositories for ${owner} are set as ${commasAndAnds(ownerTypes)} in the config, they can only be set as one type.`)
        }

        // The URL to get the list of repos for the model repository's organisation
        const url = ownerTypes[0] === "organisation" ? `orgs/${owner}/repos` : `users/${owner}/repos`

        // Make the GitHub API call to get the list of repositories under the organisation
        let repositories: Endpoints["GET /orgs/{org}/repos"]["response"]["data"] | Endpoints["GET /users/{username}/repos"]["response"]["data"] = await getGitResource(codeRepositories, tenant, init, url, true, 100, [`type=all`])

        // Now convert to an array of options
        let repositoryOptions: Option<string, RepoDetails>[] = repositories.map((repo) => ({
            value: repo.name,
            label: `${repo["full_name"]} (${repo.private ? "private" : "public"})`,
            disabled: !validRepositoryNames.includes(repo.name),
            details: {
                owner: {login: repo.owner.login, html_url: repo.owner.html_url},
                defaultBranch: repo.default_branch
            }
        }))

        // Sort the repositories by their display name
        repositoryOptions = sortArrayBy(repositoryOptions, "label")

        // Put the allowed options at the top
        repositoryOptions = [...repositoryOptions.filter((repo) => !repo.disabled), ...repositoryOptions.filter((repo) => repo.disabled)]


        return {label: owner, options: repositoryOptions}
    }))

    // This payload is passed to the fulfilled hook.
    return {repositories: repositoryGroups, storeKey}
})

/**
 * A function that runs when the user changes their selected GitHub repository. It updates the store and gets the
 * information about the repository.
 */
export const getRepository = createAsyncThunk<// Return type of the payload creator
    {
        branches?: Option<string>[],
        contributors?: Repository["contributors"],
        modelFileExtensions?: undefined | string[],
        pulls?: Repository["pulls"],
        repositoryDetails?: Repository["details"],
        storeKey: keyof UploadFromGitHubStoreState["uses"],
        tracConfigName?: undefined | string
    },
    // First argument to the payload creator
    { storeKey: keyof UploadFromGitHubStoreState["uses"] } | ButtonPayload,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('uploadFromGitHubStore/getRepository', async (payload, {getState}) => {

    let url

    // To get the user for the store we have to cope with the two different payloads, one where this function is called
    // from the gitLogin function and one where the user clicks on a button to refresh the repositories list
    const storeKey = hasOwnProperty(payload, "storeKey") ? payload.storeKey : payload.name

    // We need to make sure that when clicking a button to refresh the repositories that the name string is a user in the store
    if (typeof storeKey !== "string" || !isKeyOf(getState().uploadFromGitHubStore.uses, storeKey)) {
        throw new Error(`Store user '${storeKey}' is not set or is not a valid user of the uploadFromGitHub store`)
    }

    // Get the parameters we need from the store
    const {authorisation, repositories: {selectedOption}} = getState().uploadFromGitHubStore.uses[storeKey]
    const {cookies: {"trac-tenant": tenant}} = getState().applicationStore
    const {production} = getState().applicationStore.platformInfo
    const {codeRepositories} = getState().applicationStore.clientConfig

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")

    // First generate the request init
    const init = getGitRequestInit(authorisation)

    // If there is no option do nothing
    if (selectedOption == null) return {storeKey}

    // Get the repository info if an option is set
    const {details, value: repoName} = selectedOption

    // Get the information about the repository
    url = `repos/${details.owner.login}/${repoName}`
    const repositoryDetails: Endpoints["GET /repos/{owner}/{repo}"]["response"]["data"] = await getGitResource(codeRepositories, tenant, init, url)

    // Get the information about the repository contributors
    url = `repos/${details.owner.login}/${repoName}/contributors`
    const contributors: Endpoints["GET /repos/{owner}/{repo}/contributors"]["response"]["data"] = await getGitResource(codeRepositories, tenant, init, url, true)

    // Get the information about the repository branches
    url = `repos/${details.owner.login}/${repoName}/branches`
    const branches: Endpoints["GET /repos/{owner}/{repo}/branches"]["response"]["data"] = await getGitResource(codeRepositories, tenant, init, url, true)

    // Now convert to an array of option
    let branchOptions: Option<string>[] = branches.map((branch) => ({
        value: branch.name,
        label: `${branch.name} (${branch.protected ? "protected" : "unprotected"})`,
        disabled: Boolean(production === true && details.defaultBranch !== branch.name)
    }))

    // Sort the branches by name
    branchOptions = sortArrayBy(branchOptions, "label")

    // Put the allowed options at the top
    branchOptions = [...branchOptions.filter((branch) => !branch.disabled), ...branchOptions.filter((branch) => branch.disabled)]

    // Get the information about the repository pull requests
    url = `repos/${details.owner.login}/${repoName}/pulls`
    const pulls: Endpoints["GET /repos/{owner}/{repo}/pulls"]["response"]["data"] = await getGitResource(codeRepositories, tenant, init, url, true)

    // Get the model repo info about the metadata file names and their extensions
    const {
        modelFileExtensions,
        tracConfigName
    } = (getModelRepositories(codeRepositories, tenant).find(repo => repo.owner === details.owner.login && selectedOption !== null && selectedOption.value === repo.name) || {})

    // This payload is passed to the fulfilled hook.
    return {
        branches: branchOptions,
        contributors,
        modelFileExtensions,
        pulls,
        repositoryDetails,
        storeKey,
        tracConfigName
    }
})

/**
 * A function that runs when the user changes their selected repository branch. It updates the store and gets the
 * information about the branch.
 */
export const getBranch = createAsyncThunk<// Return type of the payload creator
    { options?: Group<string, CommitDetails>[], storeKey: keyof UploadFromGitHubStoreState["uses"] },
    // First argument to the payload creator
    { storeKey: keyof UploadFromGitHubStoreState["uses"] } | ButtonPayload,
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('uploadFromGitHubStore/getBranch', async (payload, {getState}) => {

    let url

    // To get the user for the store we have to cope with the two different payloads, one where this function is called
    // from the gitLogin function and one where the user clicks on a button to refresh the repositories list
    const storeKey = hasOwnProperty(payload, "storeKey") ? payload.storeKey : payload.name

    // We need to make sure that when clicking a button to refresh the repositories that the name string is a user in the store
    if (typeof storeKey !== "string" || !isKeyOf(getState().uploadFromGitHubStore.uses, storeKey)) {
        throw new Error(`Store user '${storeKey}' is not set or is not a valid user of the uploadFromGitHub store`)
    }

    // Get the parameters we need from the store
    const {
        authorisation,
        repository: {details, branches: {selectedOption}}
    } = getState().uploadFromGitHubStore.uses[storeKey]

    const {cookies: {"trac-tenant": tenant}} = getState().applicationStore
    const {codeRepositories} = getState().applicationStore.clientConfig

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")

    // If there is no option do nothing
    if (selectedOption == null) return {storeKey}

    if (details == null) throw new Error("The repository details that you are getting the branches for is null, this is not allowed")

    // Get the branch info if an option is set
    const {value: branchName} = selectedOption

    // First generate the request init
    const init = getGitRequestInit(authorisation)

    // Get the information about the commits
    url = `repos/${details.owner.login}/${details.name}/commits`

    // The perPage = 1 gets only the latest commit
    const commits: Endpoints["GET /repos/{owner}/{repo}/commits"]["response"]["data"] = await getGitResource(codeRepositories, tenant, init, url, false, 1, [`sha=${branchName}`]) || []

    // Get the information about the tags
    url = `repos/${details.owner.login}/${details.name}/tags`
    const tags: Endpoints["GET /repos/{owner}/{repo}/tags"]["response"]["data"] = await getGitResource(codeRepositories, tenant, init, url, true, 100, [`sha=${branchName}`]) || []

    // Get the information about the releases
    url = `repos/${details.owner.login}/${details.name}/releases`
    const releases: Endpoints["GET /repos/{owner}/{repo}/releases"]["response"]["data"] = await getGitResource(codeRepositories, tenant, init, url, true, 100, [`sha=${branchName}`]) || []

    // Releases do not have a commit SHA that we can use to reference the commit. However, all releases have a tag
    // associated with them from which we can get the SHA. So what we do here is match the releases to their tag
    const releasesWithSha: (ArrayElement<typeof releases> & { sha: undefined | string })[] = releases.map((release) => {

        const tag = tags.find((tag) => release["tag_name"] === tag.name)
        return {...release, sha: tag?.commit.sha}
    })

    // Construct the list of commits as a set of options with headers. We add the commit/release/tag value
    // to the value to avoid warnings about repeated keys and to ensure each option is unique
    const options: Group<string, CommitDetails>[] = [
        {
            label: "Latest commit",
            options: commits.map((item) => ({
                value: `${item.commit.tree.sha}_commit`,
                label: `${item.commit.committer?.date ? convertIsoDateStringToFormatCode(item.commit.committer.date, "DATETIME") : "Unknown"} (${item.committer?.login || "Unknown"})`,
                details: {commit: {...item, identifier: "commit"}, treeSha: item.commit.tree.sha}
            }))
        },
        {
            label: "Releases",
            options: releasesWithSha.map((item) => ({
                value: `${item.sha}_release`,
                label: `${item.name} (${item.published_at ? convertIsoDateStringToFormatCode(item.published_at, "DATETIME") : "Unknown"})`,
                details: {commit: {...item, identifier: "release"}, treeSha: item.sha}
            }))
        }
    ];

    // This payload is passed to the fulfilled hook.
    return {options, storeKey}
})

/**
 * A function that runs when the user changes the branch commit. It updates the store and gets the
 * information about the tree corresponding to the commit.
 */
export const getTree = createAsyncThunk<// Return type of the payload creator
    {
        fileTree?: FileTree
        folderTree?: FolderTree
        storeKey: keyof UploadFromGitHubStoreState["uses"]
        treeHasSelectableItem?: boolean
        treeHasUnselectableItem?: boolean
    },
    // First argument to the payload creator
    { storeKey: keyof UploadFromGitHubStoreState["uses"] },
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('uploadFromGitHubStore/getTree', async (payload, {getState}) => {

    const {storeKey} = payload

    let url

    // Get the parameters we need from the store
    const {
        authorisation,
        repositories,
        repository,
        branch: {commit: {selectedOption}}
    } = getState().uploadFromGitHubStore.uses[storeKey]

    const {cookies: {"trac-tenant": tenant}} = getState().applicationStore
    const {codeRepositories} = getState().applicationStore.clientConfig

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")

    // If there is no option for the tree to get do nothing
    if (selectedOption == null) return {storeKey}

    if (repository.details == null) throw new Error("The repository details that you are getting the tree for is null, this is not allowed")

    if (selectedOption.details.treeSha === undefined) throw new Error("The repository tree SHA is null, this is not allowed")

    const {treeSha} = selectedOption.details

    // First generate the request init
    const init = getGitRequestInit(authorisation)

    // Get the information about the commit tree
    url = `repos/${repository.details.owner.login}/${repository.details.name}/git/trees/${treeSha}`
    const fileTree: Endpoints["GET /repos/{owner}/{repo}/git/trees/{tree_sha}"]["response"]["data"] = await getGitResource(codeRepositories, tenant, init, url, false, 100, ["recursive=1"])

    // Now we have to do a large amount of processing on the tree

    // The paths to folders in the tree grouped by how deep we are in the tree hierarchy
    const pathsByLength: Record<number, (string[])[]> = {}

    // We have two ways to view the file tree, one with everything showing and one where we hide anything
    // other than the likely files that the user would want to upload. To allow for this in one object structure
    // we tag the files that won't show if the user has the 'hide' view turned on.

    // It looks like organisation can set stored in two locations depending on what type of repository tree the SHA is
    // for (standard vs release for example)
    const organisation = repository.details.organization?.login || repository.details.owner.login

    if (repositories.selectedOption == null) throw new TypeError("The selected repository that you are getting the tree for is null, this is not allowed")

    let selectableFiles: string[] = []

    // If we are loading a model then the file extensions to allow the user to load are defined in the
    // repo definitions
    if (storeKey === "uploadAModel") {

        // Get a list of the file types that can be loaded from this tree
        selectableFiles = getModelRepositories(codeRepositories, tenant).find(repo => repo.owner === organisation && repositories.selectedOption && repositories.selectedOption.value === repo.name)?.modelFileExtensions || []

    } else if (storeKey === "uploadASchema") {

        // We only allow schemas to be loaded by csv
        selectableFiles = ["csv"]
    }

    // The tree from the gitHub API comes back as a flat array of items with a path to the file. We
    // want to create a file tree like in windows, so we need to recast it to an object of objects.
    // The function below does this.

    // These are used for messaging on case there are no items - we don't want to show a blank tree.
    let treeHasSelectableItem = false
    let treeHasUnselectableItem = false

    // This is the first main object to create, the file tree to pass to the FileTree component
    const restructuredFileTree: FileTree = {};

    // This is the second main object to create, the folder tree to pass to the FileTree component
    const restructuredFolderTree: FolderTree = {};

    // The branch of the tree we are processing
    let treeBranch: FileTree

    fileTree.tree.forEach((item) => {

        // Split the path to the file up and remove blank parts
        const pathAsArray = item.path?.split('/').filter(pathPart => pathPart !== "") ?? []

        // Some paths will end in a file, but we want to store each unique path without
        // the file. We want to use these paths without files to work out if the folder
        // should be shown (because all the files in it are unselectable/hidden). Start by taking a
        // copy of the path as an array
        const pathAsArrayWithoutFile = [...pathAsArray]

        // If the last item is a file then remove it
        if (item.type !== "tree") pathAsArrayWithoutFile.pop()

        // We are going to store the paths without any files in an array keyed into an object
        // by the number of steps. Property keys can only be strings.
        const pathLengthWithoutFile = pathAsArrayWithoutFile.length

        // Add the path to the right key of path lengths in pathsByLength. This is used to establish if folders
        // have no files that are not unselectable/hidden - and in which case we should hide from the user
        if (!pathsByLength.hasOwnProperty(pathLengthWithoutFile)) {

            pathsByLength[pathLengthWithoutFile] = []
        }

        // Is the path already stored
        const foundPathAlready = pathsByLength[pathLengthWithoutFile].some((pathAsArray) => pathAsArray.join("/") === pathAsArrayWithoutFile.join("/"))

        // Store the path without the file in the array in an object keyed by their length
        if (!foundPathAlready) pathsByLength[pathLengthWithoutFile].push(pathAsArrayWithoutFile)

        // Take a copy, notice that this is not a deep copy, so we are mutating restructuredFileTree here also
        treeBranch = restructuredFileTree;

        // No we are back to the full paths that can include files at the end. For each step
        // in the path if the folder is not already in the object of objects then we add it in
        pathAsArray.forEach((pathPart, i) => {

            if (!(pathPart in treeBranch)) {

                // Add a node for the folder
                treeBranch[pathPart] = {};

                // If we are at the end of the path and the item are not a tree (a folder) then
                // add the file information into the object
                if (i === pathAsArray.length - 1 && item.type !== "tree") {

                    const fileName = pathAsArray[pathAsArray.length - 1]
                    const fileExtension = getFileExtension(fileName)

                    treeBranch[pathPart] = {
                        ...item, ...{
                            fileExtension: fileExtension,
                            selectable: false,
                            location: pathAsArrayWithoutFile.join('/')
                        }
                    }

                    // Set a property to say if this file should be shown, if no file filters are set allow all
                    if (selectableFiles.length === 0 || (selectableFiles.includes(fileExtension) && fileName !== "__init__.py")) {

                        treeBranch[pathPart].selectable = true
                        treeHasSelectableItem = true

                    } else {

                        treeBranch[pathPart].selectable = false
                        treeHasUnselectableItem = true
                    }

                } else {

                    // Otherwise, add the folder information - this is treated as if it is a file in the folder
                    // because folderInfo is a legitimate name of a folder in the repo we add a random string to
                    // it to avoid clashes. The use of this random string is a weakness, as there will be bug if the
                    // tree contains a folder with this is the actual name.
                    restructuredFolderTree[pathAsArrayWithoutFile.join('/')] = {...item, selectable: false}
                }
            }

            // Move down the folders within the branch...
            // The treeBranch variable is originally of type FileTree
            // but its properties have type FileTreeNode. This means that as we move down the branch the type changes.
            // The recursion works but typescript can't handle the change in type. The typescript below resets the type.
            treeBranch = treeBranch[pathPart] as FileTree;
        })
    })

    // Now back to the object containing the paths without files at the end, keyed by the path lengths. We need to
    // iterate backwards through the tree. This is the trick - to work out if a folder should be hidden because there
    // is nothing in there any further down the tree that needs to be shown - we need to check the ends of the branches
    // and then work backwards to the start.

    // Start by sorting the path lengths in reverse order
    const reversedPathLengths = Object.keys(pathsByLength).map(key => parseInt(key)).sort(function (a, b) {
        return -b - a;
    })

    // For each path length found (the longest first)
    reversedPathLengths.forEach(pathLength => {

        // Get the array of paths of that length
        pathsByLength[pathLength].forEach((pathAsArray) => {

            // Get the tree object at the end of the path
            const objectFromFileTree: FileTree = pathAsArray.reduce((prev: any, path) => prev && prev[path], restructuredFileTree)

            // Get the tree object at the end of the path, note that it is keyed by the full path
            const objectFromFolderTree = restructuredFolderTree[pathAsArray.join('/')]

            // If it is a folder. do the files in the folder at the end have any items that should be shown. Or if
            // there is a folder in the folder does that folder contain anything in it that should be shown.
            const anythingSelectable = objectFromFolderTree && (objectFromFolderTree.selectable || Object.keys(objectFromFileTree).some(key => {

                // Now if a folder only has folders in it the objectFromFileTree won't have a selectable flag for the folder (as that only stores this
                // for files). So we need to check if the sub item is a folder and if so get that folder's selectable statue
                const subFolderPath = pathAsArray.concat(key).join('/')
                return restructuredFolderTree[subFolderPath]?.selectable || objectFromFileTree[key].selectable
            }))

            // Now store the information for this folder
            if (objectFromFolderTree) objectFromFolderTree.selectable = anythingSelectable

        })
    })

    // This payload is passed to the fulfilled hook.
    return {
        fileTree: restructuredFileTree,
        folderTree: restructuredFolderTree,
        storeKey,
        treeHasSelectableItem,
        treeHasUnselectableItem
    }
})

/**
 * A function that runs when the user changes the selected file from a commit. It updates the store and gets the
 * file from the commit.
 */
export const getFile = createAsyncThunk<// Return type of the payload creator
    { file?: Omit<File, "status" | "message" | "schema" | "model"> & { schema?: File["schema"] } & { model?: File["model"] }, storeKey: keyof UploadFromGitHubStoreState["uses"] },
    // First argument to the payload creator
    { selectedFileDetails: Tree["selectedOption"], storeKey: keyof UploadFromGitHubStoreState["uses"] },
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('uploadFromGitHubStore/getFile', async (payload, {dispatch, getState}) => {

    const {selectedFileDetails, storeKey} = payload

    let url, init

    // Put the selected file into the store
    dispatch(setFile(payload))

    // Get the parameters we need from the store
    const {
        authorisation,
        repository,
        repositories,
        repository: {details},
        branch: {commit},
        tree: {fileTree}
    } = getState().uploadFromGitHubStore.uses[storeKey]

    const {cookies: {"trac-tenant": tenant}, tracApi: {searchAsOf}} = getState().applicationStore
    const {codeRepositories} = getState().applicationStore.clientConfig

    // If the user has not set a tenant then the API call can not be made, similarly other missing items cause errors
    if (tenant === undefined) throw new Error("tenant is undefined")
    if (commit.selectedOption == null) throw new Error("The selected branch option that you are getting the file for is null, this is not allowed")
    if (details == null) throw new Error("The repository details that you are getting the file for is null, this is not allowed")
    if (fileTree == null) throw new Error("The file tree that you are getting the file from is null, this is not allowed")

    // The payload will be null if the user deselects a file and then closes the FileTree component
    if (selectedFileDetails == null || selectedFileDetails.path == null) return {storeKey}

    // Get the file from the repo, we use the 'contents' endpoint rather than the 'blobs' endpoint as it has the html_url in the
    // response which we use to provide links in the UI to see the file.
    init = getGitRequestInit(authorisation)

    // Get the information about the file
    url = `repos/${details.owner.login}/${details.name}/contents/${selectedFileDetails.path}`

    // The sha for the commit is in a different place depending on whether we load a commit or a tag, the latter
    // means we have to go to commit.sha, which should be in the same for both as we added it in when we mapped 'releasesWithSha'
    const sha = commit.selectedOption.details.commit.sha
    const fileDetails: Endpoints["GET /repos/{owner}/{repo}/contents/{path}"]["response"]["data"] = await getGitResource(codeRepositories, tenant, init, url, false, 1, [`ref=${sha}`])

    // Decode the file
    const fileContent = !Array.isArray(fileDetails) && "encoding" in fileDetails ? (fileDetails.encoding === "base64" ? Buffer.from(fileDetails.content, 'base64').toString() : fileDetails.content) : undefined

    if (fileContent == null) return {storeKey}

    // Convert the file path into a whole range of pieces of info
    const fileInfo = getFileDetails(selectedFileDetails.path)

    // This is the definition for the file that we are going to return, it has different properties depending on what
    // type of file we are getting from the repository.
    let newFile: undefined | Omit<File, "status" | "message" | "schema" | "model"> & { schema?: File["schema"] } & { model?: File["model"] } = undefined

    // When loading a csv schema file from GitHub the file details have the path and file name and the size. However,
    // we don't have when the file was actually last modified. When loading a dataset from a local csv we get all of these details.
    // We want to mirror the information from loading a dataset, so we need to get the last modified date. We get this by getting
    // a list of commits that modified the schema file in question and take the first one as the latest, and get the date of the
    // commit. Note we can not take the date of the commit the user loaded the file from as that might not be when it was last
    // modified.
    url = url = `repos/${details.owner.login}/${details.name}/commits`
    const fileCommits: Endpoints["GET /repos/{owner}/{repo}/commits"]["response"]["data"] = await getGitResource(codeRepositories, tenant, init, url, false, 1, [`path=${selectedFileDetails.path}`]) || []

    if (storeKey === "uploadAModel") {

        // First search the code to get all the classes that have the TRAC model API as an argument. It would be unlikely that
        // a single file would have two or more TRAC models defined, but it is possible, so we check for multiple classes.
        // Here we are looking for the string 'class <class name>(trac.TracModel)' and extracting the <class name>. If more than
        // one class is found we ask the user to select which class to load as the model entrypoint.
        const allTracModelClasses = getAllRegexMatches(fileContent, /class\s+([a-zA-Z0-9_-]+)\s*\(\s*trac.TracModel|tracdap.TracModel/g)

        // Create a set of options for the classes we found.
        const tracModelClassOptions = convertArrayToOptions(allTracModelClasses, false)

        // Set the default option for model class to add to the endpoint.
        const selectedTracModelClassOption = tracModelClassOptions.length > 0 ? tracModelClassOptions[0] : null

        // So now we need to work out the path to the file and the entry point to specify in the TRAC
        // API call to load the model.
        const pathAndPackages = calculatePathsAndPackages(fileTree, fileInfo.folderPathAsArray)

        // Calculate the endpoint and the path for the model, this is needed to upload the model, it tells TRAC how to execute the model
        const entryPointAndPath = calculateModelEntryPointAndPath(pathAndPackages, fileInfo.fileNameWithoutExtension, selectedTracModelClassOption?.value)

        // This is used to see if there is a model already loaded with the same details
        const modelDetails: Pick<trac.metadata.IImportModelJob, "repository" | "path" | "entryPoint" | "version"> = {
            repository: repository.tracConfigName,
            path: entryPointAndPath?.path,
            entryPoint: entryPointAndPath?.entryPoint,
            version: commit.selectedOption.details.commit.sha,
        }

        // If we can not get the information we need to check if the model has already been loaded, or then we need to skip
        // doing the searches
        const fileDetailsMissing = Object.values(modelDetails).includes(null)

        // If we skip the searches we will still need to provide the variables into the store, so they are initialised here.
        // as a blank array.
        let modelSearchResults: trac.metadata.ITag[] = []

        // Search to see if the model is already in TRAC
        if (!fileDetailsMissing) {

            // See if the model is already in TRAC, we check the same file and model class in the same location in the same commit.
            // If it is there then we will not allow the model to be loaded again. The commit is optional, if set to false
            // then you can not reload the file twice.
            modelSearchResults = await isModelLoaded({modelDetails, searchAsOf, searchByCommit: true, tenant})
        }

        // This is the model file info minus the metadata information
        newFile = {
            details: fileDetails,
            fileNameWithoutExtension: fileInfo.fileNameWithoutExtension,
            path: modelDetails.path,
            metadata: {
                hasMetadata: false,
                hasValidMetadata: false,
                filename: null,
                url: null,
                json: null
            },
            alreadyInTrac: {
                foundInTrac: Boolean(modelSearchResults.length > 0),
                tag: modelSearchResults?.[0] ?? null
            },
            model: {
                entryPoint: modelDetails.entryPoint,
                tracModelClassOptions,
                selectedTracModelClassOption,
            }
        }

    } else if (storeKey === "uploadASchema") {

        const {errorMessages, fields} = convertStringToSchema(fileContent)

        // Now we have all the information we need, we can check if the schema already exists in TRAC
        // First spoof an object to look like what the schema's file info would look like if loaded
        // from a local file.
        const fileSystemInfo: Pick<FileSystemInfo, "sizeInBytes" | "fileName" | "lastModifiedTracDate"> = {
            sizeInBytes: typeof selectedFileDetails.size === "number" ? selectedFileDetails.size : 0,
            fileName: fileInfo.fileNameWithExtension,
            lastModifiedTracDate: fileCommits?.[0].commit.committer?.date ?? ""
        }

        // If we do not have the information we need to check if the schema has already been loaded, then we need to skip
        // doing the searches
        const fileDetailsMissing = Object.values(fileSystemInfo).includes(0) || Object.values(fileSystemInfo).includes("")

        // If we skip the searches we will still need to provide the variables into the store, so they are initialised here.
        // as a blank array.
        let schemaSearchResults: trac.metadata.ITag[] = []

        // Search to see if the schema is already in TRAC
        if (!fileDetailsMissing) {

            // See if the model is already in TRAC, we check the same file and model class in the same location in the same commit.
            // If it is there then we will not allow the model to be loaded again
            schemaSearchResults = await isObjectLoadedByLocalFileDetails({
                fileDetails: fileSystemInfo,
                objectType: trac.metadata.ObjectType.SCHEMA,
                searchAsOf,
                tenant
            })
        }

        // This is the schema file info minus the metadata information
        newFile = {
            details: fileDetails,
            path: selectedFileDetails.path,
            fileNameWithoutExtension: fileInfo.fileNameWithoutExtension,
            metadata: {
                hasMetadata: false,
                hasValidMetadata: false,
                filename: null,
                url: null,
                json: null,
            },
            alreadyInTrac: {
                foundInTrac: Boolean(schemaSearchResults.length > 0),
                tag: schemaSearchResults?.[0] ?? null
            },
            schema: {
                errorMessages,
                fields
            }
        }
    }

    // Now we are able to search to see if there is any metadata for the model stored in the repo. If we don't find this
    // then the user has to define the key and the name and the description etc. in the UI themselves. The config for
    // the model repo defines an expected file name for the metadata.

    // The organisation or owner of the repository
    const organisation = details.organization?.login || details.owner.login

    // The expected name of the metadata file
    const {
        modelMetadataName,
        modelMetadataExtension
    } = getModelRepositories(codeRepositories, tenant).find(repo => repo.owner === organisation && repositories.selectedOption && repositories.selectedOption.value === repo.name) || {}

    // If there is no metadata filename defined or the file can not be decoded then we will still need to provide the
    // variables into the store, so they are initialised here.

    // hasValidMetadata is whether the metadata file was decoded without error
    let hasValidMetadata: boolean = false

    // metadataInfo is the information about the metadata file from the downloaded tree
    let metadataFileFromTree: File["details"] = null

    // metadataInfo is the API information about the metadata file
    let metadataDetails: File["details"] = null

    // Expected name of the metadata file if it exists
    let metadataFilename = modelMetadataExtension !== null && modelMetadataExtension !== fileInfo.fileExtension ? `${fileInfo.fileNameWithoutExtension}${modelMetadataName !== null ? modelMetadataName : ""}.${modelMetadataExtension}` : null

    // This is the metadata file from the repo, we initialise it as an empty object in case there is not one or one that
    // can not be decoded
    let metadataJson: null | Record<string, any> = null

    // Since we are selecting a new model file we need to reset the metadata attributes in the UI, this information
    // is owned by the setAttributesStore
    dispatch(resetAttributesToDefaults({storeKey}))

    // So if a metadata filename is defined we look for the file in the repo
    if (metadataFilename === null) return {storeKey}

    // Use the path of the metadataFilename file to see if one actually exists in the file tree or not. We expect that the file will be in
    // the same folder as the model itself, reduce is really cool for doing this.
    metadataFileFromTree = [...fileInfo.folderPathAsArray, metadataFilename].reduce((prev: any, path) => prev && prev[path], fileTree)

    // If the repo contains a metadata file, and its is an object (a single file rather than an array), and has the "encoding" property so relates to a file rather than a folder or symlink
    if (metadataFileFromTree && !Array.isArray(metadataFileFromTree) && "path" in metadataFileFromTree) {

        init = getGitRequestInit(authorisation)

        // Get the metadata file from the repo
        url = `repos/${details.owner.login}/${details.name}/contents/${metadataFileFromTree.path}`
        metadataDetails = await getGitResource(codeRepositories, tenant, init, url, false, 1, [`ref=${commit.selectedOption.details.commit.sha}`])

        // If the repo contains a metadata file, and its is an object (a single file rather than an array), and has the "encoding" property so relates to a file rather than a folder or symlink
        if (metadataDetails && !Array.isArray(metadataDetails) && "encoding" in metadataDetails) {

            // Decode file replace the encoded version, it might be invalid JSON
            try {

                metadataJson = JSON.parse(metadataDetails.encoding === "base64" ? Buffer.from(metadataDetails.content, 'base64').toString() : metadataDetails.content)
                hasValidMetadata = true

            } catch {

                metadataJson = null
                hasValidMetadata = false
            }
        }

        // If there is valid metadata in the Git repository then we can try and use this to set some
        // attributes from this. However, we need to validate that the supplied keys and their values are
        // valid. We don't allow dates to be set this way, basically just being lazy.
        if (metadataJson) {

            // We can pass the JSON object to the setAttributesStore to use as the metadata attributes for the
            // model
            dispatch(setAttributesAutomatically({storeKey, values: metadataJson}))
        }

        // When we are loading a model from GitHub TRAC will add information such as the location and hash of the
        // commit, we just need to add that the model was loaded by the UI. For a schema we add information as if it
        // was a file.
        let objectSpecificAttributes: Record<string, ExtractedTracValue> = {}

        if (storeKey === "uploadASchema") {

            // These are the additional attributes we are going to add to the loaded schema
            objectSpecificAttributes = {
                import_method: ["user_interface"],
                original_file_size: selectedFileDetails.size || null,
                original_file_name: fileInfo.fileNameWithExtension,
                original_file_modified_date: fileCommits?.[0].commit.committer?.date ?? null
            }
        }

        // Set the hidden information about the schema
        dispatch(setAttributesAutomatically({
                storeKey, values: {
                    import_method: ["user_interface"],
                    ...objectSpecificAttributes
                }
            }
        ))
    }

    // Add in the metadata for all use cases
    if (newFile !== undefined) {
        newFile.metadata = {
            hasMetadata: Boolean(metadataFileFromTree),
            hasValidMetadata: hasValidMetadata,
            filename: metadataFilename,
            url: metadataDetails !== null && !Array.isArray(metadataDetails) && "html_url" in metadataDetails ? metadataDetails?.html_url || null : null,
            json: metadataJson,
        }
    }

    return {file: newFile, storeKey}
})

/**
 * A function that makes an API call to TRAC to upload a model from a Git repository.
 */
export const uploadModelToTrac = createAsyncThunk<// Return type of the payload creator
    { jobStatus: trac.api.JobStatus, alreadyInTrac: File["alreadyInTrac"], storeKey: keyof UploadFromGitHubStoreState["uses"] },
    // First argument to the payload creator
    { storeKey: keyof UploadFromGitHubStoreState["uses"] },
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('uploadFromGitHubStore/uploadModelToTrac', async (payload, {getState}) => {

    const {storeKey} = payload

    // Get the parameters we need from the store
    const {
        branch: {commit},
        file,
        repository
    } = getState().uploadFromGitHubStore.uses[storeKey]

    const {cookies: {"trac-tenant": tenant}} = getState().applicationStore

    const {processedAttributes, values} = getState().setAttributesStore.uses[storeKey].attributes

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")
    if (commit.selectedOption == null) throw new Error("The selected commit details that you are getting the file from is null, this is not allowed")
    if (repository.tracConfigName == null) throw new Error("The selected repository does not have a TRAC name set, this means that the TRAC API does not know what repository to load from. Please add the tracConfigName property to the application config (src/config/config_general.ts). The matching names to use are set in the TRAC config (etc/trac-platform.yaml)")
    if (file.path == null) throw new Error("The selected file's path is null, this is not allowed")
    if (file.model.entryPoint == null) throw new Error("The selected file's entryPoint is null, this is not allowed")

    const modelAttrs = createTagsFromAttributes(processedAttributes, values)

    const importDetails: trac.metadata.IImportModelJob = {

        language: "python",
        repository: repository.tracConfigName,
        path: file.path,
        entryPoint: file.model.entryPoint,
        version: commit.selectedOption.details.commit.sha,
        modelAttrs
    }

    const jobStatus = await importModel({importDetails, tenant})

    // Pause for 5 seconds, what we are trying to do here as a bit of a hack is give the system enough time to load the
    // model. This means that we can then check to see if it is loaded and the UI will respond by not allowing the
    // same model from the same commit to be loaded. However, there is no guarantee that the time delay will be enough.
    // The delay does do a good job at preventing accidental button spamming.
    await wait(5000)

    // Search for the model in TRAC to see if it found, note we search as of when it was loaded and ignore if time travel mode is on
    const modelSearchResults = await isModelLoaded({modelDetails: importDetails, searchAsOf: null, searchByCommit: true, tenant})

    return {
        alreadyInTrac: {
            foundInTrac: Boolean(Array.isArray(modelSearchResults) && modelSearchResults.length > 0),
            tag: modelSearchResults?.[0] ?? null
        },
        jobStatus,
        storeKey
    }
})

/**
 * A function that makes an API call to TRAC to upload a schema from a Git repository.
 */
export const uploadSchemaToTrac = createAsyncThunk<// Return type of the payload creator
    { alreadyInTrac: File["alreadyInTrac"], storeKey: keyof UploadFromGitHubStoreState["uses"] },
    // First argument to the payload creator
    { storeKey: keyof UploadFromGitHubStoreState["uses"] },
    // Optional fields for defining thunkApi field types, getState type is inferred from state
    {
        state: RootState
    }>('uploadFromGitHubStore/uploadSchemaToTrac', async (payload, {dispatch, getState}) => {
    const {storeKey} = payload

    // Get the parameters we need from the store
    const {file} = getState().uploadFromGitHubStore.uses[storeKey]

    const {cookies: {"trac-tenant": tenant}} = getState().applicationStore

    const {processedAttributes, values} = getState().setAttributesStore.uses[storeKey].attributes

    // If the user has not set a tenant then the API call can not be made
    if (tenant === undefined) throw new Error("tenant is undefined")
    if (file.path == null) throw new Error("The selected file's path is null, this is not allowed")
    if (file.schema.fields == null) throw new Error("The schema is null, this is not allowed")

    const schemaAttrs = createTagsFromAttributes(processedAttributes, values)

    const tagHeader = await importSchema({attrs: schemaAttrs, fields: file.schema.fields, tenant})

    // Get the full metadata tag for the new schema, this is only really done so that we have a full tag in both
    // the model and schema loading
    const tag = await dispatch(checkForMetadata(tagHeader)).unwrap()

    // Since we now know that the data is loaded we block the same dataset being loaded again
    return {
        alreadyInTrac: {
            foundInTrac: true,
            tag
        },
        storeKey
    }
})

export const uploadFromGitHubStore = createSlice({
    name: 'uploadFromGitHubStore',
    initialState: initialState,
    reducers: {
        /**
         * A reducer that updates the userName and token information entered by the user.
         */
        setLoginDetails: (state, action: PayloadAction<SelectValuePayload>) => {

            const {id, isValid, name: storeKey, value} = action.payload

            // The type is a general SelectValue component payload, so we need to confirm that it meets the
            // expected type
            if (isKeyOf(state.uses, storeKey) && (id === "userName" || id === "token") && typeof value != "number") {

                state.uses[storeKey].authorisation[id] = {value, isValid}
                // Hide any validation messages when the credentials are changed
                state.uses[storeKey].authorisation.validationChecked = false
                // Hide any login authorization error messages
                state.uses[storeKey].user.message = undefined
            }
        },
        /**
         * A reducer that shows/hides the validation messages about the login values e.g. warning about the username being blank.
         */
        toggleValidationMessages: (state, action: PayloadAction<{ storeKey: keyof UploadFromGitHubStoreState["uses"], value: boolean }>) => {

            const {storeKey, value} = action.payload

            state.uses[storeKey].authorisation.validationChecked = value
        },
        /**
         * A reducer that sets the selected repository option.
         */
        setRepository: (state, action: PayloadAction<SelectOptionPayload<Option<string, RepoDetails>, false>>) => {

            const {name: storeKey, value} = action.payload

            if (isKeyOf(state.uses, storeKey)) {
                state.uses[storeKey].repositories.selectedOption = value
            }
        },
        /**
         * A reducer that sets the selected branch option.
         */
        setBranch: (state, action: PayloadAction<SelectOptionPayload<Option<string>, false>>) => {

            const {name: storeKey, value} = action.payload

            if (isKeyOf(state.uses, storeKey)) {
                state.uses[storeKey].repository.branches.selectedOption = value
            }
        },
        /**
         * A reducer that sets the selected commit option.
         */
        setCommit: (state, action: PayloadAction<SelectOptionPayload<Option<string, CommitDetails>, false>>) => {

            const {name: storeKey, value} = action.payload

            if (isKeyOf(state.uses, storeKey)) {
                state.uses[storeKey].branch.commit.selectedOption = value
            }
        },
        /**
         * A reducer that sets whether to show or hide the unselectable files from the folder tree.
         */
        toggleShowUnselectableFiles: (state, action: PayloadAction<{ storeKey: keyof UploadFromGitHubStoreState["uses"] }>) => {

            const {storeKey} = action.payload

            state.uses[storeKey].tree.showUnselectableFiles = !state.uses[storeKey].tree.showUnselectableFiles
        },
        /**
         * A reducer that sets the selected file to load.
         */
        setFile: (state, action: PayloadAction<{ selectedFileDetails: Tree["selectedOption"], storeKey: keyof UploadFromGitHubStoreState["uses"] }>) => {

            const {selectedFileDetails, storeKey} = action.payload

            state.uses[storeKey].tree.selectedOption = selectedFileDetails
        },
        /**
         * A reducer that sets the selected model class to use in the endpoint to load the model. This is used when
         * there are multiple classes and one option needs to be set.
         */
        setTracModelClass: (state, action: PayloadAction<SelectOptionPayload<Option<string>, false>>) => {

            const {name: storeKey, value} = action.payload

            // The type is a general SelectOption component payload, so we need to confirm that it meets the
            // expected type
            if (isKeyOf(state.uses, storeKey)) {

                state.uses[storeKey].file.model.selectedTracModelClassOption = value

                const {fileNameWithoutExtension, path} = state.uses[storeKey].file
                const {fileTree} = state.uses[storeKey].tree

                if (value !== null && fileNameWithoutExtension !== null && fileTree !== null && path != null) {
                    const fileInfo = getFileDetails(path)
                    const pathAndPackages = calculatePathsAndPackages(fileTree, fileInfo.folderPathAsArray)
                    state.uses[storeKey].file.model.entryPoint = calculateModelEntryPointAndPath(pathAndPackages, fileNameWithoutExtension, value.value).entryPoint
                } else {
                    state.uses[storeKey].file.model.entryPoint = null
                }
            }
        },
        /**
         * A reducer that logs the user out and resets the state of the component.
         */
        logout: (state, action: PayloadAction<ButtonPayload>) => {

            const {name: storeKey,} = action.payload

            // The type is a general Button component payload, so we need to confirm that it meets the
            // expected type
            if (isKeyOf(state.uses, storeKey)) {

                state.uses[storeKey] = {
                    authorisation: setupAuthorisation(),
                    user: setupUser(),
                    repositories: setupRepositories(),
                    repository: setupRepository(),
                    branch: setupBranch(),
                    tree: setupTree(),
                    file: setupFile(),
                    upload: setupUpload()
                }
            }
        }
    },
    extraReducers: (builder) => {

        // A set of lifecycle reducers to run before/after the gitLogin function
        builder.addCase(gitLogin.pending, (state, action) => {

            const {storeKey} = action.meta.arg

            // Clear all the messages
            toast.dismiss()
            state.uses[storeKey].user.status = "pending"
            state.uses[storeKey].user.message = undefined
        })
        builder.addCase(gitLogin.fulfilled, (state, action: PayloadAction<{ userDetails: User["details"], storeKey: keyof UploadFromGitHubStoreState["uses"] }>) => {

            const {storeKey, userDetails} = action.payload

            state.uses[storeKey].user.status = "succeeded"
            state.uses[storeKey].user.message = undefined
            state.uses[storeKey].user.details = userDetails
        })
        builder.addCase(gitLogin.rejected, (state, action) => {

            const {storeKey} = action.meta.arg

            state.uses[storeKey].user.status = "failed"
            state.uses[storeKey].user.message = enrichErrorMessages(action.error.message)
            // No toast as we show the message in the login box
        })

        // A set of lifecycle reducers to run before/after the getGitRepositories function
        builder.addCase(getGitRepositories.pending, (state, action) => {

            const storeKey = hasOwnProperty(action.meta.arg, "storeKey") ? action.meta.arg.storeKey : action.meta.arg.name

            if (typeof storeKey === "string" && isKeyOf(state.uses, storeKey)) {

                // Clear all the messages
                toast.dismiss()
                state.uses[storeKey].repositories.status = "pending"
                state.uses[storeKey].repositories.message = undefined
                // Reset the upload, file, tree, branch and repository properties of the store as these are downstream of the repositories
                state.uses[storeKey].repository = setupRepository()
                state.uses[storeKey].branch = setupBranch()
                state.uses[storeKey].tree = setupTree()
                state.uses[storeKey].file = setupFile()
                state.uses[storeKey].upload = setupUpload()
                // Remove any existing repository selection, this is so if the user clicks the refresh button the selected
                // option is removed - so a selection need to be remade to continue
                state.uses[storeKey].repositories.selectedOption = null
            }
        })
        builder.addCase(getGitRepositories.fulfilled, (state, action: PayloadAction<{ repositories?: Repositories["options"], storeKey: keyof UploadFromGitHubStoreState["uses"] }>) => {

            const {repositories, storeKey} = action.payload

            state.uses[storeKey].repositories.status = "succeeded"
            state.uses[storeKey].repositories.message = undefined

            if (repositories !== undefined) {
                state.uses[storeKey].repositories.options = repositories
            }
        })
        builder.addCase(getGitRepositories.rejected, (state, action) => {

            const storeKey = hasOwnProperty(action.meta.arg, "storeKey") ? action.meta.arg.storeKey : action.meta.arg.name

            if (typeof storeKey === "string" && isKeyOf(state.uses, storeKey)) {

                state.uses[storeKey].repositories.status = "failed"

                const text = {
                    title: "Failed to download the list of repositories",
                    message: "The request to retrieve the details of the model repositories from GitHub failed. This is usually because the repositories specified in the config do not exist.",
                    details: enrichErrorMessages(action.error.message)
                }

                showToast("error", text, "getGitRepositories/rejected")
                console.error(action.error)
            }
        })

        // A set of lifecycle reducers to run before/after the getRepository function
        builder.addCase(getRepository.pending, (state, action) => {

            const storeKey = hasOwnProperty(action.meta.arg, "storeKey") ? action.meta.arg.storeKey : action.meta.arg.name

            if (typeof storeKey === "string" && isKeyOf(state.uses, storeKey)) {

                // Clear all the messages
                toast.dismiss()
                state.uses[storeKey].repository.status = "pending"
                state.uses[storeKey].repository.message = undefined
                // Remove any existing branch selection, this is so if the user clicks the refresh button the selected
                // option is removed - so a selection need to be remade to continue
                state.uses[storeKey].repository.branches.selectedOption = null
                // Reset the upload, file, branch, tree and branch properties of the store as these are downstream of the repository
                state.uses[storeKey].branch = setupBranch()
                state.uses[storeKey].tree = setupTree()
                state.uses[storeKey].file = setupFile()
                state.uses[storeKey].upload = setupUpload()
            }
        })
        builder.addCase(getRepository.fulfilled, (state, action: PayloadAction<{
            branches?: Option<string>[],
            contributors?: Repository["contributors"],
            modelFileExtensions?: undefined | string[],
            pulls?: Repository["pulls"],
            repositoryDetails?: Repository["details"],
            storeKey: keyof UploadFromGitHubStoreState["uses"],
            tracConfigName?: undefined | string
        }>) => {

            const {storeKey, repositoryDetails, branches, contributors, pulls, modelFileExtensions, tracConfigName} = action.payload

            state.uses[storeKey].repository.status = "succeeded"
            state.uses[storeKey].repository.message = undefined

            // The payload can be empty if there is no repository set
            if (repositoryDetails !== undefined && branches !== undefined && contributors !== undefined && pulls !== undefined && modelFileExtensions !== undefined && tracConfigName !== undefined) {
                state.uses[storeKey].repository.details = repositoryDetails
                state.uses[storeKey].repository.branches.options = branches
                state.uses[storeKey].repository.contributors = contributors
                state.uses[storeKey].repository.pulls = pulls
                state.uses[storeKey].repository.modelFileExtensions = modelFileExtensions
                state.uses[storeKey].repository.tracConfigName = tracConfigName
            }
        })
        builder.addCase(getRepository.rejected, (state, action) => {

            const storeKey = hasOwnProperty(action.meta.arg, "storeKey") ? action.meta.arg.storeKey : action.meta.arg.name

            if (typeof storeKey === "string" && isKeyOf(state.uses, storeKey)) {

                state.uses[storeKey].repository.status = "failed"
                showToast("error", enrichErrorMessages(action.error.message), "getRepository/rejected")
                console.error(action.error)
            }
        })

// A set of lifecycle reducers to run before/after the getBranch function
        builder.addCase(getBranch.pending, (state, action) => {

            const storeKey = hasOwnProperty(action.meta.arg, "storeKey") ? action.meta.arg.storeKey : action.meta.arg.name

            if (typeof storeKey === "string" && isKeyOf(state.uses, storeKey)) {

                // Clear all the messages
                toast.dismiss()
                state.uses[storeKey].branch.status = "pending"
                state.uses[storeKey].branch.message = undefined
                // Reset the file, upload and tree properties of the store as these are downstream of the branch
                state.uses[storeKey].tree = setupTree()
                state.uses[storeKey].file = setupFile()
                state.uses[storeKey].upload = setupUpload()
                // Remove any existing commit selection, this is so if the user clicks the refresh button the selected
                // option is removed - so a selection need to be remade to continue
                state.uses[storeKey].branch.commit.selectedOption = null
            }
        })
        builder.addCase(getBranch.fulfilled, (state, action: PayloadAction<{ options?: Group<string, CommitDetails>[], storeKey: keyof UploadFromGitHubStoreState["uses"] }>) => {

            const {options, storeKey} = action.payload

            state.uses[storeKey].branch.status = "succeeded"
            state.uses[storeKey].branch.message = undefined

            if (options) {
                state.uses[storeKey].branch.commit.options = options
            }
        })
        builder.addCase(getBranch.rejected, (state, action) => {

            const storeKey = hasOwnProperty(action.meta.arg, "storeKey") ? action.meta.arg.storeKey : action.meta.arg.name

            if (typeof storeKey === "string" && isKeyOf(state.uses, storeKey)) {

                state.uses[storeKey].branch.status = "failed"
                showToast("error", enrichErrorMessages(action.error.message), "getBranch/rejected")
                console.error(action.error)
            }
        })

        // A set of lifecycle reducers to run before/after the getTree function
        builder.addCase(getTree.pending, (state, action) => {

            const {storeKey} = action.meta.arg

            // Clear all the messages
            toast.dismiss()
            state.uses[storeKey].tree.status = "pending"
            state.uses[storeKey].tree.message = undefined
            // Reset the file and upload properties of the store as these are downstream of the tree
            state.uses[storeKey].file = setupFile()
            state.uses[storeKey].upload = setupUpload()
        })
        builder.addCase(getTree.fulfilled, (state, action: PayloadAction<{
            fileTree?: FileTree
            folderTree?: FolderTree
            storeKey: keyof UploadFromGitHubStoreState["uses"]
            treeHasSelectableItem?: boolean
            treeHasUnselectableItem?: boolean
        }>) => {

            const {fileTree, folderTree, storeKey, treeHasSelectableItem, treeHasUnselectableItem} = action.payload

            if (fileTree != undefined && folderTree != undefined && treeHasSelectableItem != undefined && treeHasUnselectableItem !== undefined) {

                state.uses[storeKey].tree.status = "succeeded"
                state.uses[storeKey].tree.message = undefined
                state.uses[storeKey].tree.fileTree = fileTree
                state.uses[storeKey].tree.folderTree = folderTree
                state.uses[storeKey].tree.treeHasSelectableItem = treeHasSelectableItem
                state.uses[storeKey].tree.treeHasUnselectableItem = treeHasUnselectableItem
                state.uses[storeKey].tree.selectedOption = null
            }
        })
        builder.addCase(getTree.rejected, (state, action) => {

            const {storeKey} = action.meta.arg

            state.uses[storeKey].tree.status = "failed"
            showToast("error", enrichErrorMessages(action.error.message), "getTree/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the getFile function
        builder.addCase(getFile.pending, (state, action) => {

            const {storeKey} = action.meta.arg

            // Clear all the messages
            toast.dismiss()
            state.uses[storeKey].file.status = "pending"
            state.uses[storeKey].file.message = undefined
            // Reset the upload property of the store as this is downstream of the repository
            state.uses[storeKey].upload = setupUpload()
        })
        builder.addCase(getFile.fulfilled, (state, action: PayloadAction<{ file?: Omit<File, "status" | "message" | "schema" | "model"> & { schema?: File["schema"] } & { model?: File["model"] }, storeKey: keyof UploadFromGitHubStoreState["uses"] }>) => {

            const {file, storeKey} = action.payload

            state.uses[storeKey].file.status = "succeeded"
            state.uses[storeKey].file.message = undefined

            // The payload is void when the user deselects a file from the FileTree component, meaning that no GitHub API
            // request is made to get details.
            if (file) {

                state.uses[storeKey].file.details = file.details
                state.uses[storeKey].file.alreadyInTrac = file.alreadyInTrac
                state.uses[storeKey].file.metadata = file.metadata

                if (file.path !== undefined) {
                    state.uses[storeKey].file.path = file.path
                }

                if (file.model) {
                    state.uses[storeKey].file.model.entryPoint = file.model.entryPoint
                    state.uses[storeKey].file.model.tracModelClassOptions = file.model.tracModelClassOptions
                    state.uses[storeKey].file.model.selectedTracModelClassOption = file.model.selectedTracModelClassOption
                } else if (file.schema) {
                    state.uses[storeKey].file.schema.errorMessages = file.schema.errorMessages
                    state.uses[storeKey].file.schema.fields = file.schema.fields
                }

            } else {

                // If there is no payload the file can not be loaded so set the state to failed
                // so the attributes are not shown to the user to fill in
                state.uses[storeKey].file.details = null
                state.uses[storeKey].file.alreadyInTrac = {foundInTrac: false, tag: null}
                state.uses[storeKey].file.metadata = {
                    json: null,
                    hasMetadata: false,
                    filename: null,
                    hasValidMetadata: false,
                    url: null
                }
                state.uses[storeKey].file.path = null
                state.uses[storeKey].file.model.entryPoint = null
                state.uses[storeKey].file.model.tracModelClassOptions = []
                state.uses[storeKey].file.model.selectedTracModelClassOption = null
                state.uses[storeKey].file.schema.errorMessages = []
                state.uses[storeKey].file.schema.fields = null

            }
        })
        builder.addCase(getFile.rejected, (state, action) => {

            const {storeKey} = action.meta.arg

            state.uses[storeKey].file.status = "failed"
            showToast("error", enrichErrorMessages(action.error.message), "getFile/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the uploadModelToTrac function
        builder.addCase(uploadModelToTrac.pending, (state, action: { meta: { arg: { storeKey: keyof UploadFromGitHubStoreState["uses"] } } }) => {

            const {storeKey} = action.meta.arg

            // Clear all the messages
            toast.dismiss()
            state.uses[storeKey].upload.status = "pending"
            state.uses[storeKey].upload.message = undefined
        })
        builder.addCase(uploadModelToTrac.fulfilled, (state, action: PayloadAction<{ jobStatus: trac.api.JobStatus, alreadyInTrac: File["alreadyInTrac"], storeKey: keyof UploadFromGitHubStoreState["uses"] }>) => {

            const {alreadyInTrac, jobStatus, storeKey} = action.payload

            state.uses[storeKey].upload.status = "succeeded"
            state.uses[storeKey].upload.message = undefined

            // Update the information about whether the model is loaded in TRAC (if the delay we used was enough
            // time for it to load )
            state.uses[storeKey].file.alreadyInTrac = alreadyInTrac

            const text = `The job to load the model into TRAC was successfully started with job ID ${jobStatus.jobId?.objectId}, you can see its progress in the 'Find a job' page.`
            showToast("success", text, "uploadModelToTrac/fulfilled")
        })
        builder.addCase(uploadModelToTrac.rejected, (state, action) => {

            const {storeKey} = action.meta.arg

            state.uses[storeKey].upload.status = "failed"

            const text = {
                title: "Failed to load the model",
                message: "The job to import the model was not successfully submitted.",
                details: action.error.message
            }

            showToast("error", text, "uploadModelToTrac/rejected")
            console.error(action.error)
        })

        // A set of lifecycle reducers to run before/after the uploadSchemaToTrac function
        builder.addCase(uploadSchemaToTrac.pending, (state, action) => {

            const {storeKey} = action.meta.arg

            // Clear all the messages
            toast.dismiss()
            state.uses[storeKey].upload.status = "pending"
            state.uses[storeKey].upload.message = undefined
        })
        builder.addCase(uploadSchemaToTrac.fulfilled, (state, action: PayloadAction<{ alreadyInTrac: File["alreadyInTrac"], storeKey: keyof UploadFromGitHubStoreState["uses"] }>) => {

            const {alreadyInTrac, storeKey} = action.payload

            state.uses[storeKey].upload.status = "succeeded"
            state.uses[storeKey].upload.message = undefined

            // Update the information about whether the dataset is loaded in TRAC
            state.uses[storeKey].file.alreadyInTrac = alreadyInTrac

            showToast("success", `The schema was loaded into TRAC was successfully with object ID ${alreadyInTrac.tag?.header?.objectId}.`, "uploadSchemaToTrac/fulfilled")
        })
        builder.addCase(uploadSchemaToTrac.rejected, (state, action) => {

            const {storeKey} = action.meta.arg

            state.uses[storeKey].upload.status = "failed"

            const text = {
                title: "Failed to load the model",
                message: "The loading of the schema into TRAC was not completed successfully.",
                details: action.error.message
            }

            showToast("error", text, "uploadSchemaToTrac/rejected")
            console.error(action.error)
        })
    }
})

// Action creators are generated for each case reducer function
export const {
    logout,
    setBranch,
    setCommit,
    setFile,
    setLoginDetails,
    setRepository,
    setTracModelClass,
    toggleShowUnselectableFiles,
    toggleValidationMessages
} = uploadFromGitHubStore.actions

export default uploadFromGitHubStore.reducer