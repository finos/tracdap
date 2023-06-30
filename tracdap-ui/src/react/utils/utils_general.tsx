/**
 * A group of general utilities, these are tested in tests/utils/utils_general.test.ts
 * @category Utils
 * @module GeneralUtils
 */

import addDays from "date-fns/addDays";
import addMonths from "date-fns/addMonths";
import addSeconds from "date-fns/addSeconds";
import differenceInCalendarDays from "date-fns/differenceInCalendarDays";
import {applyNumberFormat, convertDateObjectToFormatCode, setAnyFormat} from "./utils_formats";
import {
    BasicTypeConvertor,
    BusinessSegmentGroup,
    CodeRepositories,
    ColumnColourDefinition,
    DataRow,
    DataValues,
    DateFormat,
    DatetimeFormat, FileTree,
    GenericGroup,
    GenericOption,
    ModelRepository,
    Option,
    SelectDateProps,
    TableValues,
    ThemeColours,
    ThemesList,
    TracCookieNames,
    TracGroup,
    TracNumberBasicTypes,
    TracUiCookieNames,
    UiBusinessSegmentsDataRow
} from "../../types/types_general";
import CloseButton from "react-bootstrap/CloseButton";
import {convertArrayToOptions, makeArrayOfObjectsUnique, makeArrayOfObjectsUniqueByProperty, sortArrayBy} from "./utils_arrays";
import {getObjectName} from "./utils_trac_metadata";
import differenceInSeconds from "date-fns/differenceInSeconds";
import endOfMonth from "date-fns/endOfMonth";
import enGb from 'date-fns/locale/en-GB';
import formatISO from "date-fns/formatISO";
import {General as Themes} from "../../config/config_themes";
import {General} from "../../config/config_general";
import {hasOwnProperty, includes, isDefined, isGroupOption, isKeyOf, isObject, isThemesList, isTracBoolean, isTracDateOrDatetime, isTracNumber, isTracString} from "./utils_trac_type_chckers";
import {Icon} from "../components/Icon";
import {OptionsOrGroups, SingleValue} from "react-select";
import parseISO from "date-fns/parseISO";
import {checkUrlEndsRight, convertStringToInteger, convertStringToObjectType, convertStringValueToBoolean, isValidIsoDateString, isValidIsoDatetimeString, isValidNumberAsString} from "./utils_string";
import React from "react";
import {roundNumberToNDecimalPlaces} from "./utils_number";
import startOfMonth from "date-fns/startOfMonth";
import {Style} from "@react-pdf/types";
import {toast, ToastOptions, TypeOptions} from "react-toastify";
import {Toast} from "../components/Toast";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {Types} from "../../config/config_trac_classifications";
import startOfDay from "date-fns/startOfDay";
import startOfQuarter from "date-fns/startOfQuarter";
import getMonth from "date-fns/getMonth";
import setMonth from "date-fns/setMonth";
import startOfYear from "date-fns/startOfYear";
import startOfWeek from "date-fns/startOfWeek";
import endOfDay from "date-fns/endOfDay";
import endOfQuarter from "date-fns/endOfQuarter";
import endOfYear from "date-fns/endOfYear";
import endOfWeek from "date-fns/endOfWeek";

/**
 * A function that sets a cookie so that the values can persist across sessions. It is a host only cookie
 * and can not be shared with anything other than the creating domain.
 *
 * @see https://medium.com/swlh/working-with-cookies-and-creating-cookies-in-javascript-764eb95aa4a1
 *
 * @param name - The name of the variable to save.
 * @param value - The value of the variable to save.
 * @param setSecure - Whether the cookie will only be saved if the page is served on a secure connection.
 * There is a client-config.json setting for whether to allow this. In development environments there is often
 * no SSL configured so setSecure needs to be false, but in production environments with SSL this should
 * be true.
 * @param expiry - The time in seconds that the cookie will be valid for. Defaults to 10 years (10 * 60 * 60 * 24 * 365).
 */
export const setCookie = (name: TracUiCookieNames, value: string, setSecure: boolean, expiry: number = 315360000): void => {

    if (name && value) {
        document.cookie = `${name}=${value}; ${setSecure ? "Secure;" : ""} Path=/; SameSite=strict; max-age=${expiry}`;
    }
}

/**
 * A function that gets a cookie so that a value can be retrieved across sessions.
 * @param name - The name of the cookie to retrieve.
 * @returns The cookie value or undefined if not set.
 */
export function getCookie(name: TracUiCookieNames | TracCookieNames): string | undefined {

    let cookie: Record<string, string> = {};

    // Get the cookie and break the string up and add the key value pairs to cookie object
    document.cookie.split(';').forEach(
        function (el: string) {

            let [k, v] = el.split('=');

            // Bad code could add an undefined variable as a cookie value which would
            // convert into an undefined string, so we don't allow that value to be retrieved.
            // Also decode the cookie strings, %20 becomes " ".
            if (v && v !== "undefined") cookie[k.trim()] = decodeURI(v);
        }
    )

    return cookie[name];
}

/**
 * A function that sets the colours to use based on the selected theme in the browser.
 * @param theme - The name of the user selected theme.
 */
export const setThemeColours = (theme: ThemesList): void => {

    // Get the selected theme from the config
    const selectedTheme = Themes[theme];

    // For each colour defined in the theme set the CSS variable.
    isObject(selectedTheme) && Object.keys(selectedTheme).forEach(key => {

        if (isKeyOf(selectedTheme, key)) document.documentElement.style.setProperty(key, selectedTheme[key])
    })
}

/**
 * A function that gets the font colour as a hexadecimal from the css variable name and the name of the
 * theme selected by the user. There is an additional check to make sure that the theme name is valid.
 *
 * @param theme - The name of the theme selected by the user.
 * @param name - The name of the css variable that you want to get.
 */
export const getThemeColour = (theme: string, name: ThemeColours): string | undefined => {

    return isThemesList(theme) && isKeyOf(Themes[theme], name) ? Themes[theme][name] : undefined
}

/**
 * A function that takes the expiry date of the JWT authentication in the application store
 * and calculates how many seconds are left and also whether this is below the limit to
 * show the re-authentication modal.
 *
 * @param expiryDatetime - The JWT expiry datetime. It's a string as we can't store Javascript
 * date objects in a Redux store.
 * @param secondsToWarnAt - The remaining seconds to show the re-authentication modal at.
 * @returns Whether the JWT is close to expiry and the number of seconds left, number of seconds left is
 * null if the expiry datetime of the JWT is null or is not a valid number.
 */
export const checkJwtExpiry = (expiryDatetime: null | string, secondsToWarnAt: number = 120): { jwtCloseToExpiry: boolean, jwtSecondsToExpiry: null | number } => {

    if (expiryDatetime) {

        const secondsRemaining = differenceInSeconds(new Date(expiryDatetime), new Date())

        return {
            jwtCloseToExpiry: Boolean(secondsRemaining < secondsToWarnAt || secondsToWarnAt === 0),
            jwtSecondsToExpiry: secondsRemaining
        }
    }

    return {jwtCloseToExpiry: false, jwtSecondsToExpiry: null}
}

/**
 * A function that works out from the URL of th application whether the application is running
 * locally on localhost or on a server.
 */
export const isAppRunningLocally = (): boolean => {

    return window.location.hostname === "localhost"
}

/**
 * A function that can be called outside a component that will cause a pop-up to show with a message to the user.
 * @param variant - The style of message to show e.g. a red error message.
 * @param text - The messages to show which can be separated into title, message and details.
 * @param id - The unique message ID, pop-ups of the same ID won't show more than once.
 */
export const showToast = (variant: TypeOptions, text: string | React.ReactElement<unknown, string> | { title?: string, message: React.ReactElement<unknown, string> | string, details?: string | string[] }, id: string): React.ReactText => {

    // Different timers based on the type of message, false is no timer
    const autoClose: Record<string, number | false | undefined> = {
        info: 7500,
        error: false,
        warning: 7500,
        success: 7500
    }

    const icons = {
        info: 'bi-info-circle',
        error: 'bi-exclamation-circle',
        warning: "bi-exclamation-diamond",
        success: "bi-check-circle",
        default: 'bi-info-circle'
    }

    const options: ToastOptions = {
        autoClose: autoClose[variant],
        toastId: id,
        type: variant,
        // Turn off the default close and toast icons
        closeButton: false,
        icon: false
    }

    // A tile can only be a string, if the text argument is a React element then pass that as the message
    const title = React.isValidElement(text) ? undefined : typeof text === "string" ? undefined : hasOwnProperty(text, 'message') ? text.title : undefined

    // Message can be a string in the text object, the text argument if it is not passed as an object or a React element if that is what text is
    const message = React.isValidElement(text) ? text : typeof text === 'string' ? text : !hasOwnProperty(text, 'message') ? text : text.message

    // Details can be a React element, an array of string messages or a single string.
    const details = React.isValidElement(text) || typeof text === 'string' ? undefined : typeof text.details === 'string' ? text.details.split('\n') : text.details

    return (
        toast(<Toast icon={<Icon ariaLabel={variant}
                                 className={"me-2"}
                                 icon={icons[variant]}
                                 size={"1.25rem"}/>}
                     title={title}
                     message={message}
                     details={details}
                     onClose={<CloseButton aria-label={"Close message"}
                                           id={id}
                                           onClick={e => toast.dismiss(e.currentTarget.id)}
                                           variant={typeof text === 'string' || !hasOwnProperty(text, 'title') || !text.title ? undefined : "white"}/>}
        />, options)
    )
}

/**
 * A function that converts a Javascript datetime object into an ISO string date or datetime.
 * @param value - The Javascript date object to convert.
 * @param dateReturnType - The date format to return e.g. dateIso
 * @returns The ISO date or datetime string.
 */
export const convertDateObjectToIsoString = (value: Date, dateReturnType: "dateIso" | "datetimeIso" = "dateIso"): string => {

    if (dateReturnType === "dateIso") {

        return formatISO(value, {representation: 'date'})

    } else {

        return value.toISOString()
    }
}

/**
 * A function that takes an object key and converts it into a TRAC tag selector that can be used to get the
 * object's metadata (or data). Object keys are of the form "MODEL-4dfc8132-153c-43eb-95b3-bd4b64e04122-v1".
 *
 * @remarks
 * Note that this function does not validate that the string is an object key, that needs to be done before
 * calling this function.
 *
 * @param objectKey - The object key to convert.
 * @param searchAsOf - The datetime for the tag selector, if this is set and the tag selector is used to get
 * as item, then it will its metadata as at that point in time.
 */
export const convertObjectKeyToTagSelector = (objectKey: string, searchAsOf?: null | trac.metadata.IDatetimeValue): trac.metadata.ITagSelector | undefined => {

    // The leading string before the first '-'. In a valid objectKey this is a TRAC object type e.g. "DATA"
    const objectKeyAsArray = objectKey.split("-")
    const objectType = objectKeyAsArray[0].toUpperCase()

    // The includes function has a typechecker that will assert objectType as the same type of the array
    if (!includes(Types.tracObjectTypes.map(item => item.value), objectType)) throw new Error(`The object key is not a recognised TRAC object, the value ${objectKeyAsArray[0]} was found`)

    // Convert the object type to its enum
    const foundObjectType = convertStringToObjectType(objectType)

    // Create the TagSelector
    const tagSelector: trac.metadata.ITagSelector = {
        // This joins the array from index 1 to index length - 1. i.e. removing the first and last elements
        objectId: objectKeyAsArray.slice(1, -1).join("-"),
        objectType: foundObjectType,
        latestTag: true,
        objectAsOf: searchAsOf,
        tagAsOf: searchAsOf
    }

    // Now we have to handle both cases where the object version part is 'latest' or a specific version
    if (objectKeyAsArray[objectKeyAsArray.length - 1] === "latest") {
        tagSelector.latestObject = true
    } else {
        // See https://stackoverflow.com/questions/1862130/strip-all-non-numeric-characters-from-string-in-javascript
        tagSelector.objectVersion = convertStringToInteger(objectKeyAsArray[objectKeyAsArray.length - 1])
    }

    return tagSelector
}

/**
 * A function that takes two options for the {@link SelectOption} component and returns whether they are the same. This
 * is used as a check whether a 'change' of option should be updated in the store.
 *
 * @param optionA - The first option to compare.
 * @param optionB - The second option to compare.
 */
export const isOptionSame = (optionA: SingleValue<GenericOption>, optionB: SingleValue<GenericOption>): boolean => {

    return Boolean(optionA != null && optionB != null && optionA.value === optionB.value && optionA.label === optionB.label)
}

/**
 * A function that takes an array of options for the  component which are for selecting an object in TRAC and updates the label.
 * Options for TRAC objects have the 'tag' property which contains their metadata Tag and can be used to provide information for the label. This
 * function is used where you want to allow users to add or take away information from a list that helps them find the option they want, such as
 * adding in the version or creation date.
 *
 * @param myOptions - The array of options to update.
 * @param showObjectId - Whether to add the object IDs to the label.
 * @param showVersions - Whether to add the object and tag versions to the label.
 * @param showUpdatedDate - Whether to add the object updated datetime to the label.
 * @param showCreatedDate - Whether to add the object created datetime to the label.
 * @returns The array of options with the labels updated.
 */
export function updateObjectOptionLabels<T extends Option<void, trac.metadata.ITag>>(myOptions: T[], showObjectId: boolean, showVersions: boolean, showUpdatedDate: boolean, showCreatedDate: boolean): T[] {

    return myOptions.map(option => {

        let newOption = {...option}
        newOption.label = getObjectName(option.tag, showObjectId, showVersions, showUpdatedDate, showCreatedDate)
        return newOption
    })
}

/**
 * A function that takes a status code from an API call and enriches the message to the user.
 * @param status - The API status code.
 * @returns The message to show the user.
 */
export const enrichApiCodes = (status: number): string | undefined => {

    if (status === 401) {
        return `Invalid authentication credentials (HTTP status ${status})`
    } else if (status === 403) {
        return `Access to the requested resource is forbidden (HTTP status ${status})`
    } else if (status === 304) {
        return `There is no need to retransmit the requested resources (HTTP status ${status})`
    } else if (status === 404) {
        return `Page Not Found or File Not Found (HTTP status ${status})`
    } else if (status === 422) {
        return `Request is well-formed, however, due to semantic errors it is unable to be processed (HTTP status ${status})`
    } else {
        return undefined
    }
}

/**
 * A function that tries to add additional info to any error messages from a non-TRAC API. The main use case is that a
 * failed to fetch message means that the user is not connected to the internet. This can be extended to cover other cases
 * like the TRAC API not being available or the processing of error messages using '\n' as new lines.
 *
 * @param message - The original API error message.
 * @returns The message to show the user.
 */
export const enrichErrorMessages = (message: string | undefined): string => (

    message === "Failed to fetch" ? "Not connected to the internet" : message !== undefined ? message : "Unknown error"
)

/**
 * A function that takes either a number or an array and works out whether the word before it needs to be 'do' or 'does'.
 * For example if the number was a number of columns 0 would be '0 columns do', 1 would be '1 column does' etc.
 *
 * @param myArray - The item to check.
 * @returns 'do' or 'does'.
 */
export const doOrDoes = (myArray: any[] | number): string => (

    (Array.isArray(myArray) && myArray.length === 1) || (typeof myArray === "number" && myArray === 1) ? "does" : "do"
)

/**
 * A function that takes either a number or an array and works out whether the word before it needs to be 'is' or 'are'.
 * For example if the number was a number of columns 0 would be '0 columns are', 1 would be '1 column is' etc.
 *
 * @param myArray - The item to check.
 * @returns 'are' or 'is'.
 */
export const isOrAre = (myArray: any[] | number): string => (

    (Array.isArray(myArray) && myArray.length === 1) || (typeof myArray === "number" && myArray === 1) ? "is" : "are"
)

/**
 * A function that takes either a number or an array and works out whether the word before it needs to be 'was' or 'were'.
 * For example if the number was a number of columns 0 would be '0 columns were', 1 would be '1 column was' etc.
 *
 * @param myArray - The item to check.
 * @returns 'was' or 'were'.
 */
export const wasOrWere = (myArray: any[] | number): string => (

    (Array.isArray(myArray) && myArray.length === 1) || (typeof myArray === "number" && myArray === 1) ? "was" : "were"
)

/**
 * A function that takes either a number or an array and works out whether the word to describe it needs an 's' at the end or not.
 * For example if the number was a number of seconds 0 would be '0 seconds', 1 would be '1 second' etc.
 *
 * @param myArray - The item to check.
 * @param plural - How to make the plural. So if you have an array of 10 elements that counts bananas then the plural
 * is 's' as in 10 bananas. If you have an array of 10 elements that counts churches then the plural is 'es' as in 10 churches.
 */
export const sOrNot = (myArray: any[] | number, plural: string = "s"): string => (

    (Array.isArray(myArray) && (myArray.length > 1 || myArray.length === 0)) || (typeof myArray === "number" && (myArray > 1 || myArray === 0)) ? plural : ""
)

/**
 * A function that takes an array and returns a string of the values separated by
 * commas and and and. So [1,2,3] becomes "1,2 and 3".
 *
 * @param myArray - The array to convert.
 * @returns The concatenated string.
 */
export const commasAndAnds = (myArray: (string | number)[]): string => {

    if (myArray.length === 0) {

        return ""

    } else if (myArray.length === 1) {

        return myArray[0].toString()

    } else {

        const endElement = myArray[myArray.length - 1]
        return myArray.slice(0, myArray.length - 1).join(", ") + " and " + endElement.toString()
    }
}

/**
 * A function that takes an array and returns a string of the values separated by
 * commas and or. So [1,2,3] becomes "1,2 or 3".
 *
 * @param myArray - The array to convert.
 * @returns The concatenated string.
 */
export const commasAndOrs = (myArray: (string | number)[]): string => {

    if (myArray.length === 0) {

        return ""

    } else if (myArray.length === 1) {

        return myArray[0].toString()

    } else {

        const endElement = myArray[myArray.length - 1]
        return myArray.slice(0, myArray.length - 1).join(", ") + " or " + endElement.toString()
    }
}

/**
 * A function that takes a string and returns 'a' or 'an' depending on the first letter of the word.
 * @param myString - The string to check.
 * @returns 'a' or 'an'.
 */
export const aOrAn = (myString: string): "a" | "an" => {

    return myString.charAt(0).toLocaleLowerCase() in ["a", "e", "i", "o", "u"] ? "an" : "a"
}

/**
 * A function that returns the Bootstrap icon to show based on the language name, this is used in the UploadAModelScene
 * scene for example when displaying information about a repository or a file. There is a fallback icon name if the
 * requested language of fie type is not set.
 *
 * @param languageOtFile - The language or file extension to get the icon for.
 * @returns The icon name.
 */
export const setLanguageOrFileIcon = (languageOtFile?: null | string): string => {

    // A key value pair mapping between the language name and the icon to show
    const iconLookup: Record<string, string> = {
        "JAVA": "bi-filetype-java",
        "JAVASCRIPT": "bi-filetype-js",
        "JS": "bi-filetype-js",
        "PYTHON": "bi-filetype-py",
        "PY": "bi-filetype-py",
        "JSON": "bi-filetype-json",
        "YML": "bi-filetype-yml",
        "YAML": "bi-filetype-yml",
        "PDF": "bi-filetype-pdf",
        "DOC": "bi-filetype-doc",
        "DOCX": "bi-filetype-docx",
        "CSV": "bi-filetype-csv",
        "JPG": "bi-filetype-jpg",
        "PNG": "bi-filetype-png",
        "XS": "bi-filetype-xs",
        "XLX": "bi-filetype-xlx",
        "PPT": "bi-filetype-ppt",
        "PPTX": "bi-filetype-pptx",
    }

    if (!languageOtFile) {
        return "bi-code-slash"
    } else {
        return iconLookup[languageOtFile.toUpperCase()] || "bi-file-earmark-text"
    }
}

/**
 * A function that extracts the file extension from a file name.
 *
 * @see https://stackoverflow.com/questions/190852/how-can-i-get-file-extensions-with-javascript/12900504#12900504
 *
 * @param filename - The file name to extract the extension from.
 * @returns The file extension without the '.'.
 */
export const getFileExtension = (filename: string): string => (

    filename.slice((filename.toLowerCase().lastIndexOf(".") - 1 >>> 0) + 2)
)

/**
 * A function that takes a file path which includes the file name and extension at the end and converts it into
 * a range of properties that can be used in various other functions.
 *
 * @param filePath - The file path to break down.
 * @param splitString - The string to use to break the file path down into the separate folders.
 * @param joinString - The string to use to join the split file path together - just in case a different format is needed.
 * @returns A range of information about the supplied file path.
 */
export const getFileDetails = (filePath: string, splitString: string = "/", joinString: string = "/") => {

    const pathAsArray = filePath.split(splitString)

    return {
        filePathAsArray: pathAsArray,
        filePath: filePath,
        fileNameWithExtension: pathAsArray[pathAsArray.length - 1],
        fileNameWithoutExtension: pathAsArray[pathAsArray.length - 1].replace(new RegExp(`.${getFileExtension(pathAsArray[pathAsArray.length - 1])}$`), ""),
        fileExtension: getFileExtension(pathAsArray[pathAsArray.length - 1]),
        folderPathAsArray: pathAsArray.slice(0, pathAsArray.length - 1),
        folderPath: pathAsArray.slice(0, pathAsArray.length - 1).join(joinString),
    }
}

/**
 * A function that calculates the entry point and path properties for a model, this is used when uploading a model as TRAC needs
 * to know how to locate and execute the code.
 *
 * @param pathAndPackages - An object containing an array of strings that when concatenated form the path to the entry point.
 * (these strings are folders that have a "__init__.py" file in them), and an array of strings that when concatenated from the
 * path from the root of the project repository to the start of the packaging (these strings are folders that do not have a
 * "__init__.py" file in them).
 * @param fileNameWithoutExtension - The name of the code file to execute without the file extension.
 * @param tracModelClass - The model class in the code file to run when TRAC calls the model.
 */
export const calculateModelEntryPointAndPath = (pathAndPackages: { path: string[], packages: string[] }, fileNameWithoutExtension: string, tracModelClass?: string): { entryPoint: null | string, path: string } => {

    return {
        entryPoint: tracModelClass === undefined ? null : `${pathAndPackages.packages.join(".")}${pathAndPackages.packages.length === 0 ? "" : "."}${fileNameWithoutExtension}.${tracModelClass}`,
        // For files in the root of the repo the path needs to be a '.'
        path: pathAndPackages.path.length === 0 ? "." : pathAndPackages.path.join("/")
    }
}

/**
 * A function that takes a path to a model in the GitHub repo that the user is trying to load and returns the equivalent
 * path and package locations. The path is the location from the root of the repository to the source of the python packages.
 * The package is the location from the source of the python packages to the model being loaded.
 *
 * @param fileTree - The file tree for the repo selected to load the file from.
 * @param folderPathAsArray - The folder path to the file (without the file name at the end).
 */
export const calculatePathsAndPackages = (fileTree: FileTree, folderPathAsArray: string[]): { path: string[], packages: string[] } => {

    // So now we need to work out the path to the file and the entry point to specify in the TRAC
    // API call to load the model.
    let pathAndPackages: { path: string[], packages: string[] } = {path: [], packages: []}

    // This is an array of strings that together form the path to the file in the repo
    let copyOfFolderPathAsArray = [...folderPathAsArray]

    do {
        // Add in a file at the end of the path, this is the file that should be there in the folder
        // where the code file is if it is a python package
        copyOfFolderPathAsArray.push("__init__.py")

        // Does the file tree contain the init file at the required location?
        if (copyOfFolderPathAsArray.reduce((prev: any, path) => prev && prev[path], fileTree)) {
            // If yes then the folder is considered a python package and is added to the packages list
            // since we added the init file we have to add the second to last element
            pathAndPackages.packages.unshift(copyOfFolderPathAsArray[copyOfFolderPathAsArray.length - 2])
        } else {
            //If no then the folder is not considered a python package is added to the path list
            // since we added the init file we have to add the second to last element
            pathAndPackages.path.unshift(copyOfFolderPathAsArray[copyOfFolderPathAsArray.length - 2])
        }

        // Remove the init file and the processed element
        copyOfFolderPathAsArray.pop()
        copyOfFolderPathAsArray.pop()
    }
    while (copyOfFolderPathAsArray.length > 0);

    return pathAndPackages
}

/**
 * A function that takes a date object and increments it by a specific time interval, the intervals available map to
 * the allowed date formats.
 *
 * @param myDate - The date object to increment.
 * @param increment - The interval to increment by.
 */
export const incrementDate = (myDate: Date, increment: DateFormat | DatetimeFormat): Date => {

    if (increment === "DAY" || increment === "ISO") {
        return addDays(myDate, 1)
    } else if (increment === "WEEK") {
        return addDays(myDate, 7)
    } else if (increment === "MONTH") {
        return addMonths(myDate, 1)
    } else if (increment === "QUARTER") {
        return addMonths(myDate, 3)
    } else if (increment === "HALF_YEAR") {
        return addMonths(myDate, 6)
    } else if (increment === "YEAR") {
        return addMonths(myDate, 12)
    } else {
        return addSeconds(myDate, 1)
    }
}

/**
 * A function that converts minimum, maximum and increment information into a set of date options
 * for the SelectOption component.
 *
 * @param start - The start of the date range.
 * @param end - The end of the date range
 * @param increment - The step size for the list of options.
 * @param formatCodeAsString - The format for the option label.
 */
export const createDateRangeOptions = (start: string, end: string, increment: DateFormat | DatetimeFormat = "MONTH", formatCodeAsString: DateFormat | DatetimeFormat = "MONTH"): Option<string>[] => {

    // Deal with a bug where the user sets the min and the max dates are the wrong way around
    let startDateObject = (parseISO(end) < parseISO(start) ? parseISO(end) : parseISO(start))
    let endDateObject = (parseISO(start) > parseISO(end) ? parseISO(start) : parseISO(end))

    let options: Option<string>[] = []

    let date = startDateObject

    while (date <= endDateObject) {

        options.push({
            value: convertDateObjectToIsoString(date, "dateIso"),
            label: convertDateObjectToFormatCode(date, formatCodeAsString)
        })

        date = incrementDate(date, increment)
    }

    return options
}

/**
 * A function that converts minimum, maximum and increment information into a set of number options
 * for the SelectOption component.
 *
 * @param start - The start of the number range.
 * @param end - The end of the number range
 * @param increment - The step size for the list of options.
 * @param basicType {BasicType} Either "float" or "integer", the type of number the range should be.
 * @param formatCodeAsString {string | undefined} The decimal places to show in the labels for floats.
 */
export const createNumberRangeOptions = (start: number, end: number, increment: number, basicType: TracNumberBasicTypes, formatCodeAsString: undefined | string = undefined) => {

    // If the increment is positive we need the end position to be higher than the start otherwise
    // we can never create the range of options as end will never be reached.
    if (increment > 0 && start > end) {
        const startTemp = start
        start = end
        end = startTemp
    }

    if (increment < 0 && start < end) {
        const startTemp = start
        start = end
        end = startTemp
    }

    let options = []

    let number = basicType === trac.INTEGER ? Math.round(start) : start

    while (number <= end) {

        options.push({
            value: number,
            label: formatCodeAsString ? applyNumberFormat(number, formatCodeAsString) : number.toString()
        })
        number = roundNumberToNDecimalPlaces(number + increment, basicType === trac.INTEGER ? 0 : 8)
    }

    return options
}

/**
 * A function that converts two strings into a set of options for the {@link SelectOption} component.
 *
 * @param values - The string to use as values, separated by the separator e.g. "value1||value2".
 * @param labels - The string to use as labels, separated by the separator e.g. "label1||label2".
 * @param basicType - The type of variable the value of the option should be.
 * @param formatCodeAsString - The format for the date, datetime or precision of floats in the label.
 * @param separator - The delimiter that separates the items in the values and labels strings.
 * @returns The array of options.
 */
export const createOptionsFromConcatenatedStrings = (values: string, labels: string | null | undefined, basicType: trac.BasicType, formatCodeAsString: DateFormat | DatetimeFormat | string | undefined | null = undefined, separator: string = "||"): Option[] => {

    const valuesArray = values.split(separator)

    // If we only have a set of values for a string parameter then return the options using the util
    if (basicType === trac.STRING && !labels) return convertArrayToOptions(valuesArray, true)

    // For the remaining code we have to use the values and the labels to generate the options
    let labelsArray

    // If the concatenated labels are not set then we guess them
    if (labels) {

        labelsArray = labels.split(separator)

    } else {

        labelsArray = valuesArray.map(value => {

            if (formatCodeAsString) {

                return setAnyFormat(basicType, formatCodeAsString, convertDataBetweenTracTypes(trac.STRING, basicType, value))

            } else {

                return value
            }
        })
    }

    // Account for errors where the user sets the values and labels with different lengths
    const optionCount = Math.min(valuesArray.length, labelsArray.length)

    // Now build the options array
    let options: Option[] = []

    for (let i = 0; i < optionCount; i++) {

        // We have to create a specific variable for the label to appease typescript
        const label = labelsArray[i]

        options.push({
            value: convertDataBetweenTracTypes(trac.STRING, basicType, valuesArray[i]),
            label: label == null ? valuesArray[i] : label
        })
    }

    return options
}

/**
 * A function that converts an instructional string into a date, for example the string could be for
 * a date in 60 months time to the point when the user is running a model. This is used when setting a
 * parameter or attribute default value or minimum or maximum value. If you load a model into TRAC and have
 * the default value for a parameter as 2022-06-30 which is the month you loaded it up then every time that
 * the model is loaded up into the user interface that will be the default time set. Hover what the user
 * may want to do it to load the model up with that date as the current month, or the previous month to the
 * current month. These instructions allow the user to achieve this.
 *
 * @param instruction - The string instruction to turn into a date.
 * @param format - The format of the returned ISO string, either a date or a datetime.
 * @returns The ISO date or null value to use.
 */
export const getDateFromStringInstruction = (instruction: null | string, format: "dateIso" | "datetimeIso"): null | string => {

    // Some clients like to store dates as 2022/12/01 for Dec 2022 and some would like 2022/12/31.
    // In places where a date needs to be set by the UI there is a config for which approach is taken.
    const periodFunction = General.dates.endOrStartOfPeriod === "start" ? startOfMonth : endOfMonth

    // This returns null when it is an empty string or a null value
    if (!instruction) return null

    // If the date is an ISO date string then just return that
    else if (isValidIsoDateString(instruction) || isValidIsoDatetimeString(instruction)) return instruction

    // Now look to process the instruction
    let date = null

    if (instruction === "CURRENT_DAY") {

        date = new Date()

    } else if (instruction === "PREVIOUS_DAY") {

        date = addDays(new Date(), -1)

    } else if (instruction === "NEXT_DAY") {

        date = addDays(new Date(), 1)

    } else if (instruction === "CURRENT_MONTH") {

        date = periodFunction(new Date())

    } else if (instruction === "PREVIOUS_MONTH") {

        date = addMonths(periodFunction(new Date()), -1)

    } else if (instruction === "NEXT_MONTH") {

        date = addMonths(periodFunction(new Date()), 1)

    } else if (/^BACKWARD_[0-9]+_MONTHS$/.test(instruction)) {

        const monthsBackward = instruction.match(/(\d+)/g)
        date = monthsBackward == null ? null : addMonths(periodFunction(new Date()), -1 * parseInt(monthsBackward[0]))

    } else if (/^FORWARD_[0-9]+_MONTHS$/.test(instruction)) {

        const monthsForward = instruction.match(/(\d+)/g)
        date = monthsForward == null ? null : addMonths(periodFunction(new Date()), parseInt(monthsForward[0]))

    } else if (/^BACKWARD_[0-9]+_DAYS$/.test(instruction)) {

        const daysBackward = instruction.match(/(\d+)/g)
        date = daysBackward === null ? null : addDays(new Date(), -1 * parseInt(daysBackward[0]))

    } else if (/^FORWARD_[0-9]+_DAYS$/.test(instruction)) {

        const daysForward = instruction.match(/(\d+)/g)
        date = daysForward === null ? null : addDays(new Date(), parseInt(daysForward[0]))
    }

    // Convert the date object to an ISO string version
    return date ? convertDateObjectToIsoString(date, format) : null
}

/**
 * A function that converts between TRAC types, for example a string may need to be converted to a number or a date to a
 * string. This function checks the variable and returns the converted value if it is possible. By using this function
 * as much of the information is retained as possible rather than just setting the new variable to null.
 *
 * @remarks
 * This function uses multiple signatures so Typescript can handle the right conditional return type.
 *
 * @see https://stackoverflow.com/questions/52817922/typescript-return-type-depending-on-parameter
 *
 * @param oldType - The existing TRAC basic type for the variable.
 * @param newType - The new TRAC basic type after the conversion.
 * @param oldValue {null|bool|number|string} The existing value to be converted.
 */
export function convertDataBetweenTracTypes<P extends trac.BasicType, U extends trac.BasicType, T extends null | string | number | boolean>(oldType: P, newType: U | P, oldValue: T): null | BasicTypeConvertor<U>;
export function convertDataBetweenTracTypes(oldType: trac.BasicType, newType: trac.BasicType, oldValue: null | string | number | boolean): null | string | number | boolean {

    let newValue: DataValues

    if (oldType === newType || oldValue == null) {

        newValue = oldValue
    }

        // If we are moving between string and number we check to see if the string is
    // valid number and use that if it is
    else if (oldType === trac.STRING && isTracNumber(newType)) {

        // This returns 'true' if the value is not a number
        if (typeof oldValue !== "string" || !isValidNumberAsString(oldValue)) {
            // Inputs that are numeric can not take null or undefined values
            newValue = null

        } else if (newType === trac.INTEGER) {
            // Use parseFloat instead of Number - tighter checking
            newValue = roundNumberToNDecimalPlaces(parseFloat(oldValue), 0)

        } else {

            newValue = parseFloat(oldValue)
        }
    }
    // If we are moving from a number to a string we convert it to a string, null, undefined, false and "" are all set to null
    else if (newType === trac.STRING) {

        newValue = oldValue ? oldValue.toString() : null
    }
    // If we are moving between integer and float/decimal we don't need to do anything
    else if (oldType === trac.INTEGER && (newType === trac.FLOAT || newType === trac.DECIMAL)) {

        newValue = oldValue
    }
    // When going from float/decimal to integer we need to round
    else if ((oldType === trac.FLOAT || oldType === trac.DECIMAL) && newType === trac.INTEGER) {

        newValue = oldValue ? Math.round(Number(oldValue)) : null
    }
    // Strings can be converted into dates if it is in ISO format.
    else if (oldType === trac.STRING && typeof oldValue === "string" && newType === trac.DATE && isValidIsoDateString(oldValue)) {

        newValue = oldValue
    }
    // Strings can be converted to datetime if it is in ISO format. This assumes that the string is in ISO format
    else if (oldType === trac.STRING && typeof oldValue === "string" && newType === trac.DATETIME && isValidIsoDatetimeString(oldValue)) {

        newValue = oldValue

    } else if (isTracNumber(oldType) && typeof oldValue === "number" && newType === trac.BOOLEAN) {

        newValue = oldValue ? Boolean(Math.round(oldValue) === 1) : false

    } else if (oldType === trac.BOOLEAN && isTracNumber(newType)) {

        newValue = oldValue === true ? 1 : 0

    } else if (oldType === trac.STRING && typeof oldValue === "string" && newType === trac.BOOLEAN) {

        newValue = convertStringValueToBoolean(oldValue)

    } else if (isTracNumber(oldType) && newType === trac.BOOLEAN) {

        newValue = Boolean(oldValue === 1)

    } else if (newType === trac.BOOLEAN) {

        newValue = false

    } else if (isTracNumber(newType)) {

        newValue = 0

    } else {

        newValue = null
    }

    return newValue
}

/**
 * A function that returns the object structure and value for a Trac Tag. These vary according to the basicType and is
 * used by the createTag function when creating an attribute to add to an object in Trac.
 *
 * @remarks
 * There is a sister function to this called {@link extractValueFromTracValueObject} that does the reverse operation, it
 * receives a TRAC value object and extracts the value from it.
 *
 * @param basicType - The basic type of the attribute.
 * @param value - The value of the attribute.
 * @returns A TRAC value object.
 */
export const setTracValue = (basicType: trac.metadata.BasicType, value: null | boolean | string | number): Pick<trac.metadata.IValue, "stringValue" | "booleanValue" | "dateValue" | "datetimeValue" | "floatValue" | "decimalValue" | "integerValue"> => {

    const keyLookup: Record<number, Pick<trac.metadata.IValue, "stringValue" | "booleanValue" | "dateValue" | "datetimeValue" | "floatValue" | "decimalValue" | "integerValue">> = {
        [trac.STRING]: {"stringValue": typeof value === "string" ? value : null},
        [trac.BOOLEAN]: {"booleanValue": typeof value === "boolean" ? value : null},
        [trac.DATE]: {"dateValue": {isoDate: typeof value === "string" ? value : null}},
        [trac.DATETIME]: {"datetimeValue": {isoDatetime: typeof value === "string" ? value : null}},
        [trac.FLOAT]: {"floatValue": typeof value === "number" ? value : null},
        [trac.DECIMAL]: {"decimalValue": {decimal: typeof value === "string" ? value : null}},
        [trac.INTEGER]: {"integerValue": typeof value === "number" ? value : null}
    }

    return keyLookup[basicType]
}

/**
 * A function that takes a string or array of strings and makes it align to the TRAC standard, it removes leading and trailing
 * blanks and replaces spaces with underscores. Note that this does not fix the issues raised in the isFieldNameValid function.
 * If you use this function you will still need to ensure that the field names are unique.
 *
 * @remarks
 * Here we set multiple signatures so Typescript can handle the right conditional return type
 * https://stackoverflow.com/questions/52817922/typescript-return-type-depending-on-parameter
 *
 * @param fieldNames - The field names to standardise.
 * @returns The cleaned field names.
 */
export function standardiseStringArray<T extends string | string[]>(fieldNames: T): T;
export function standardiseStringArray(fieldNames: string | string[]): string | string[] {

    function standardise(myString: string): string {
        return myString.toUpperCase().trim().replace(/ /g, "_")
    }

    // Remove leading and trailing spaces, replace spaces with underscores and make uppercase
    return !Array.isArray(fieldNames) ? standardise(fieldNames) : fieldNames.map(fieldName => standardise(fieldName))
}

/**
 * A function that checks a string or array of strings to see if they are valid TRAC schema
 * variable names. This is needed when the user is uploading data, and we need controls over
 * what they are allowed to upload.
 *
 * @param fieldNames - The names to check.
 * @returns An object with an overall view of whether the 'fieldNames' argument is valid and if
 * not an array containing messages about the errors found.
 */
export const isFieldNameValid = (fieldNames: string | string[]): { isValid: boolean, errors: string[] } => {

    // Handle use case where fieldNames is a single string and make it an array
    const finalFieldNames = !Array.isArray(fieldNames) ? [fieldNames] : fieldNames

    let errors = finalFieldNames.map(fieldName => {

        // __EMPTY is what the XLSX plugin uses as the prefix for missing column names
        if (fieldName.substring(0, 7) === "__EMPTY" || fieldName === "") {

            return "Column names can not be blank"

        } else if (fieldName.charAt(0) === " ") {

            return "Column names can not start with space"

        } else if (fieldName.charAt(0) === "_") {

            return "Column names can not start with '_'"

        } else if (["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"].includes(fieldName.charAt(0))) {

            return "Column names can not start with numbers"

        } else if (fieldName.substring(0, 4).toUpperCase() === "TRAC") {

            return "Column names can not start with 'TRAC'"

        } else if (/[^a-zA-Z0-9_]/.test(fieldName.toString())) {

            return "Column names can only have numbers, letters and underscores"

        } else {

            return ""
        }

    }).filter(message => message !== "")

    // Make the list of errors unique
    errors = [...new Set(errors)]

    return {isValid: errors.length === 0, errors: errors}
}

/**
 * A function that checks a string to see if it is a valid TRAC attribute
 * name. This is needed when the user is defining attributes, and we need controls over
 * what they are allowed to upload. They can be defined in the {@link ApplicationSetupScene}
 * for example.
 *
 * @param attributeName - The name to check.
 * @returns An object with an overall view of whether the 'attribute' argument is valid and if
 * not an array containing messages about the errors found.
 */
export const isAttributeNameValid = (attributeName: string): { isValid: boolean, errors: string[] } => {

    let errors: string[] = []

    if (attributeName === "") {

        errors.push("Attribute names can not be blank")

    } else if (attributeName.charAt(0) === " ") {

        errors.push("Attribute names can not start with space")

    } else if (attributeName.charAt(0) === "_") {

        errors.push("Attribute names can not start with '_'")

    } else if (["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"].includes(attributeName.charAt(0))) {

        errors.push("Attribute names can not start with numbers")

    } else if (attributeName.substring(0, 4).toUpperCase() === "TRAC") {

        errors.push("Attribute names can not start with 'TRAC'")

    } else if (/[^a-zA-Z0-9_]/.test(attributeName.toString())) {

        errors.push("Attribute names can only have numbers, letters and underscores")

    }

    return {isValid: errors.length === 0, errors: errors}
}

/**
 * A function that searches through an array of options or option group and returns whether the provided option exists in the
 * list. This is pretty straight forward for an array of options but for option groups there is a little extra logic needed.
 *
 * @param selectedOption - The option to check if it exists.
 * @param options - The array of options or option group to search.
 */
export const doesOptionExist = (selectedOption: Option, options: OptionsOrGroups<GenericOption, GenericGroup>): boolean => (

    options.some(optionOrGroup => hasOwnProperty(optionOrGroup, "value") ? optionOrGroup.value === selectedOption.value : doesOptionExist(selectedOption, optionOrGroup.options))
)

/**
 * A function that searches through an option group and returns the group and position of the provided option in the
 * option group. If the option can not be found -1 is returned as the indices.
 *
 * @remarks Note that this function will return
 * the highest tier that the option is found in, so if it is present in multiple option groups the index will be for the
 * last tier it is in
 *
 * @param selectedOption - The option to check if it exists.
 * @param groups - The option group to get the position of the option from.
 */
export function getGroupOptionIndex(selectedOption: Option, groups: GenericGroup[]): { tier: number, index: number } {

    let result: { tier: number, index: number } = {tier: -1, index: -1}

    groups.forEach((group, tier) => {

        const index = group.options.findIndex(option => option.value === selectedOption.value)

        if (index > -1) {
            result.tier = tier
            result.index = index
        }
    })

    return result
}

/**
 * A function that checks whether an array of options or option group and returns whether the {@link SelectOption} component
 * has any options in it. This is pretty straight forward for an array of options but for option groups there is a little
 * extra logic needed.
 *
 * @param options - The array of options or option group to search.
 */
export const hasOptions = (options: GenericOption[] | GenericGroup[]): boolean => (

    isGroupOption(options) ? options.some(optionGroup => hasOwnProperty(optionGroup, "options") && optionGroup.options.length > 0) : options.length > 0
)

/**
 * A function that takes a set of option groups (options broken down into different categories or groups) and looks for copies of the
 * selected option. You can specify a group to keep the selected option in and hide any other duplicates across either all the other
 * groups or in s second specific group. Hidden options are marked and disabled not removed.
 * @param selectedOption - The option to check if it exists.
 * @param groups - The option group to hide duplicates in.
 * @param tierToKeep - The tier that if the selected option is found in you don't want to hide
 * @param tierToHide - The tier that if the selected option is found in you want to hide. If not
 * supplied then the option will be hidden in all tiers except for the tierToKeep value.
 */
export function hideDuplicatedGroupOptions<T extends GenericGroup[]>(selectedOption: Option, groups: T, tierToKeep: number, tierToHide?: number): T {

    if (tierToKeep > groups.length - 1) return groups

    const {tier: tier1, index: index1} = getGroupOptionIndex(selectedOption, [groups[tierToKeep]])

    if (tier1 > -1 && index1 > -1) {

        // And is it in the new search options

        // If the user has specified a particular tier to hide duplicates in (so it only hides duplicates in this
        // tier) then we only check there
        if (tierToHide !== undefined) {

            const {tier: tier2, index: index2} = getGroupOptionIndex(selectedOption, [groups[tierToHide]])
            if (tier2 > -1 && index2 > -1) {
                // Hide the duplicated option in the new search results
                groups[tierToHide].options[index2].disabled = true
            }

        } else {

            // Otherwise, check all tiers except where we are keeping the selected option in
            groups.forEach((group, index) => {

                if (index !== tierToKeep) {
                    const {tier: tier2, index: index2} = getGroupOptionIndex(selectedOption, [groups[index]])
                    if (tier2 > -1 && index2 > -1) {
                        // Hide the duplicated option in the new search results
                        groups[index].options[index2].disabled = true
                    }
                }
            })
        }
    }
    return groups
}

/**
 * A function that defines what TRAC basic types can be set as categorical. In version 0.5.0 of TRAC only strings
 * can be categorical, but this may be relaxed in future versions, in which can this function can be used to
 * update how categorical basic types are handled. An example of where this is used is when allowing the user to
 * define the schema for a dataset that they are loading.
 *
 * @param basicType - The TRAC basic type to check e.g. trac.STRING
 */
export const canBeCategorical = (basicType: null | undefined | trac.BasicType): boolean => basicType != null && isTracString(basicType)

/**
 * A function that converts a row in a dataset kinto an option for use in the {@link SelectOption} component. It can
 * also make the data unique by a property. null values in the data are removed as valid options.
 *
 * @remarks
 * In the Typescript interface T here is a generic for the types of fields in the data argument, this
 * is passed as the type of the options that is returned.
 *
 * @param row - The row of data to convert.
 * @param valueColumn - The name of the column to use to set as the value.
 * @param labelColumn - The name of the column to use to set as the label.
 * @returns An option for the SelectOption component.
 */
export function convertRowIntoOption<T extends DataValues>(row: DataRow, valueColumn: keyof DataRow, labelColumn: keyof DataRow): null | Option<Exclude<T, null>> {

    // Filter those rows that don't have the required keys and convert the data to the required structure
    if (!row.hasOwnProperty(valueColumn) || !row.hasOwnProperty(labelColumn) || row[valueColumn] == null) return null

    return {
        value: row[valueColumn],
        label: `${(row[labelColumn] || "Not set")}`

    } as Option<Exclude<T, null>>
}

/**
 * A function that converts a dataset for use in the SelectOption component. It can also make the
 * data unique by a property. null values in the data are removed as valid options.
 *
 * @remarks
 * In the Typescript interface T here is a generic for the types of fields in the data argument, this
 * is passed as the type of the options that is returned.
 *
 * @param data - The array of data to convert.
 * @param valueColumn - The name of the column to use to set as the value.
 * @param labelColumn - The name of the column to use to set as the label.
 * @param makeUnique - Whether to make the data unique by a property.
 * @param uniqueProperty - The property to make the array unique by. This can be value, label.
 */
export function convertDataIntoOptions<T extends null | string | boolean | number>(data: Record<string, T>[], valueColumn: keyof DataRow, labelColumn: keyof DataRow, makeUnique: boolean = true, uniqueProperty: "value" | "label" = 'value'): Option<Exclude<T, null>>[] {

    // If selected make the data unique by the key
    const uniqueData = makeUnique && uniqueProperty ? makeArrayOfObjectsUniqueByProperty(data, uniqueProperty) : data

    return uniqueData.filter(row => {

        return row.valueColumn !== null

        // Filtering of those rows that don't have the required keys and convert the data to the required structure
        // is done by the convertRowIntoOption function
    }).map(row => convertRowIntoOption<T>(row, valueColumn, labelColumn)).filter(isDefined)
}

/**
 * A function that returns the GitHub repository URL for a model in TRAC but can handle different applications used
 * across the industry. This first matches the model to a model repository in the client-config.json and then
 * uses this to calculate the URL for the file, if this can't be done then undefined is returned.
 *
 * @param codeRepositories - The array of model repositories defined in the client-config.json.
 * @param tag - The TRAC metadata for the model.
 * @param tenant - The tenant the user is in, model repositories are assigned to individual tenants, so we need
 * to make sure that the URL is for a model in a repository that is assigned to the current tenant in use.
 * @returns
 */
export const matchModelToRepositoryConfigAndGetUrl = (codeRepositories: CodeRepositories, tag: trac.metadata.ITag, tenant: string): undefined | string => {

    if (tag?.attrs?.trac_model_repository?.stringValue == null) {
        throw new Error("The tag object does not have the attrs property or the 'trac_model_repository' attribute is not a string")
    }

    // This is the name of the repo in the TRAC config etc/trac-platform.yaml, it is added as an attribute by TRAC
    // for models
    const tracModelRepository = tag.attrs?.trac_model_repository?.stringValue

    // The code versioning application associated with the model, we get the trac_model_repository value and look for a
    // repository in the config with that name, then we get the key it is associated with e.g. "gitHub"
    const modelRepositoryConfig = codeRepositories.find(repo => repo.tenants.includes(tenant) && repo.tracConfigName === tracModelRepository)

    // No matching repository found
    if (modelRepositoryConfig == null) {
        return
    }

    // Matching repository found but of wrong type
    if (modelRepositoryConfig?.type == null || modelRepositoryConfig?.type !== "gitHub") {
        return
    }

    // Matching repository found but missing url
    if (modelRepositoryConfig?.httpUrl == null) {
        return
    }

    // The external URL associated with the application
    const {httpUrl} = modelRepositoryConfig

    return getGitHubFileUrl(httpUrl, tag, modelRepositoryConfig)
}

/**
 * A function that constructs the URL for a file stored in GitHub using the config and metadata information. If the required information is
 * not available from the model metadata the url is not defined.
 *
 * @param url - The URL of the gitHub repository (stored in client-config.json).
 * @param tag - The metadata header for the TRAC model to link to.
 * @param repositoryConfig - The model repository definition from the client-config.json.
 * @param type - The type of file in the repository.
 * @returns The URL for the file in a GitHub repository.
 */
export const getGitHubFileUrl = (url: string, tag: trac.metadata.ITag, repositoryConfig: ModelRepository, type: string = "blob"): undefined | string => {

    // Typescript hygiene
    if (!tag.attrs) {
        throw new Error("The tag object does not have the attrs property")
    }

    const {trac_model_entry_point, trac_model_path, trac_model_version, trac_model_language} = tag.attrs

    // Typescript hygiene
    if (!trac_model_entry_point || !trac_model_language || !trac_model_version || !trac_model_path) {
        return
    }

    // Typescript hygiene
    if (!trac_model_entry_point.stringValue || !trac_model_language.stringValue || !trac_model_version.stringValue || !trac_model_path.stringValue) {
        return
    }

    if (!hasOwnProperty(General.languageExtensions, trac_model_language.stringValue)) {
        return
    }

    // If the metadata for the file comes from TRAC then there will be an entryPoint
    // property that includes the class to run the model. This needs to be removed
    // and the correct file extension added back on for the link to be valid.
    let newEntryPoint: string | string[] = trac_model_entry_point?.stringValue.split(".")

    // Remove the class at the end
    newEntryPoint.pop()
    // Change the path to the .py file
    newEntryPoint = newEntryPoint.join("/") + "." + General.languageExtensions[trac_model_language.stringValue]

    // Files in the root of the repo have "." as their path, so we have to adjust for that
    let newPath: string = trac_model_path.stringValue === "." ? "" : trac_model_path.stringValue

    return `${checkUrlEndsRight(url)}${repositoryConfig.owner}/${repositoryConfig.name}/${type}/${trac_model_version.stringValue}/${checkUrlEndsRight(newPath)}${newEntryPoint}`
}

/**
 * A function that build a style object for a table in a PDF document created by the application. This is used in the
 * PdfReport 'src/react/components/PdfReport' to get the right style for a table based on its characteristics.
 *
 * @param styles - The styles object for the pdf. This is created by applying the StyleSheet.create() function
 * to the PdfCss Object in 'src/config/config_pdf_css.ts'.
 * @param i - The row number of the row being formatted.
 * @param length - The number of rows in the table/
 * @returns A css style object.
 */
export const setPdfTableCellStyle = (styles: Record<string, Style>, i: number, length: number): Style => {

    let styleCol = styles.tableCol

    // Row striping
    if (i % 2 === 0) {
        styleCol = {...styleCol, ...styles.tableColFill}
    }

    // Top row
    if (i === 0) {
        styleCol = {...styleCol, ...styles.tableColTop}
    }

    // Bottom row
    if (i === length - 1) {
        styleCol = {...styleCol, ...styles.tableColBottom}
    }

    return styleCol
}

/**
 * A function that takes the business segment definition dataset owned by the {@link applicationSetupStore} and converts
 * it to a set of options for the {@link SelectOption} component. These options can be used when the user is searching by
 * business segment for example. It needs to be deduplicated because the business segment options dataset is structured
 * in four hierarchical tiers, and we need to reduce it to a single list of options.
 *
 * @param data - The business segment options dataset to convert.
 * @param businessSegmentsSchema - The fields in the business segment data.
 * @param addAllOption - Whether to add an "All" option to the list of options.
 * @returns A option group for the business segments.
 */
export const convertBusinessSegmentDataToOptions = (data: UiBusinessSegmentsDataRow[], businessSegmentsSchema: trac.metadata.IFieldSchema[], addAllOption: boolean = false): BusinessSegmentGroup => {

    let uniqueOptions: BusinessSegmentGroup = [
        {
            label: businessSegmentsSchema.find(variable => variable.fieldName === "GROUP_01_NAME")?.label || 'Level 1',
            options: []
        },
        {
            label: businessSegmentsSchema.find(variable => variable.fieldName === "GROUP_02_NAME")?.label || 'Level 2',
            options: []
        },
        {
            label: businessSegmentsSchema.find(variable => variable.fieldName === "GROUP_03_NAME")?.label || 'Level 3',
            options: []
        },
        {
            label: businessSegmentsSchema.find(variable => variable.fieldName === "GROUP_04_NAME")?.label || 'Level 4',
            options: []
        }
    ]

    data.forEach(row => {

        if (row.GROUP_01_ID != null && row.GROUP_01_NAME != null) uniqueOptions[0].options.push({
            value: row.GROUP_01_ID,
            label: row.GROUP_01_NAME
        })
        if (row.GROUP_02_ID != null && row.GROUP_02_NAME != null) uniqueOptions[1].options.push({
            value: row.GROUP_02_ID,
            label: row.GROUP_02_NAME
        })
        if (row.GROUP_03_ID != null && row.GROUP_03_NAME != null) uniqueOptions[2].options.push({
            value: row.GROUP_03_ID,
            label: row.GROUP_03_NAME
        })
        if (row.GROUP_04_ID != null && row.GROUP_04_NAME != null) uniqueOptions[3].options.push({
            value: row.GROUP_04_ID,
            label: row.GROUP_04_NAME
        })
    })

    uniqueOptions[0].options = sortArrayBy(makeArrayOfObjectsUnique(uniqueOptions[0].options), "label")
    uniqueOptions[1].options = sortArrayBy(makeArrayOfObjectsUnique(uniqueOptions[1].options), "label")
    uniqueOptions[2].options = sortArrayBy(makeArrayOfObjectsUnique(uniqueOptions[2].options), "label")
    uniqueOptions[3].options = sortArrayBy(makeArrayOfObjectsUnique(uniqueOptions[3].options), "label")

    if (addAllOption) uniqueOptions.unshift({label: undefined, options: [{value: "ALL", label: "All"}]})

    return uniqueOptions
}

/**
 * A function that scrolls the page to the top. This is used when the user changes scene to move the page to the top.
 * The scroll is set to happen instantaneously rather than as a smooth scroll, see the notes in the function.
 *
 * @see https://github.com/microsoft/TypeScript-DOM-lib-generator/issues/1195
 */
export const goToTop = () => {
    // The behaviour property has the options "smooth", "instant" and "auto". There is some debate about this as
    // auto should do the same as instant and some say instant is not part of the standard. Here auto doesn't
    // work and the scroll is smooth but very visible to the user. This happens I think because Bootstrap uses css
    // to apply 'smooth' as the scroll-behavior css property, I think that the application is inheriting this.
    // SO the solution is to use 'instant' which is not recognised but works.
    // @ts-ignore
    window.scrollTo({top: 0, behavior: "instant"})
}

/**
 * A function that returns whether the right or left mouse button was clicked.
 *
 * @see https://developer.mozilla.org/en-US/docs/Web/API/MouseEvent/button
 *
 * @param e - The mouse event.
 * @returns Which mouse button was clicked.
 */
export function whichMouseButtonClicked<T>(e: React.MouseEvent<T>): "right" | "left" {
    return (e.button && e.button === 2) ? "right" : "left"
}

/**
 * A function that takes a hexadecimal colour string e.g. #f9f9f9 and converts it into a rbg colour equivalent.
 *
 * @param hex - The hexadecimal colour to convert e.g. "#f9f9f9".
 * @returns The RGB values in abject.
 */
export const convertHexToRgb = (hex: string): { r: number, g: number, b: number } => {

    const result = /^#?([0-9a-fA-F]{2})([0-9a-fA-F]{2})([0-9a-fA-F]{2})$/.exec(hex)

    if (!result) throw new Error(`The string ${hex} is not a valid hexadecimal colour.`)

    return {r: parseInt(result[1], 16), g: parseInt(result[2], 16), b: parseInt(result[3], 16)}
}

/**
 * A function that gets the CSS fade to apply to a colour to be able to colour shift a colour to its ranking in
 * an ordered list. The fade is between 0 and 1.
 *
 * This is used when setting the colour to apply to a heat map in a table. The values in a column are ranked
 * and the minimum and maximum value found, then this function is used to set the CSS fade for the
 * background colour to the cell so that visually the table is heat mapped.
 *
 * @param value - The value that we want to get the fade value for.
 * @param minimumValueInRange - The minimum value in the range of the heat mapped data.
 * @param maximumValueInRange - The maximum value in the range of the heat mapped data.
 * @param fadeMinimum - The minimum value for the fade value.
 * @param fadeMaximum - The maximum value for the fade value.
 * @returns A number between 0 and 1.
 */
export const getHeatMapFade = (value: number, minimumValueInRange: number, maximumValueInRange: number, fadeMinimum: number = 0, fadeMaximum: number = 1): number => (

    (value - minimumValueInRange) / (maximumValueInRange - minimumValueInRange) * (fadeMaximum - fadeMinimum) + fadeMinimum
)

/**
 * A function that takes a value and a minimum and maximum of the range that the value is in and converts this to a
 * rgb colour relating to its position in the heat map. Although any table value can be passed in, only numeric
 * values can be assigned heat map colours.
 *
 * @param columnColours - An object containing the definition for the heat map.
 * @param columnColours.lowColour - A hexadecimal background colour for values below the inflection value.
 * @param columnColours.highColour - A hexadecimal background colour for above the inflection value.
 * @param columnColours.minimumValue - The minimum value in the range of the heat mapped data.
 * @param columnColours.maximumValue - The maximum value in the range of the heat mapped data.
 * @param columnColours.basicType - The TRAC basic type of the value.
 * @param value - The value that we want to get the colour for.
 */
export const getHeatmapColour = (columnColours: ColumnColourDefinition, value: TableValues): { background: undefined | string, color: undefined | string } => {

    const {basicType, highColour, lowColour, maximumValue, minimumValue} = columnColours

    // Convert the hexadecimal colours e.g. #f9f9f9 to a rgb equivalent
    const lowColourAsRgb = convertHexToRgb(lowColour)
    const highColourAsRgb = convertHexToRgb(highColour)

    // Strings can not have heat maps applied to them and null values array of strings in tables have no colour
    // We can't test the type of value to see if it is a string as date and datetime values  are stored as strings in TRAC
    if (isTracString(basicType) || value === null || Array.isArray(value)) return {
        background: undefined,
        color: undefined
    }

    // We can't alter the destructured arguments, well we can for primitives, but we shouldn't
    let finalMinimumValue = minimumValue
    let finalMaximumValue = maximumValue

    // If there ain't a range there ain't a heatmap
    if (finalMinimumValue === finalMaximumValue) return {background: undefined, color: undefined}

    // If we are applying heat maps to date or datetime columns then they will be ISO strings, to apply the heatmap colour we convert them to
    // seconds, so we can handle them like numbers
    if (isTracDateOrDatetime(basicType) && typeof (finalMinimumValue) === "string" && typeof (finalMaximumValue) === "string" && typeof (value) === "string") {

        finalMinimumValue = parseISO(finalMinimumValue).getTime()
        finalMaximumValue = parseISO(finalMaximumValue).getTime()
        value = parseISO(value).getTime()

    } else if (isTracBoolean(basicType) && typeof (value) === "boolean") {

        // Similar to date and datetime columns we convert booleans to numbers
        finalMinimumValue = finalMinimumValue ? 1 : 0
        finalMaximumValue = finalMaximumValue ? 1 : 0
        value = value ? 1 : 0
    }

    // This is type protection as getHeatMapFade expects a number
    if (typeof (value) !== "number" || typeof finalMinimumValue !== "number" || typeof finalMaximumValue !== "number") return {
        background: undefined,
        color: undefined
    }

    // Get the fade to apply to the colour for the number to show its rank
    const fade = getHeatMapFade(value, finalMinimumValue, finalMaximumValue)

    let diffRed = (highColourAsRgb.r - lowColourAsRgb.r) * fade + lowColourAsRgb.r
    let diffGreen = (highColourAsRgb.g - lowColourAsRgb.g) * fade + lowColourAsRgb.g
    let diffBlue = (highColourAsRgb.b - lowColourAsRgb.b) * fade + lowColourAsRgb.b

    // Get YIQ ratio see https://gomakethings.com/dynamically-changing-the-text-color-based-on-background-color-contrast-with-vanilla-js/
    const yiq = getYiq(diffRed, diffGreen, diffBlue);

    return {
        background: `rgb(${Math.round(diffRed)},${Math.round(diffGreen)},${Math.round(diffBlue)})`,
        // This is the text colour for the heatmap to make sure that the contrast is high enough to see the text
        color: (yiq >= 128) ? 'black' : 'white'
    }
}

/**
 * A function that takes a value and converts it into a rgb colour relating to whether it is above or below a transition
 * or inflection value.
 *
 * @param columnColours - An object containing the definition for the traffic light mapping.
 * @param columnColours.lowColour - A hexadecimal background colour for values below the inflection value.
 * @param columnColours.lowTextColour - A hexadecimal text colour for values below the inflection value.
 * @param columnColours.highColour - A hexadecimal background colour for above the inflection value.
 * @param columnColours.highTextColour - A hexadecimal text colour for above the inflection value.
 * @param columnColours.transitionColour - The background colour for values equal to the inflection point.
 * @param columnColours.transitionTextColour - The text colour for values equal to the inflection point.
 * @param columnColours.transitionValue - The value for the inflection point.
 * @param columnColours.basicType - The TRAC basic type of the value.
 * @param value - The value that we want to get the colour for.
 */
export const getTrafficLightColour = (columnColours: ColumnColourDefinition, value: TableValues): { background: undefined | string, color: undefined | string } => {

    const {basicType, highColour, highTextColour, lowColour, lowTextColour, transitionColour, transitionTextColour, transitionValue} = columnColours

    if (isTracString(basicType) || value === null) return {background: undefined, color: undefined}

    if (isTracBoolean(basicType)) {

        return {
            background: value === true ? highColour : lowColour,
            color: value === true ? highTextColour : lowTextColour
        }

    } else if ((isTracNumber(basicType) || isTracDateOrDatetime(basicType)) && transitionValue != null) {

        return value > transitionValue ? {
            background: highColour,
            color: highTextColour
        } : value < transitionValue ? {
            background: lowColour, color: lowTextColour
        } : {
            background: transitionColour,
            color: transitionTextColour
        }

    } else {

        return {background: undefined, color: undefined}
    }
}

/**
 * A function that takes two date or datetime values stored as ISO strings and calculates the midpoint between the two as
 * an ISO string. This is used when showing dates in traffic lights in a table for example, here we need to specify a
 * transition point for the colours which is by default the mid-point.
 *
 * @param myDateString1 - The first date for the midpoint calculation.
 * @param myDateString2 - The second date for the midpoint calculation.
 * @param format - The format of the returned ISO string, either a date or a datetime.
 * @returns The midpoint of the two dates.
 */
export const calculateMidPointOfTwoIsoDateStrings = (myDateString1: string, myDateString2: string, format: "dateIso" | "datetimeIso" = "datetimeIso"): string => {

    // Error if the strings are not valid dates
    if (isNaN(parseISO(myDateString1).getTime()) || isNaN(parseISO(myDateString2).getTime())) throw new Error("Either of the dates to calculate the midpoint of is not an ISO date string")

    return convertDateObjectToIsoString(new Date(Math.round((parseISO(myDateString1).getTime() + parseISO(myDateString2).getTime()) / 2)), format)
}

/**
 * A function that calculates the YIQ score related to a provided RGB colour. This can be used to determine whether
 * the best contrast colour should be light or dark. This is used for example when showing a column in a heat map or
 * a traffic light for example, here we need to set the colour of the text in the column to have a high contrast to
 * the background colour.
 *
 * @param r - The red index in an RGB colour.
 * @param g - The green index in an RGB colour.
 * @param b - The blue index in an RGB colour.
 * @returns The YIQ index.
 */
export const getYiq = (r: number, g: number, b: number): number => {

    // Get YIQ ratio see https://gomakethings.com/dynamically-changing-the-text-color-based-on-background-color-contrast-with-vanilla-js/
    return ((Math.round(r) * 299) + (Math.round(g) * 587) + (Math.round(b) * 114)) / 1000;
}

/**
 * A function that calculates the midpoint to use when a table is set as a traffic light. This midpoint is the transition
 * between the 'high' and 'low' colour. The value needs to be set for boolean, number, date and datetime variables,
 * string variables can not be set as traffic light columns.
 *
 * @param minimum - The minimum in the range of data.
 * @param maximum - The maximum in the range of data.
 * @param basicType - The basicType of the variable that we want to get the transition value for.
 */
export const getColumnColourTransitionValue = (minimum: string | number | boolean | undefined, maximum: string | number | boolean | undefined, basicType: trac.BasicType): number | string | null => {

    // Set the transition point as the midpoint for numbers
    if (typeof (minimum) === "number" && typeof (maximum) === "number") {

        if (minimum < 0 && maximum > 0) {
            return 0
        } else {
            return (minimum + maximum) / 2
        }

    } else if (isTracDateOrDatetime(basicType) && typeof (minimum) === "string" && typeof (maximum) === "string") {

        // Set the transition point as the midpoint for date and datetime columns
        return calculateMidPointOfTwoIsoDateStrings(minimum, maximum)

    } else {

        return null
    }
}

/**
 * A function that creates a template for an option group for running a model or a flow. It allows for results
 * from the search API to be separated from those added by the user or those added by the application as they
 * are needed to re-run a job.
 */
export function createBlankTracGroup(): TracGroup {
    return [
        {label: "Added by user", options: []},
        {label: "Loaded for re-run", options: []},
        {label: "Search results", options: []}
    ]
}

/**
 * A function that takes the array of model repositories that is defined in the client-config.json and returns those that
 * are listed as being for a particular application e.g. for GitHub.
 *
 * @param codeRepositories - The list of model repositories from the client-config.json file.
 * @param tenant - The tenant that the user is using.
 * @param repoType - The code versioning application to search for.
 * @returns The filtered list of repositories.
 */
export const getModelRepositories = (codeRepositories: CodeRepositories, tenant?: string, repoType: string = "gitHub"): ModelRepository[] => {

    return codeRepositories.filter(repo => tenant !== undefined && repo.tenants.includes(tenant) && repo.type === repoType)
}

/**
 * A function that converts a delimited string or an array of primitive types or an object of primitive types into an
 * array of strings. It also removes any blank values or null values. This is useful when you want to create a set of
 * option or where you want to create a set of badges from an array or object.
 *
 * @param variable - The variable to turn into an array.
 * @param delimiter - The delimiter to break the string up by if it is a string.
 * @returns An array of strings.
 */
export const convertAnythingToStringArray = (variable?: string | (string | boolean | number | null)[] | null | Record<string, null | string | number | boolean>, delimiter: string = "|"): string[] => {

    if (typeof variable === 'string') {

        // Remove anything which is an empty string caused by repeated delimiters
        return variable.split(delimiter).filter(variable => variable !== '')

    } else if (Array.isArray(variable)) {

        // Remove null values
        return variable.filter(isDefined).map(item => item.toString())

    } else if (isObject(variable)) {

        // For objects concatenate the key and the property value.
        return Object.entries(variable).filter(([, value]) => value != null).map(([key, value]) => `${key} : ${value != null ? value.toString() : '-'}`)

    } else {

        return []
    }
}

/**
 * A function that moves a Javascript date object to either the start or end of a given time period. For example if
 * a date is for 2021-02-14 and the format is for a quarterly date then this will be set as 2021-01-01 or 2021-03-31
 * depending on whether the start or end position is needed.
 *
 * @param date - The date to convert.
 * @param formatCodeAsString - The date format for the date.
 * @param position - What position to move the date to.
 * @returns A Javascript date at the start or end of the period.
 */
export const setDatetimePosition = (date: Date, formatCodeAsString: DateFormat | DatetimeFormat | null, position: SelectDateProps["position"]): Date => {

    if (!formatCodeAsString || !position) return date

    if (position === "start") {

        if (formatCodeAsString === "DAY") return startOfDay(date)
        else if (formatCodeAsString === "WEEK") return startOfWeek(date, {weekStartsOn: enGb.options?.weekStartsOn})
        else if (formatCodeAsString === "MONTH") return startOfMonth(date)
        else if (formatCodeAsString === "QUARTER") return startOfQuarter(date)
        else if (formatCodeAsString === "HALF_YEAR") return getMonth(date) <= 5 ? startOfQuarter(setMonth(date, 0)) : startOfQuarter(setMonth(date, 6))
        else if (formatCodeAsString === "YEAR") return startOfYear(date)

    } else if (position === "end") {

        if (formatCodeAsString === "DAY") return endOfDay(date)
        else if (formatCodeAsString === "WEEK") return endOfWeek(date, {weekStartsOn: enGb.options?.weekStartsOn})
        else if (formatCodeAsString === "MONTH") return endOfMonth(date)
        else if (formatCodeAsString === "QUARTER") return endOfQuarter(date)
        else if (formatCodeAsString === "HALF_YEAR") return getMonth(date) <= 5 ? endOfQuarter(setMonth(date, 5)) : endOfQuarter(setMonth(date, 11))
        else if (formatCodeAsString === "YEAR") return endOfYear(date)
    }

    return date
}

/**
 * A function that converts a Javascript Date object to UTC milliseconds.
 * @param d - The Javascript date to convert.
 *
 * @see https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/UTC
 */
export const convertDateObjectToUtcSeconds = (d: Date): number => {

    return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), d.getUTCHours(), d.getUTCMinutes(), d.getUTCSeconds(), d.getUTCMilliseconds())
}

/**
 * A function that works out if the UI should be usable based on an expiry date set when the project is built.
 */
export const getDaysToExpiry = (): number => (

    TRAC_UI_EXPIRY === undefined ? 999 : differenceInCalendarDays(parseISO(Buffer.from(TRAC_UI_EXPIRY, "base64").toString('utf-8')), new Date())
)

/**
 * A function that gets the expiry date for the UI based on an expiry date set when the project is built.
 */
export const getExpiry = (): undefined | Date => (

    TRAC_UI_EXPIRY === undefined ? undefined : parseISO(Buffer.from(TRAC_UI_EXPIRY, "base64").toString('utf-8'))
)

/**
 * A function that rewrites the URL when it contains parameters used to define the version of an object being viewed by the user.
 * It ensures that the object version is a number rather than the 'latest'. This is needed because if a user shares the URL for the
 * object then recipient needs to be guaranteed to see the same version of the object. If it was updated between the user sending
 * the link and the recipient viewing it then a 'latest' object ID would mean the users see different objects.
 *
 * @example
 * The user might be viewing a job: 'http://localhost:8080/trac-ui/app/view/job-summary/cb156225-b9fe-4bc2-bfbc-255e626488c0/latest/latest'.
 * This function rewrites the URL to 'http://localhost:8080/trac-ui/app/view/job-summary/cb156225-b9fe-4bc2-bfbc-255e626488c0/2/latest'
 * where 2 is the number associated with the latest version of the object being viewed.
 *
 * @param objectVersion - The object version number corresponding to the 'latest' version.
 */
export const rewriteUrlOfObjectParam = (objectVersion: number): void => {

    let newPath = window.location.pathname.split("/")
    // We don't want to change the tagVersion so if it is 'latest' we keep it as 'latest'. To do this we keep a
    // copy, and we will put it back at the end
    const tagVersion = newPath[newPath.length - 1]
    newPath.pop()
    newPath.pop()
    newPath.push(`${objectVersion}`)
    newPath.push(tagVersion)
    // replaceState does not trigger a re-render
    window.history.replaceState(null, '', newPath.join("/"));
}

/**
 * A function that takes a Javascript date and converts it to an ISO date time string
 * in the format 'YYYY-MM-DDTHH:mm:ss.sssZ'. The 'Z' specifies the UTC timezone. The
 * 'Z' is removed from the string, this is done as some APIs such as Arrow require no
 * timezone is specified and assume that it is UTC.
 *
 * @param dateValue - The Javascript date value to convert.
 */
export function isoDatetimeNoZone(dateValue: Date): string {

    const dateStr = dateValue.toISOString();
    return dateValue.toISOString().substring(0, dateStr.length - 1);
}

/**
 * A function that takes a string that should be an ISO date time string and if it has
 * 'Z' at the end removes it. The 'Z' specifies the UTC timezone. The
 * 'Z' is removed from the string, this is done as some APIs such as Arrow require no
 * timezone is specified and assume that it is UTC.
 *
 * @param dateStr - The ISO date string to remove the UTC timezone flag.
 */
export function removeIsoZone(dateStr: string): string {

    // It may be necessary to handle other zone formats here,
    // such as +01:00 or full local zone info

    return dateStr.endsWith("Z") ? dateStr.substring(0, dateStr.length - 1) : dateStr;
}