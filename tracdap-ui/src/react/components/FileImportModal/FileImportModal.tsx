/**
 * A component that allows the user to load up files stored locally on their computer into the browser.
 * There are several use cases with slightly different outcomes:
 *
 * 1. The user is loading up a dataset from a csv or Excel file into a table that's already defined. In this case the
 *  data schema is already known (e.g. the labels and the variable types) and all the user needs to do is provide a
 *  dataset with the same schema. The 'allowedSchema' prop is used to define the schema to match to.
 *
 * 2. The user is loading a dataset from a csv or Excel file without a pre-existing template. In this case the component
 * will try and guess the schema.
 *
 * 3. The user loads other types of files (e.g. json, pdf) or they want to load csv or Excel files as a file rather than
 * as a dataset. Note that the 'importAsAFile' prop can be used when loading a csv or an Excel file to say whether you
 * want to load it as a JSON object or as a file blob.
 *
 * @remarks
 * A set of text csv, xlsx, and pdf files are available in the testing folder ('tests/FileImportModal') to ensure that
 * this complex component does not have bugs. These tests must be completed if this component or its functions are changed.
 * Some of these work and some are built to test the error messages.
 *
 * @remarks
 * This component uses the {@link https://github.com/SheetJS/sheetjs|XLSX} and {@link https://www.papaparse.com|papaparse}
 * plugins to allow importing of XLSX and csv file types as datasets, if a new type is needed then new functions
 * for handling their loading will need to be added.
 *
 * @remarks
 * If a user is loading a non-csv file then this component first loads the file into memory using the
 * {@link https://developer.mozilla.org/en-US/docs/Web/API/FileReader|file reader} and then either uses a parser for
 * Excel files or uses the output directly from the file reader for other types to hand back the file to the parent
 * component. Because we load the file reader output into memory we have file size limits in place to prevent browser crashes.
 *
 * If a user is loading a csv file then we need to be able to handle loading very large datasets into TRAC. Here we
 * can't load the whole dataset into memory and send it back to the user. What we do is pretty complex, but it works.
 * We pass the selected file reference directly to a plugin called {@link https://www.papaparse.com|papaparse}. Papaparse
 * handles using a reader to chunk the data and papaparse processes each chunk, since each chunk is small we never cause the
 * browser to crash. The complexity comes from the fact that after the user selects their file they may have some intervening
 * steps to complete before loading the data into their downstream process (e.g. saving the data in TRAC). To enable
 * papaparse to handle the data processing we need to pass out the reference to the selected file to the parent component. The
 * file reference is part of the payload passed to the 'returnImportedData' function prop.
 *
 * So what's the bottom line? If you are using the FileImportModal to load csv files then you will need to load the data from
 * the file reference rather than be provided with the loaded JSON.
 *
 * @module
 * @category Component
 */

import {
    addGuessedTypeToOptions,
    calculateImportFieldsLookup,
    generateGuessedSchema,
    guessedVariableTypeUserMessages,
    guessVariableType,
    makeFinalTypeRecommendation,
    validateAllowedSchema
} from "../../utils/utils_schema_guessing";
import {Alert} from "../Alert";
import {AsyncThunk} from "@reduxjs/toolkit";
import {Button} from "../Button";
import cloneDeep from "lodash.clonedeep";
import Col from "react-bootstrap/Col";
import {commasAndAnds, commasAndOrs, convertDateObjectToIsoString, getFileExtension, isFieldNameValid, standardiseStringArray} from "../../utils/utils_general";
import {convertBasicTypeToString} from "../../utils/utils_trac_metadata";
import {convertDateObjectToFormatCode} from "../../utils/utils_formats";
import {DataRow, DataValues, FileImportModalPayload, FileSystemInfo, GuessedVariableTypes, ImportedFileSchema, Option, SelectOptionPayload} from "../../../types/types_general";
import {duplicatesInArray} from "../../utils/utils_arrays";
import {DragAndDrop} from "../DragAndDrop";
import {humanReadableFileSize} from "../../utils/utils_number";
import {Icon} from "../Icon";
import {Loading} from "../Loading";
import Modal from "react-bootstrap/Modal";
import {parse, ParseLocalConfig, ParseMeta, Parser} from 'papaparse';
import PropTypes from "prop-types";
import React, {useCallback, useReducer, useRef} from "react";
import {RootState} from "../../../storeController";
import Row from "react-bootstrap/Row";
import {SelectOption} from "../SelectOption";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../types/types_hooks";
//import * as XLSX from "xlsx";

// We define the grid for various parts of the layout as a separate object
// Layouts is the main div in the modal.
const layouts = {
    md: {span: 10, offset: 1},
    xs: {span: 12}
}

// 'layoutWithSpan'  is the status div inside 'layouts'
const layoutWithSpan = {span: 4, offset: 4}

/**
 * A function that is run in two instances. First when the component mounts to set the initial state.
 * Second when the user closes the modal it resets the state back to its initial values.
 */
const resetState = (): State => {

    return ({
        allowedFileSize: 0,
        canAbortImport: false,
        convertingFile: false,
        data: null,
        dragging: false,
        excelTabs: null,
        excelWorkbook: null,
        fileInfo: null,
        guessedVariableTypes: undefined,
        importedSchema: undefined,
        messages: {error: [], warning: []},
        progress: "0%",
        returning: false,
        selectedExcelTab: null,
        selectedFileExtension: null,
        status: ""
    })
};

/**
 * The interface for the 'action' argument in the reducer function.
 */
export type Action =

    { type: 'messages', messages: Partial<State["messages"]>, status?: State["status"], canAbortImport?: boolean } |
    { type: 'newFile', fileInfo: FileSystemInfo, allowedFileSize: number } |
    { type: 'numberOfRows', numberOfRows?: number | string } |
    { type: "importStart" | "importAborted" | "parsing" | "reset" | "parsingCsv" } |
    { type: "progress", progress: string } |
    // { type: "excelLoaded", excelTabs: Option<string>[], excelWorkbook: XLSX.WorkBook } |
    { type: "excelLoaded", excelTabs: Option<string>[], excelWorkbook: null } |
    { type: "excelTab", excelTab: State["selectedExcelTab"] } |
    {
        type: "parseComplete",
        importedSchema: State["importedSchema"],
        guessedVariableTypes: State["guessedVariableTypes"],
        data: State["data"]
    } |
    { type: "dragging", dragging: State["dragging"] } |
    { type: "returning", returning: State["returning"] }

/**
 * The reducer function that runs when the FileImportModal component updates its state. It contains the ability to
 * perform a range of types of updates.
 *
 * @param prevState - The component state before the update.
 * @param action - The action and the values to update in state.
 */
function reducer(prevState: State, action: Action): State {

    switch (action.type) {

        case "messages":
            return ({
                ...prevState,
                canAbortImport: action?.canAbortImport ?? prevState.canAbortImport,
                convertingFile: false,
                messages: {...{error: [], warning: []}, ...action.messages},
                progress: "!",
                status: action.status || prevState.status
            })

        case "newFile":
            const freshState = resetState()

            return ({
                ...freshState,
                allowedFileSize: action.allowedFileSize,
                canAbortImport: false,
                messages: {error: [], warning: []},
                progress: "0%",
                status: "Not imported",
                fileInfo: action.fileInfo,
                selectedFileExtension: action.fileInfo.fileExtension,
            })

        case "numberOfRows":

            if (prevState.fileInfo != null) {
                return ({
                    ...prevState,
                    fileInfo: {...prevState.fileInfo, ...{numberOfRows: action.numberOfRows}}
                })
            } else {
                return prevState
            }

        case "importStart":
            return ({
                ...prevState,
                convertingFile: true,
                messages: {error: [], warning: []},
                // We don't get enough progress updates with Excels for the progress to matter
                progress: prevState.selectedFileExtension === "xlsx" ? <Loading text={false}/> : "0%",
                status: "Importing...",
                canAbortImport: false,
                importedSchema: undefined,
                guessedVariableTypes: undefined,
                selectedExcelTab: null
            })

        case "importAborted":
            return ({
                ...prevState,
                convertingFile: false,
                status: "File import aborted",
                messages: {error: [], warning: []},
                progress: "0%",
                canAbortImport: false,
            })

        case "progress":
            return ({
                ...prevState,
                progress: action.progress
            })

        case "parsingCsv":
            return ({
                ...prevState,
                canAbortImport: true,
                status: "Guessing schema",
                convertingFile: true,
                progress: <Loading text={false}/>
            })

        case "parsing":
            return ({
                ...prevState,
                canAbortImport: false,
                status: "Converting file",
                convertingFile: true,
                progress: <Loading text={false}/>
            })

        case "excelLoaded":
            return ({
                ...prevState,
                excelWorkbook: action.excelWorkbook,
                status: "Loaded tab information",
                convertingFile: false,
                progress: "50%",
                excelTabs: action.excelTabs,
            })

        case "excelTab":
            return ({
                ...prevState,
                selectedExcelTab: action.excelTab,
            })

        case "parseComplete":
            return ({
                ...prevState,
                importedSchema: action.importedSchema,
                guessedVariableTypes: action.guessedVariableTypes,
                data: action.data,
                status: "Import complete",
                convertingFile: false,
                progress: "100%",
                canAbortImport: false
            })

        case "dragging":
            return ({
                ...prevState,
                dragging: action.dragging
            })

        case "returning":
            return ({
                ...prevState,
                returning: action.returning
            })

        case "reset":
            return resetState()

        default:
            throw new Error("Reducer method not recognised")
    }
}

/**
 * An interface for the FileImportModal component props.
 */
export interface Props {

    /**
     * The file extensions that the modal will allow to be imported.
     *
     * The 'allowedSchema' prop can be tested using the dedicated csv and xlsx files in
     * 'tests/FileImportModal'. The schema these test files are expecting is:
     *
     *  ```json
     *  const allowedSchema = [
     *  {fieldName: "A", fieldType: trac.BOOLEAN},
     *  {fieldName: "B", fieldType: trac.DATE},
     *  {fieldName: "C", fieldType: trac.DATETIME},
     *  {fieldName: "D", fieldType: trac.FLOAT},
     *  {fieldName: "E", fieldType: trac.DECIMAL},
     *  {fieldName: "F", fieldType: trac.INTEGER},
     *  {fieldName: "G", fieldType: trac.STRING}
     *  ]
     *  ```
     */
    allowedFileTypes: string[]
    /**
     * The schema required for the loaded dataset.
     */
    allowedSchema?: trac.metadata.IFieldSchema[]
    /**
     * A function that sends the data and schema back to the parent component so that this information can be added to a store.
     * This is for when the data needs to be sent back to a store.
     */
    dispatchedReturnImportedData?: AsyncThunk<{
        importedFile: FileImportModalPayload,
        alreadyInTrac: { foundInTrac: boolean, tag: trac.metadata.ITagHeader | null | undefined },
        existingSchemas?: Partial<{
            selectedTab: string
            options: Option<string, trac.metadata.ITag>[]
            selectedOption: null | Option<string, trac.metadata.ITag>
        }>
    }, FileImportModalPayload, { state: RootState }>
    /**
     * Whether to load the file as a file, so it can be stored in TRAC, for Excel and csv files if this is false then the data will be
     * loaded as a dataset.
     */
    importAsAFile?: boolean
    /**
     * A function that sets whether the modal is visible or not.
     */
    onToggleModal: () => void
    /**
     * A function that sends the data and schema back to the parent component so that this information can be added to a store.
     * None file data types do not need this function to be processed.
     */
    returnImportedData?: (payload: FileImportModalPayload) => void
    /**
     * Whether to show the modal.
     */
    show: boolean
    /**
     * The text for the button that returns the data and closes the modal.
     */
    uploadButtonText?: string
}

/**
 * An interface for the FileImportModal component state.
 */
export interface State {

    /**
     * The maximum size of the file that can be uploaded. This is in bytes so divide by 1024 * 1024 to get the Mb equivalent.
     * This is set differently based in the file type and whether the user is loading data  ('importAsAFile' is false) or as
     * a file ('importAsAFile' is true).
     */
    allowedFileSize: number
    /**
     * Whether the import can be aborted (i.e. an import is running and the import method allows for interruption).
     * This is useful if the user selects a very large dataset by mistake and locks the browser.
     */
    canAbortImport: boolean
    /**
     * Whether the component is converting the file, during conversion a loading icon is shown. Conversion occurs after
     * the file has been loaded into memory by the browser and is when the file is converted to a Javascript dataset.
     */
    convertingFile: boolean,
    /**
     * The data that can be imported, DataRow[] is a standard Javascript array of objects (a dataset). Blob is for an
     * imported Excel file and string is for a PDF.
     */
    data: null | Record<string, DataValues | Date>[] | Blob | Uint8Array | string
    /**
     * Whether the user is dragging a file into the DragAndDrop component.
     */
    dragging: boolean
    /**
     * The xlsx workbook (the whole file with all the tabs) stored in the XLSX plugin format. This is basically a
     * Javascript object which contains all the xlsx data and formatting information. We need to store the whole
     * file rather than just one tab to allow the user to change tab without having to reimport the file.
     */
    // excelWorkbook: null | XLSX.WorkBook
    excelWorkbook: null
    /**
     * A set of options for the tabs found in an imported Excel file.
     */
    excelTabs: null | Option<string>[]
    /**
     * The file system info about the file provided by the file reader.
     */
    fileInfo: null | FileSystemInfo
    /**
     * An object from the guessVariableTypes util function, this is a summary of the data found including the variable
     * types and their formats. It's used heavily to define the schema for the dataset. It also lists the recommended
     * options for what data types each column could be e.g. a value of "2.0" could be an integer, float, decimal,
     * string These options are used by the SchemaEditor component to allow the user to override the default.
     */
    guessedVariableTypes: undefined | GuessedVariableTypes
    /**
     * The schema for the dataset, this is either guessed or based on importedSchema if it is passed as a prop.
     */
    importedSchema?: ImportedFileSchema[]
    /**
     * An array of messages to show in the component, the colour of them (red, amber green) is set by the property.
     */
    messages: { error: string | string[], warning: string | string[] }
    /**
     * A string representation of the percentage of the local file has been loaded e.g. "50%". It can also be a loading
     * spinner component.
     */
    progress: string | React.ReactNode
    /**
     * Whether the upload has been clicked on, this sends the data back but also does some processing that can take
     * some time on larger datasets, so we need to disable the button.
     */
    returning: boolean
    /**
     * The selected Excel tab to load as a dataset.
     */
    selectedExcelTab: null | Option<string>
    /**
     * The file extension of the selected file, this is used to decide the path that the file processing goes through.
     */
    selectedFileExtension: null | string
    /**
     * The status of the loading process to show below the loading circle.
     */
    status: "" | "Not imported" | "Importing..." | "File import aborted" | "Converting file" | "Loaded tab information" | "Import complete" | "Failed" | "Guessing schema"
}

export const FileImportModal = (props: Props) => {

    console.log("Rendering FileImportModel")

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // The allowed file sizes and how mush data should be processed and viewed by default
    const {uploading} = useAppSelector(state => state["applicationStore"].clientConfig)

    // The state for the component, note that the resetState function sets the initial values.
    const [state, updateFileImportState] = useReducer(reducer, null, resetState)

    // This is a reference to the file input in the DOM if one is not provided, the useRef means that changing its value
    // does not trigger a rerender
    const fileInputRef = useRef<HTMLInputElement>(null)

    // This allows us to access the file selected to be uploaded by the user, the useRef means that changing its
    // value does not trigger a rerender
    let readerRef = useRef<FileReader>(new FileReader())

    // A reference to the Papa Parser so that we can run the abort function if needed
    let parserRef = useRef<null | Parser>(null)

    const {
        allowedFileTypes,
        allowedSchema,
        dispatchedReturnImportedData,
        importAsAFile = false,
        onToggleModal,
        returnImportedData,
        show,
        uploadButtonText = "Upload",
    } = props

    const {
        allowedFileSize,
        canAbortImport,
        convertingFile,
        data,
        excelTabs,
        excelWorkbook,
        fileInfo,
        guessedVariableTypes,
        importedSchema,
        messages,
        progress,
        returning,
        selectedExcelTab,
        selectedFileExtension,
        status
    } = state

    // Get what we need from the store
    const {userName, userId} = useAppSelector(state => state["applicationStore"].login)

    /**
     * A function that runs when the user selects a file from the 'Choose a file' button,
     * this button can lead to the user not selecting a file if they open then close the
     * file loading window, so we need to cope with the situation where no file is selected.
     */
    const onChangeFile = useCallback((event: FileList | React.ChangeEvent<HTMLInputElement>): void => {

        // Get the file selected from the payload
        const file = event instanceof FileList ? event?.[0] : event?.target.files?.[0]

        // If they selected a file
        if (file) {

            const fileExtension = getFileExtension(file.name)
            const allowedFileSize = file && fileExtension ? (uploading[importAsAFile ? "file" : "data"].sizeLimit?.[fileExtension] ?? uploading[importAsAFile ? "file" : "data"].sizeLimit.default) : 0

            // Create a payload of information about the selected file
            const fileSystemInfo: FileSystemInfo = {
                fileExtension,
                fileName: file.name,
                lastModifiedTracDate: convertDateObjectToIsoString(new Date(file.lastModified), "datetimeIso"),
                lastModifiedFormattedDate: convertDateObjectToFormatCode(new Date(file.lastModified), "DATETIME"),
                sizeAsText: humanReadableFileSize(file.size),
                sizeInBytes: file.size,
                mimeType: file.type,
                userName,
                userId
            }

            // Update the store
            updateFileImportState({type: "newFile", fileInfo: fileSystemInfo, allowedFileSize})

        } else {

            // If the user opens the file select but does not select anything then the files will be null
            // in this case the component will still be showing what was selected before so in this case
            // we reset the component state
            updateFileImportState({type: "reset"})
        }
    }, [importAsAFile, uploading, userId, userName])

    /**
     * A function that is passed to the DragAndDrop component that runs when the user
     * has dragged a file to the drop area and let go of the file. Rather than coding
     * a separate route to check and then load this file we pass it to the file input
     * ref and use the same functions that that method has to process the file.
     *
     * @param files - An array of files dragged by the user.
     */
    const onDrop = useCallback((files: FileList): void => {

        // Check that the file type is allowed.
        if (allowedFileTypes.includes(getFileExtension(files[0].name))) {

            if (!fileInputRef.current) return

            // Pass the file to the file input ref
            fileInputRef.current.files = files

            // Handle this as if the user had not dragged and dropped the file but selected it from the filesystem.
            onChangeFile(files)

        } else {

            updateFileImportState({
                type: "messages",
                messages: {
                    error: `This file import does not support the loading of ${getFileExtension(files[0].name)} files. Only .${commasAndAnds(allowedFileTypes)} extensions are supported`
                }
            })
        }
    }, [allowedFileTypes, fileInputRef, onChangeFile])

    /**
     * A function that validates the set of field names found for a dataset from either a csv or Excel file.
     *
     * @param fieldNames - The field names found in an imported file.
     * @param fileExtension - The file extension of the original file, this is used in messaging.
     */
    const validateFieldNames = useCallback((fieldNames: string[], fileExtension: string): boolean => {

        // Clean up the names from the csv/xlsx - this includes uppercase
        const standardFieldNames = standardiseStringArray(fieldNames)

        // Check if the fields found in the CSV are valid names for TRAC
        const areFieldNamesValid = isFieldNameValid(standardFieldNames)

        // The keys are standardised versions of the csv/xlsx worksheet column names and the value is the name as it
        // was in the csv, this can then be used as a lookup. It's an array as multiple column names could map to a
        // single key after the standardisation.
        // e.g. {MY_VARIABLE_1: ["my variable 1", " My_Variable_1]}
        let jsonVariables: Record<string, string[]> = {}
        fieldNames.forEach(field => {

            const standardName = standardiseStringArray(field)
            if (!jsonVariables.hasOwnProperty(standardName)) jsonVariables[standardName] = []
            jsonVariables[standardName].push(field)
        })

        // Are the field names OK and are they unique
        if (!areFieldNamesValid.isValid) {

            updateFileImportState({
                type: "messages",
                messages: {error: ["The follows errors were found with the imported file's column names:", ...areFieldNamesValid.errors]},
                status: "Not imported",
                canAbortImport: false
            })

            return false

        } else if (standardFieldNames.length !== [...new Set(standardFieldNames)].length) {

            let duplicatedFields: string[] = []

            // This is needed since multiple column names can map back to a single key in csvVariables due to the use
            // of the standardisation function. So here we need to get back to the original names of all the variables,
            // so we show the user that rather than the internal name this component gives them
            duplicatesInArray(standardFieldNames).forEach(field => {

                duplicatedFields = duplicatedFields.concat(jsonVariables[field])
            })

            updateFileImportState({
                type: "messages",
                messages: {
                    error: [`Duplicate columns were found in the ${fileExtension}. Please remove these and try again`, ...duplicatedFields]
                },
                status: "Not imported",
                canAbortImport: false
            })

            return false

        } else if (fieldNames.length === 0) {

            updateFileImportState({
                type: "messages",
                messages: {error: `No variables were found in the imported ${fileExtension}`},
                status: "Not imported",
                canAbortImport: false
            })

            return false

        } else {

            // We did not find any issues.
            return true
        }

    }, [])

    /**
     * A function that is fairly complex but that runs after the user has clicked on the import button
     * when loading a csv file or selected the tab to import when loading a xlsx file. It does multiple
     * checks on the field names of the data to make sure that it can be loaded into TRAC. The function
     * then parses the data and guesses the field types. The next step depends on whether there is a schema
     * that the dataset must adhere to. If there is then the dataset is checked against this schema or
     * if not then the schema is guessed at. Finally, the data is cleared of empty rows.
     *
     * @param fields - The list of field names for the data, passed.
     * @param data - The processed data.
     * @param fileExtension - The file type imported, used in messaging.
     */
    const processFullDataLoads = useCallback((fieldNames: Exclude<ParseMeta["fields"], undefined>, data: Record<string, DataValues | Date>[], fileExtension: string): void => {

        const fieldsAreValid = validateFieldNames(fieldNames, fileExtension)

        if (!fieldsAreValid) return

        // At this point there were no issues with the loaded csv/xlsx workbook.

        // Go through the data and try and identify the valid types that the data holds.
        // If we try to guess the variable types in the browser we very quickly get OOM errors on lower
        // spec laptops. So, to get around this we are going to user a worker thread. We pass the thread the function
        // to run (in the fileImportModalGuessVariableTypes.ts file) and wait for it to send back the result.

        // See https://webpack.js.org/guides/web-workers/
        const worker = new Worker(new URL('./fileImportModalGuessVariableTypes.ts', import.meta.url));

        // Send the function to be run by the worker the variables it needs
        worker.postMessage({data, fieldNames})

        // What to do when the worker errors
        worker.onerror = (error) => {

            console.error(error)

            updateFileImportState({
                type: "messages",
                messages: {error: ["The web worker trying to guess the variable types errored. The message was:", error.message]},
                status: "Not imported"
            })

            worker.terminate()
        }

        // What to do when the worker completes
        worker.onmessage = (e: MessageEvent) => {

            // Extract the guessed types
            let guessedVariableTypes: GuessedVariableTypes = e.data

            // If the csv/xlsx has column headers but not data guessedVariableTypes will be an empty object
            if (Object.keys(guessedVariableTypes).length === 0) {

                updateFileImportState({
                    type: "messages",
                    messages: {warning: "No data was imported, no fields were found in the data"},
                    status: "Not imported",
                    canAbortImport: false
                })

                return
            }

            // If there is an allowed schema and there is no need to tell the user what we assumed, the schema will
            // tell us the right type to use
            if (!allowedSchema) {

                const guessedVariableTypeMessages = guessedVariableTypeUserMessages(guessedVariableTypes, fileExtension)

                // Show the type validation messages, these don't stop the flow we are just messaging about what overrides we have applied
                updateFileImportState({type: "messages", messages: guessedVariableTypeMessages})
            }

            // Now we need to go through the list of variables found in the CSV and see which match the schema if
            // one is given from the parent to this component. You can think of it this way, if the user is loading
            // into an existing table we only want to retain variables in the loaded CSV that match the table.
            let importedSchema: State["importedSchema"]

            // If the user provided a schema to match against then we need to check against that
            if (allowedSchema) {

                // Do we have any mismatches between schema and data
                const fieldsLookup = calculateImportFieldsLookup(allowedSchema, fieldNames)

                // If there are case differences between the schema and the data then map the keys of guessedVariableTypes
                // to the name in the schema
                Object.entries(fieldsLookup).forEach(([schemaFieldName, lookup]) => {

                    if (schemaFieldName !== lookup.jsonName && lookup.jsonName !== undefined) {
                        guessedVariableTypes[schemaFieldName] = cloneDeep(guessedVariableTypes[lookup.jsonName])
                        delete guessedVariableTypes[lookup.jsonName]
                    }
                })

                const resultsOfAllowedSchemaValidation = validateAllowedSchema(allowedSchema, guessedVariableTypes, fieldNames)

                const {differentTypeVariables, additionalVariables, missingVariables, differentCaseVariables} = resultsOfAllowedSchemaValidation

                importedSchema = resultsOfAllowedSchemaValidation.importedSchema

                // So do we have all the variables needed in the allowed schema, if not then we message the user
                if (missingVariables.length > 0) {

                    updateFileImportState({
                        type: "messages",
                        messages: {error: ["The imported dataset does not have all of the expected columns for the dataset. The missing columns are:", ...missingVariables]},
                        status: "Not imported",
                        canAbortImport: false
                    })

                    return
                }

                // So even if we have the right number of variables are they of the right type
                if (differentTypeVariables.length > 0) {

                    updateFileImportState({
                        type: "messages",
                        messages: {error: ["The imported dataset has columns with different types to those expected. The differences are:", ...differentTypeVariables.map(item => `${item[0]} (expected ${convertBasicTypeToString(item[1])})`)],},
                        status: "Not imported",
                        canAbortImport: false
                    })

                    return
                }

                if (differentCaseVariables.length > 0 || additionalVariables.length > 0) {

                    updateFileImportState({
                        type: "messages",
                        messages: {warning: ["The imported dataset has fields with different case or additional variables to the schema, the data will be converted or ignored. The affected columns are:", ...differentCaseVariables.map(fieldName => `${fieldName} (different case)`), ...additionalVariables.map(fieldName => `${fieldName} (additional)`)]},
                        status: "Not imported",
                        canAbortImport: false
                    })
                }

            } else {

                importedSchema = generateGuessedSchema(guessedVariableTypes, fieldNames)
            }

            // Right so we have loaded the data, checked what types of data we think we have and checked the schema against
            // either an existing one that we need to match to or defined our own. Now we can process the data.

            // Wowzers we made it through
            console.log("LOG :: Processing complete")

            updateFileImportState({
                type: "parseComplete",
                importedSchema,
                data,
                guessedVariableTypes
            })

            worker.terminate()
        }


    }, [allowedSchema, validateFieldNames])

    /**
     * A function that is fairly complex but that runs after the user has clicked on the import button
     * when loading a csv file. It does multiple checks on the field names of the data to make sure that
     * it can be loaded into TRAC. The next step depends on whether there is a schema
     * that the dataset must adhere to. If there is then the dataset is checked against this schema or
     * if not then the guessed schema is used. Finally, the data is cleared of empty rows.
     *
     * @param fields - The list of field names for the data, passed.
     * @param data - The processed data.
     * @param fileExtension - The file type imported, used in messaging.
     */
    const processChunkedDataLoads = useCallback((guessedVariableTypes: GuessedVariableTypes, fieldNames: Exclude<ParseMeta["fields"], undefined>, data: Record<string, DataValues | Date>[], fileExtension: string): void => {

        const fieldsAreValid = validateFieldNames(fieldNames, fileExtension)

        if (!fieldsAreValid) return

        // If there is an allowed schema and there is no need to tell the user what we assumed, the schema will
        // tell us the right type to use
        if (!allowedSchema) {

            const guessedVariableTypeMessages = guessedVariableTypeUserMessages(guessedVariableTypes, fileExtension)

            // Show the type validation messages, these don't stop the flow we are just messaging about what overrides we have applied
            updateFileImportState({type: "messages", messages: guessedVariableTypeMessages})
        }

        // Now we need to go through the list of variables found in the CSV and see which match the schema if
        // one is given from the parent to this component. You can think of it this way, if the user is loading
        // into an existing table we only want to retain variables in the loaded CSV that match the table.
        let importedSchema: State["importedSchema"]

        // If the user provided a schema to match against then we need to check against that
        if (allowedSchema) {

            // Do we have any mismatches between schema and data
            const fieldsLookup = calculateImportFieldsLookup(allowedSchema, fieldNames)

            // If there are case differences between the schema and the data then map the keys of guessedVariableTypes
            // to the name in the schema
            Object.entries(fieldsLookup).forEach(([schemaFieldName, lookup]) => {

                if (schemaFieldName !== lookup.jsonName && lookup.jsonName !== undefined) {
                    guessedVariableTypes[schemaFieldName] = cloneDeep(guessedVariableTypes[lookup.jsonName])
                    delete guessedVariableTypes[lookup.jsonName]
                }
            })

            const resultsOfAllowedSchemaValidation = validateAllowedSchema(allowedSchema, guessedVariableTypes, fieldNames)

            const {differentTypeVariables, additionalVariables, missingVariables, differentCaseVariables} = resultsOfAllowedSchemaValidation

            importedSchema = resultsOfAllowedSchemaValidation.importedSchema

            // So do we have all the variables needed in the allowed schema, if not then we message the user
            if (missingVariables.length > 0) {

                updateFileImportState({
                    type: "messages",
                    messages: {error: ["The imported dataset does not have all of the expected columns for the dataset. The missing columns are:", ...missingVariables]},
                    status: "Not imported",
                    canAbortImport: false
                })

                return
            }

            // So even if we have the right number of variables are they of the right type
            if (differentTypeVariables.length > 0) {

                updateFileImportState({
                    type: "messages",
                    messages: {error: ["The imported dataset has columns with different types to those expected. The differences are:", ...differentTypeVariables.map(item => `${item[0]} (expected ${convertBasicTypeToString(item[1])})`)],},
                    status: "Not imported",
                    canAbortImport: false
                })

                return
            }

            if (differentCaseVariables.length > 0 || additionalVariables.length > 0) {

                updateFileImportState({
                    type: "messages",
                    messages: {warning: ["The imported dataset has fields with different case or additional variables to the schema, the data will be converted or ignored. The affected columns are:", ...differentCaseVariables.map(fieldName => `${fieldName} (different case)`), ...additionalVariables.map(fieldName => `${fieldName} (additional)`)]},
                    status: "Not imported",
                    canAbortImport: false
                })
            }

        } else {

            importedSchema = generateGuessedSchema(guessedVariableTypes, fieldNames)
        }

        // Right so we have loaded the data, checked what types of data we think we have and checked the schema against
        // either an existing one that we need to match to or defined our own. Now we can process the data.

        // Wowzers we made it through
        console.log("LOG :: Processing complete")

        updateFileImportState({
            type: "parseComplete",
            importedSchema,
            data,
            guessedVariableTypes
        })

    }, [allowedSchema, validateFieldNames])

    /**
     * A function that is run when the user clicks on the import button and when the file selected is a csv. In this situation
     * we use a plugin called papaparse to do the parsing of the csv file into an array of objects.
     *
     * The use of any as the type of the event is a hack. The type should be ProgressEvent<FileReader> however the csv
     * is read as text so the result is a string. Papaparse's parse function requires the result to be a unique symbol
     * type - however conversion of a string to a unique symbol type eluded me, and I only have so long to live.
     */
    const parseCsv = useCallback((file: File): void => {

        // Update the state to say that the file is being converted from CSV to a JSON object
        updateFileImportState({type: "parsingCsv"})

        // The fields papaparse finds in the CSV, aggregated across the rows of data
        let allFields: string[] | undefined = undefined
        // The data we build from the individual chucks, we need this to show the user a sample
        let sampleData: DataRow[] = []
        //Set up the variable that we are going to send back
        let variableTypes: GuessedVariableTypes = {}
        let numberOfRows = 0

        // The config options for the PapaParse plugin are defined here, you can see the online
        // documentation at https://www.papaparse.com for more information. The delimiter option
        // is needed to ensure that single columns can be parsed into csv files.
        const commonPapaOptions = {
            delimiter: ",",
            // dynamicTyping converts strings to their data types "2" becomes 2. We turn if off because we handle
            // the conversion in the application, there are some bugs parsing date times for example
            dynamicTyping: false,
            header: true,
            skipEmptyLines: true,
            worker: true,
            // Only up to this number will be processed, we add one to be able to know if the rows we
            // used to guess the schema were in fact all of them
            preview: uploading.data.processing.maximumRowsToUseToGuessSchema + 1
        }

        // Papa Parse config for parsing the data in full
        const papaOptionsFull: ParseLocalConfig<DataRow, File> = {
            ...commonPapaOptions,

            complete: ({meta, data}: { meta: ParseMeta, data: DataRow[], errors: { message: string, row: number }[] }): void => {

                // The fields found by papaparse in the csv
                const {fields} = meta

                if (fields !== undefined) {

                    // This is a function that checks the import, guesses the schema and then saves
                    // the data
                    processFullDataLoads(fields, data, "csv")

                } else {

                    updateFileImportState({
                        type: "messages",
                        messages: {warning: `No fields were found in the CSV, nothing was imported`}
                    })
                }
            },
            error: (err: Error) => updateFileImportState({
                type: "messages",
                messages: {error: err.message}
            })
        }

        // Papa Parse config for parsing the data in steps/chunks
        const papaOptionsStep: ParseLocalConfig<DataRow, File> = {
            ...commonPapaOptions,

            step: ({meta: {fields}, data, errors}: { meta: ParseMeta, data: DataRow, errors: { message: string, row: number }[] }, parser): void => {

                // Check if the parsing had any errors on the row
                if (errors.length > 0) {

                    const maxErrors = 10
                    // + 1 for javascript indexes starting at 0, keep a maximum of the first 10 errors
                    const errorList = errors.map(error => `${error.message} (row: ${error.row + 1})`).slice(0, maxErrors)
                    // Add a title to the list of errors
                    errorList.unshift("The following errors occurred while importing the file:")
                    // Add a ... if we had to truncate the errors
                    if (errors.length > maxErrors) errorList.push("...")

                    // Stop parsing the csv
                    parser.abort()

                    // Update the UI with the error messages
                    updateFileImportState({type: "messages", messages: {error: errorList}})

                    // Don't carry on
                    return
                }

                // Store the parser in the ref, so we can access the abort function outside this function
                if (parserRef.current === null) parserRef.current = parser

                // Only take the fields provided by the first row, we don't create a full list from
                // across every row.
                if (fields !== undefined && allFields === undefined) {
                    allFields = [...fields]
                }

                if (fields) {

                    // We add to the total row count
                    numberOfRows = numberOfRows + 1

                    // We assume all rows have all keys
                    fields.forEach(field => {

                        // We want to get the output object by the uppercase version of the key in the
                        // data so that we don't have to pass both the original (potentially mixed case)
                        // and the uppercase version around the application downstream of loading data.
                        // Downstream we want to have a single key that we know will work.
                        if (!variableTypes.hasOwnProperty(field)) {
                            variableTypes[field] = {
                                types: {found: [], recommended: []},
                                inFormats: {found: []}
                            }
                        }
                        const type = guessVariableType(data[field])

                        variableTypes = addGuessedTypeToOptions(field, type, variableTypes)
                    })
                }

                // Add the row data to the array
                if (sampleData.length < uploading.data.processing.maximumRowsToShowInSample) sampleData.push(data)
            },
            complete: (): void => {

                // Unset the parser ref
                parserRef.current = null

                if (allFields !== undefined) {

                    // Select a final schema that will work with the provided data, the user can change this
                    variableTypes = makeFinalTypeRecommendation(variableTypes)

                    //. Convert the sample data to the same types as defined in the recommended schema
                    // The 'processChunkedDataLoads' does not try and guess the types as that is already done
                    processChunkedDataLoads(variableTypes, allFields, sampleData, "csv")

                    updateFileImportState({
                        type: "numberOfRows",
                        // If we can't guarantee that we got to the end of the dataset then we don't know the number of rows
                        numberOfRows: numberOfRows <= uploading.data.processing.maximumRowsToUseToGuessSchema ? numberOfRows : `>${uploading.data.processing.maximumRowsToUseToGuessSchema}`
                    })

                } else {

                    // This is a bit horrible. We have to allow users to load data without any rows, this is still
                    // a valid dataset to load. The issue is that when chunking the data, if there are no rows, then
                    // we don't get the fields in the data returned. To get around this, if no rows are processed
                    // when chunking the data we then try and process it in as one file.
                    parse(file, papaOptionsFull)
                }
            },
            error: (error: Error): void => {

                console.error(error)

                // Unset the parser ref
                parserRef.current = null

                updateFileImportState({
                    type: "messages",
                    messages: {error: error.message}
                })
            }
        }

        // Now run the parsing of the csv using the options we specified
        // Note that we are passing the file reader file directly to papaparse so papaparse
        // handles the streaming right from the start, rather than loading from the reader
        // into memory and then processing that (which would be pointless)
        parse(file, papaOptionsStep)

    }, [processChunkedDataLoads, processFullDataLoads, uploading.data.processing.maximumRowsToShowInSample, uploading.data.processing.maximumRowsToUseToGuessSchema])

    /**
     * A function that is run when the user clicks on the import button and when the file selected is a xlsx. In this situation
     * we use a plugin called xlsx to do the parsing of the xlsx file. The object representing the file is stored in state and
     * the list of Excel tabs shown to the user to select which tab to import.
     */
    const getXlsxTabs = useCallback((event: ProgressEvent<FileReader>) => {

        // Update the state to say that the file is being converted from CSV to a JSON object
        updateFileImportState({type: "parsing"})

        // event.target.result comes from the reader onload function passing the Xlsx file read in as an array buffer
        if (event.target && event.target.result !== null && typeof event.target.result !== "string") {

            let data = new Uint8Array(event.target.result);

            // Parse the array buffer using xlsx plugin to make a Javascript object
            // const workbook = XLSX.read(data, {type: 'array', cellText: false, cellDates: true});

            // The array of options for the tabs (if loading a tab to create a dataset) or otherwise the whole file is loaded
            // so that it can be stored in TRAC
            //const excelTabs = convertArrayToOptions(workbook.SheetNames, false)

            // Wowzers we made it through
            updateFileImportState({
                type: "excelLoaded",
                // We don't store the workbook in the data property so that we can extract whatever tab they select and it
                // doesn't have to be re-imported if the user changes tab
                // excelWorkbook: workbook,
                excelWorkbook: null,
                // excelTabs: excelTabs
                excelTabs: []
                // excelTabs: excelTabs
            })

        } else {

            updateFileImportState({
                type: "messages",
                messages: {error: "The selected file does not appear to be a valid Excel file."}
            })
        }

    }, [])

    /**
     * A function that runs after the user changes the tab selection after they have loaded a xlsx file. The tab
     * selected indicates which tab they want to import as data.
     */
    const onSelectExcelTab = useCallback((result: SelectOptionPayload<Option<string>, false>): void => {

        updateFileImportState({type: "excelTab", excelTab: result.value})

        if (result.value && excelWorkbook) {

            // Get the field names, we need these in case the data has no rows, and we won't be able to get the keys
            let fields = [];

            // See https://stackoverflow.com/questions/34813980/getting-an-array-of-column-names-at-sheetjs
            let worksheet: Record<string, any> = {}
            // let worksheet = excelWorkbook.Sheets[result.value.value]
            for (let key in worksheet) {
                let regEx = new RegExp("^\(\\w\)(1)$");
                if (regEx.test(key)) {
                    fields.push(worksheet[key].v);
                }
            }

            // This gets the workbook object and parses the chosen sheet in to a json array of objects
            // See https://docs.sheetjs.com/#parsing-options
            // let sheet = XLSX.utils.sheet_to_json<DataRow>(excelWorkbook.Sheets[result.value.value], {
            //     raw: true,
            //     defval: null,
            //     dateNF: 'dd"-"mm"-"yyyy'
            // });
            let sheet: {}[] = [];

            updateFileImportState({
                type: "numberOfRows",
                numberOfRows: sheet.length
            })

            // This is a function that gets the imported data tries to guess the schema and the data types.
            processFullDataLoads(sheet.length > 0 ? Object.keys(sheet[0]) : fields, sheet, "xlsx")

        } else {

            // Reset the data to null
            updateFileImportState({
                type: "parseComplete",
                data: null,
                importedSchema: undefined,
                guessedVariableTypes: undefined
            })
        }

    }, [excelWorkbook, processFullDataLoads])

    /**
     * A function that runs after the user has selected a file to upload and clicked on the import button. It makes a
     * series of error checks and if these pass it initiates the loading of the dataset into the browser depending on
     * the file type.
     */
    const onFileImport = useCallback((): void => {

        // Get the files selected from the file input ref
        const file = fileInputRef?.current?.files?.[0]

        // Even if multiple files are dragged onto the component we process the first file only
        const selectedFileExtension = file !== undefined ? getFileExtension(file.name) : null

        // Not all browsers support the ability to load files using the method that this component uses,
        // so we need to check that the browser can perform the upload

        if (!window.FileReader) {

            updateFileImportState({
                type: "messages",
                messages: {error: ["File import is not supported in your browser"]}
            })

        } else if (!file) {

            updateFileImportState({type: "messages", messages: {error: "No file selected"}})

        } else if (selectedFileExtension == null || !allowedFileTypes.includes(selectedFileExtension)) {

            const text = selectedFileExtension == null ? "these types of" : selectedFileExtension

            updateFileImportState({
                type: "messages",
                messages: {
                    error: `This file import does not support the loading of ${text} files. Only ${commasAndAnds(allowedFileTypes)} extensions are supported`
                }
            })

        } else if (file.size > allowedFileSize) {

            updateFileImportState({
                type: "messages",
                messages: {
                    error: `The size of the selected file is ${humanReadableFileSize(file.size)}, this exceeds the maximum allowed (${humanReadableFileSize(allowedFileSize)})}`
                }
            })

        } else if (readerRef.current) {

            readerRef.current.onloadstart = () => {

                // So now if there are no errors we can finally perform the file import
                updateFileImportState({type: "importStart"})
            }

            readerRef.current.onprogress = (event: ProgressEvent<FileReader>) => {

                if (event.lengthComputable) {

                    // For Excel files the load is in two parts, the first 50% is in loading the file into the
                    // browser and the second 50% is in parsing it. So this first step is limited in the
                    // maximum progress we can show.
                    const percentageLoaded = ["xlsx"].includes(selectedFileExtension) ? Math.round((event.loaded * 50) / event.total) + "%" : Math.round((event.loaded * 100) / event.total) + "%"
                    updateFileImportState({type: "progress", progress: percentageLoaded})
                }
            }

            // This defines a set of functions to run at various points of the file loading, these vary by file type. Once
            // the file is loaded into the reader (effectively into the browser memory) then these functions are kicked off.
            readerRef.current.onload = (event: ProgressEvent<FileReader>) => {

                if (!importAsAFile && selectedFileExtension === "xlsx") {

                    getXlsxTabs(event)

                } else if (selectedFileExtension === "json") {

                    parseJson(event)

                } else {

                    if (event?.target?.result) {

                        updateFileImportState({
                            type: "parseComplete",
                            data: new Uint8Array(event.target.result as ArrayBuffer),
                            importedSchema: undefined,
                            guessedVariableTypes: undefined
                        })
                    }
                }
            }

            readerRef.current.onerror = (event: ProgressEvent<FileReader>) => {

                if (event && event.target && event.target.error) {
                    updateFileImportState({type: "messages", messages: {error: event.target.error.message}})
                } else {
                    updateFileImportState({
                        type: "messages",
                        messages: {error: "An unknown error occurred while loading this file"}
                    })
                }
            }

            // Initiate the loading of the file
            if (!importAsAFile && selectedFileExtension === "csv") {

                // This function is not loading the full file it is instead trying to guess the variable types
                // and process a sample of rows to show the user. The full dataset is not actually parsed for
                // loading until the user clicks on 'upload'. We don't use the reader to load the file we use
                // a plugin called Papa Parse that will do that for us.
                parseCsv(file)

            } else if (selectedFileExtension === "json") {

                readerRef.current.readAsText(file)

            } else if (!importAsAFile && selectedFileExtension === "xlsx") {

                readerRef.current.readAsArrayBuffer(file);

            } else {

                readerRef.current.readAsArrayBuffer(file)
            }
        }

    }, [allowedFileTypes, allowedFileSize, importAsAFile, getXlsxTabs, parseCsv])

    /**
     * A function that is run when the user clicks on the import button and when the file selected is a json. In this situation
     * the reading is done in the onFileImport function and there is no further work to do.
     *
     * @param event - The result of the onload event from reading the file.
     */
    const parseJson = (event: ProgressEvent<FileReader>): void => {

        updateFileImportState({type: "parsing"})

        // The csv and excel loaders will error if they try and load files that are not xlsx and csv (i.e. the
        // extension has been changed) but not the json as that is just read as a string. So we do some additional
        // checks on the loaded file ourselves here.
        try {
            // The parse function will error if it is not valid
            if (typeof event.target?.result === "string" && JSON.parse(event.target?.result)) {

                updateFileImportState({
                    type: "parseComplete",
                    // Note that we store the string version not the converted json object, so we can show the formatted
                    // version in a text input to the user.
                    data: event.target?.result,
                    importedSchema: undefined,
                    guessedVariableTypes: undefined
                })

            } else {

                updateFileImportState({
                    type: "messages",
                    messages: {error: "The selected file does not appear to be a valid json."}
                })
            }

        } catch (error) {

            console.error(error)
            const messages = ["The selected file does not appear to be a valid json."]

            if (typeof error === "string") {
                messages.push(error)
            } else if (error instanceof Error) {
                messages.push(error.message)
            }

            updateFileImportState({
                type: "messages",
                messages: {error: messages}
            })
        }
    }

    /**
     * If the user picks a really large file to import, and they regret this decision then we allow the
     * import to be aborted
     */
    const abortImport = () => {

        if (readerRef.current?.abort) {
            readerRef.current.abort()
            updateFileImportState({type: "importAborted"})
        }

        if (parserRef.current?.abort) {
            parserRef.current.abort()
            updateFileImportState({type: "importAborted"})
        }
    }

    /**
     * A function that runs when the user clicks to close the modal. This does some error checking and then processes the
     * data. The data is then returned to the parent component via a function that is passed to this component as a prop.
     */
    const returnData = (): void => {

        // Get the files selected from the file input ref
        const file = fileInputRef.current?.files?.[0]

        // Error checking
        if (!file) {

            updateFileImportState({
                type: "messages",
                messages: {error: "No file selected"}
            })

            return

        } else if (convertingFile) {

            updateFileImportState({
                type: "messages",
                messages: {warning: "The selected file has not completed importing"}
            })

            return

        } else if (progress !== "100%" && selectedExcelTab == null && excelTabs !== null && selectedFileExtension === "xlsx") {

            updateFileImportState({
                type: "messages",
                messages: {warning: "Please select a tab to load"}
            })

            return

        } else if (progress !== "100%") {

            updateFileImportState({
                type: "messages",
                messages: {error: "The selected file has not been imported successfully"}
            })

            return

        } else if (selectedFileExtension !== "pdf" && selectedFileExtension !== "json" && !Array.isArray(data)) {

            updateFileImportState({
                type: "messages",
                messages: {error: "No data was imported, no rows of data were found"}
            })

            return
        }

        // Yee-ha we can send the loaded data and the information about it back to whatever component uses this import.
        // Well done everyone. The last thing to do is convert the data to the required data types as PapaParse will load
        // them as string
        if ((selectedFileExtension === "csv" || (selectedFileExtension === "xlsx" && !importAsAFile)) && fileInfo) {

            // Remove empty rows that have no data in them. Although skipEmptyLines is true in papaparse this is not removing empty lines
            // it removes completely empty rows (rather than rows where all the vales are blank). We then keep only the values in
            // the agreed schema

            if (Array.isArray(data) && importedSchema && guessedVariableTypes) {

                // If we try to do this processing in the browser we very quickly get OOM errors. So, to get
                // around this we are going to user a worker thread. We pass the thread the function to run
                // (in the fileImportModalProcessing.ts file) and wait for it to send back the result.

                // Update the store
                updateFileImportState({type: "returning", returning: true})

                // See https://webpack.js.org/guides/web-workers/
                const worker = new Worker(new URL('./fileImportModalProcessing.ts', import.meta.url));

                // Send the function to be run by the worker the variables it needs
                worker.postMessage({data, importedSchema})

                // What to do when the worker errors
                worker.onerror = (error) => {

                    console.error(error)

                    // Update the store
                    updateFileImportState({type: "returning", returning: false})

                    updateFileImportState({
                        type: "messages",
                        messages: {error: ["The web worker trying to process the data into the desired format errored. The message was:", error.message]},
                        status: "Failed"
                    })
                    worker.terminate()
                }
                // What to do when the worker completes
                worker.onmessage = (e: MessageEvent) => {

                    // Update the store
                    updateFileImportState({type: "returning", returning: false})

                    const payload: FileImportModalPayload = {
                        data: e.data,
                        schema: importedSchema,
                        fileInfo,
                        guessedVariableTypes,
                        // Whether a dataset was loaded or a whole file
                        wholeFile: false,
                        // A reference to the file selected in the form input
                        file: fileInfo.fileExtension === "csv" ? file : undefined
                    }

                    // Add the data to the store or component state upstream
                    dispatchedReturnImportedData ? dispatch(dispatchedReturnImportedData(payload)) : returnImportedData ? returnImportedData(payload) : null

                    // Close the modal
                    onToggleModal()
                    worker.terminate()
                }
            }

        } else {

            // Not csv and xlsx imports, so pdf and json
            if (data != null && !Array.isArray(data) && fileInfo) {

                const payload: FileImportModalPayload = {
                    data: data,
                    schema: undefined,
                    fileInfo,
                    guessedVariableTypes: undefined,
                    wholeFile: true
                }

                // Add the data to the store or component state upstream
                dispatchedReturnImportedData ? dispatch(dispatchedReturnImportedData(payload)) : returnImportedData ? returnImportedData(payload) : null

                // Close the modal
                onToggleModal()
            }
        }
    }

    // The file selector requires the extensions to have a '.' at the start
    const acceptedFileTypes = allowedFileTypes.map(filetype => "." + filetype).join(",")

    return (

        <Modal onHide={onToggleModal}
               show={show}
               className={"file-import-modal"}
            // Reset the state when the modal is closed
               onExit={() => updateFileImportState({type: "reset"})}
            // Prevent the modal closing by clicking outside
               backdrop={convertingFile || returning ? "static" : undefined}
               keyboard={false}
        >

            <Modal.Header closeButton={Boolean(!convertingFile && !returning)}>
                <Modal.Title>
                    Upload data from {commasAndOrs(allowedFileTypes)} files
                </Modal.Title>
            </Modal.Header>

            <Modal.Body>

                {/*Add some spacing when the loading section is not showing*/}
                <Row className={!selectedFileExtension ? "mb-5" : "mb-1"}>
                    <Col xs={12} className={"text-center mt-4 d-flex justify-content-center"}>

                        <DragAndDrop onDrop={onDrop}>

                            <div>
                                <span className={"d-block mt-4"}>Drag a file here or</span>
                                <input accept={acceptedFileTypes}
                                       className={"input-file d-block"}
                                       disabled={convertingFile || returning}
                                       id={"file"}
                                       name={"file"}
                                       onChange={onChangeFile}
                                       ref={fileInputRef}
                                       type={"file"}
                                />
                                <label htmlFor={"file"} className={"btn btn-info px-5 py-4 my-3 d-inline-block"}>
                                    <Icon ariaLabel={false}
                                          icon={"bi-cursor"}
                                          className={"me-2 d-inline-block"}
                                    />
                                    Choose a file
                                </label>
                                <div className={"text-center mb-2 d-block"}>
                                    {allowedFileSize === 0 ? " " : `Maximum size ${humanReadableFileSize(allowedFileSize)}`}
                                </div>
                                <div className={"text-center mt-2 mb-3 d-block"}>
                                    {fileInfo ?
                                        <React.Fragment>
                                            You have selected <span
                                            className={"text-break"}>{fileInfo.fileName}</span>{" "}
                                            <span className={"text-nowrap"}>({fileInfo.sizeAsText})</span>
                                        </React.Fragment>
                                        : " "}
                                </div>
                            </div>

                        </DragAndDrop>
                    </Col>

                    {selectedFileExtension &&
                        <React.Fragment>
                            <Col xs={12} className={"text-center mt-4"}>
                                {status}
                            </Col>
                            <Col xs={12} md={layoutWithSpan}>
                                <div
                                    className={"mt-2 mb-4 mx-auto circle d-flex justify-content-center align-items-center border border-3 rounded-circle fs-10"}>
                                    <span>{progress}</span>
                                </div>

                            </Col>
                            <Col xs={12} md={4} className={"d-xs-none d-md-block my-auto"}>
                                {canAbortImport &&
                                    <Button ariaLabel={"Abort import"}
                                            isDispatched={false}
                                            onClick={abortImport}
                                            variant={"outline-secondary"}
                                    >
                                        <Icon ariaLabel={false}
                                              className={"me-2"}
                                              icon={"bi-x-lg"}
                                        />
                                        Abort import
                                    </Button>
                                }
                            </Col>

                            <Col xs={12} className={"text-center"}>

                                <Button ariaLabel={"Start import of file"}
                                        className={"ms-1"}
                                        disabled={convertingFile || returning}
                                        isDispatched={false}
                                        onClick={onFileImport}
                                        variant={"info"}
                                >
                                    <Icon ariaLabel={false}
                                          className={"me-2"}
                                          icon={"bi-file-earmark-arrow-down"}
                                    />
                                    {!importAsAFile ? 'Get schema' : 'Import file'}
                                </Button>

                            </Col>
                        </React.Fragment>
                    }
                </Row>

                <Row>
                    <Col md={layouts.md} xs={layouts.xs}>

                        {selectedFileExtension === "csv" &&
                            <div className={"text-center fs-13 mt-3"}>
                                {"Please note that if your data contains very small or very large values then convert these to 'number' columns rather than 'general' before saving, this is to ensure that data is not lost when Excel converts it to scientific notation."}
                            </div>
                        }

                        {selectedFileExtension === "xlsx" && !importAsAFile && excelTabs &&
                            <React.Fragment>
                                <div className={"g-1 text-center text-md-start fs-13 mt-3"}>
                                    Please select the tab in the Excel file that you want to import as a dataset.
                                    Note that the column headers must be in row 1 of the tab.
                                </div>

                                <div className={"mt-3"}>
                                    <SelectOption basicType={trac.STRING}
                                                  onChange={onSelectExcelTab}
                                                  options={excelTabs}
                                                  isDispatched={false}
                                                  validateOnMount={false}
                                                  value={selectedExcelTab}
                                    />
                                </div>
                            </React.Fragment>
                        }

                        <React.Fragment>
                            {messages.error.length > 0 &&
                                <Alert className={"mt-5 mb-3 g-0"}
                                       listHasHeader={Array.isArray(messages.error)}
                                       variant={"danger"}
                                >
                                    {messages.error}
                                </Alert>
                            }
                            {messages.warning.length > 0 &&
                                <Alert className={"mt-5 mb-3 g-0"}
                                       listHasHeader={Array.isArray(messages.warning)}
                                       variant={"warning"}
                                >
                                    {messages.warning}
                                </Alert>
                            }
                        </React.Fragment>

                    </Col>
                </Row>

            </Modal.Body>

            {selectedFileExtension &&
                <Modal.Footer className={"mt-2"}>

                    <Button ariaLabel={"Upload data"}
                            isDispatched={false}
                            loading={convertingFile || returning}
                            onClick={returnData}
                            variant={"info"}
                    >
                        <Icon ariaLabel={false}
                              className={"me-2"}
                              icon={"bi-file-earmark-arrow-up"}
                        />
                        {uploadButtonText}
                    </Button>

                </Modal.Footer>
            }

        </Modal>
    )
};

FileImportModal.propTypes = {

    allowedFileTypes: PropTypes.arrayOf(PropTypes.string),
    allowedSchema: PropTypes.arrayOf(PropTypes.shape({
        fieldName: PropTypes.string.isRequired,
        fieldType: PropTypes.number.isRequired,
        fieldOrder: PropTypes.number,
        label: PropTypes.string,
        categorical: PropTypes.bool,
        businessKey: PropTypes.bool,
    })),
    dispatchedReturnImportedData: PropTypes.func.isRequired,
    onToggleModal: PropTypes.func.isRequired,
    returnImportedData: PropTypes.func,
    show: PropTypes.bool.isRequired,
    uploadButtonText: PropTypes.string
};