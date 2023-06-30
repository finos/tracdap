/**
 * This contains functionality to execute loading a tab as a JSON dataset from a file reference.
 */

import {DataRow, DataValues, GuessedVariableTypes} from "../../../types/types_general";
// import * as XLSX from "xlsx";
import {guessVariableTypes} from "../../utils/utils_schema_guessing";
import {isFieldNameValid, standardiseStringArray} from "../../utils/utils_general";

const getExcelTabData = async (file: File, tabName: string): Promise<DOMException | undefined | { numberOfRows: number, data: Record<string, Date | DataValues>[], guessedVariableTypes: GuessedVariableTypes }> => {

    let reader = new FileReader()

    const payload: DOMException | undefined | { sheet: Record<string, Date | DataValues>[], guessedVariableTypes: GuessedVariableTypes } = await new Promise(resolve => {

        // This defines a set of functions to run at various points of the file loading, these vary by file type. Once
        // the file is loaded into the reader (effectively into the browser memory) then these functions are kicked off.
        reader.onload = (event: ProgressEvent<FileReader>) => {

            // event.target.result comes from the reader onload function passing the Xlsx file read in as an array buffer
            if (event.target && event.target.result !== null && typeof event.target.result !== "string") {

                // Parse the string converted to an array buffer using xlsx plugin to make a Javascript object
                const workbook = {};
                // const workbook = XLSX.read(new Uint8Array(event.target.result), {type: 'array', cellText: false, cellDates: true});

                // Get the field names, we need these in case the data has no rows, and we won't be able to get the keys
                let fieldNames = [];

                // See https://stackoverflow.com/questions/34813980/getting-an-array-of-column-names-at-sheetjs
                let worksheet : Record<string, any> = {}
                for (let key in worksheet) {
                    let regEx = new RegExp("^\(\\w\)(1)$");
                    if (regEx.test(key)) {
                        fieldNames.push(worksheet[key].v);
                    }
                }
                    // let worksheet = workbook.Sheets[tabName]
                // for (let key in worksheet) {
                //     let regEx = new RegExp("^\(\\w\)(1)$");
                //     if (regEx.test(key)) {
                //         fieldNames.push(worksheet[key].v);
                //     }
                // }

                // This gets the workbook object and parses the chosen sheet in to a json array of objects
                // See https://docs.sheetjs.com/#parsing-options
                let sheet = {};
                    // let sheet = XLSX.utils.sheet_to_json<DataRow>(workbook.Sheets[tabName], {
                //     raw: true,
                //     defval: null,
                //     dateNF: 'dd"-"mm"-"yyyy'
                // });

                // Clean up the names from the csv/xlsx - this includes uppercase
                const standardFieldNames = standardiseStringArray(fieldNames)

                // Check if the fields found in the CSV are valid names for TRAC
                const areFieldNamesValid = isFieldNameValid(standardFieldNames)

                // In 'jsonVariables' the keys are standardised versions of the csv/xlsx worksheet column names and the
                // value is the name as it was in the csv, this can then be used as a lookup. It's an array as multiple
                // column names could map to a single key after the standardisation.
                // e.g. {MY_VARIABLE_1: ["my variable 1", " My_Variable_1]}
                let jsonVariables: Record<string, string[]> = {}
                fieldNames.forEach(field => {

                    const standardName = standardiseStringArray(field)
                    if (!jsonVariables.hasOwnProperty(standardName)) jsonVariables[standardName] = []
                    jsonVariables[standardName].push(field)
                })

            //                 let duplicatedFields: string[] = []
            //
            // // This is needed since multiple column names can map back to a single key in csvVariables due to the use
            // // of the standardisation function. So here we need to get back to the original names of all the variables,
            // // so we show the user that rather than the internal name this component gives them
            // duplicatesInArray(standardFieldNames).forEach(field => {
            //
            //     duplicatedFields = duplicatedFields.concat(csvVariables[field])
            // })
            //
            // updateFileImportState({
            //     type: "messages",
            //     messages: {
            //         error: [`Duplicate columns were found in the ${fileExtension}. Please remove these and try again`, ...duplicatedFields]
            //     },
            //     status: "Not imported"
            // })

                if (!areFieldNamesValid) resolve(undefined)

                // Execute the processing
                const guessedVariableTypes = {}
                // const guessedVariableTypes = guessVariableTypes(sheet, fieldNames)

                // The array of options for the tabs (if loading a tab to create a dataset) or otherwise the whole file is loaded
                // so that it can be stored in TRAC
                // resolve({sheet, guessedVariableTypes})
                resolve(undefined)

            } else {
                resolve(undefined)
            }
        }

        reader.onerror = (event: ProgressEvent<FileReader>) => {

            resolve(event.target?.error ?? undefined)
        }

        reader.readAsArrayBuffer(file);
    })

    if (!(payload instanceof DOMException) && payload !== undefined) {
        return {numberOfRows: Array.isArray(payload.sheet) ? payload.sheet.length : 0, data: payload.sheet, guessedVariableTypes: payload.guessedVariableTypes}
    } else {
        return payload
    }
}

self.onmessage = async (e: MessageEvent<{ file: File, tabName: string }>) => {

    // The message passes in the variables needed for the doProcessing function
    const {data: {file, tabName}} = e

    // Execute the processing
    const payload: DOMException | undefined | { numberOfRows: number, data: Record<string, Date | DataValues>[], guessedVariableTypes: GuessedVariableTypes } = await getExcelTabData(file, tabName)

    // Return the result
    self.postMessage(payload)
}