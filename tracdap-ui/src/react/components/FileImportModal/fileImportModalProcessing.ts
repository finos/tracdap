/**
 * This contains functionality to execute processing of datasets loaded by the user using the UI in a web worker. This
 * uses a separate thread to the one the browser is using, so it does not cause the UI to freeze or become unresponsive.
 * The processing converts data from a csv or Excel file into the target schema type.
 */

import {convertRowToGuessedSchema} from "../../utils/utils_schema_guessing";
import {DataRow, ImportedFileSchema} from "../../../types/types_general";

/**
 * @param data - The dataset that is being converted.
 * @param importedSchema - The schema that the dataset needs to be converted to.
 */
const doProcessing = (data: DataRow[], importedSchema: ImportedFileSchema[]) => {

    if (!Array.isArray(data) || data.length === 0) return []

    return data.map(row => convertRowToGuessedSchema(row, importedSchema))
}

self.onmessage = (e: MessageEvent<{ data: DataRow[], importedSchema: ImportedFileSchema[] }>) => {

    // The message passes in the variables needed for the doProcessing function
    const {data: {data, importedSchema}} = e

    // Execute the processing
    const processedData = doProcessing(data, importedSchema)

    // Return the result
    self.postMessage(processedData)
}