/**
 * This contains functionality to execute loading and getting the names of Excel tabs from a file reference.
 */

import {convertArrayToOptions} from "../../utils/utils_arrays";
import {Option} from "../../../types/types_general";
//import * as XLSX from "xlsx";

const getExcelTabs = async (file: File) => {

    let reader = new FileReader()

    const excelTabs :DOMException | undefined | Option<string>[] = await new Promise(resolve => {

        // This defines a set of functions to run at various points of the file loading, these vary by file type. Once
        // the file is loaded into the reader (effectively into the browser memory) then these functions are kicked off.
        reader.onload = (event: ProgressEvent<FileReader>) => {

            // event.target.result comes from the reader onload function passing the Xlsx file read in as an array buffer
            if (event.target && event.target.result !== null && typeof event.target.result !== "string") {

                let data = new Uint8Array(event.target.result);

                // Parse the array buffer using xlsx plugin to make a Javascript object
                const workbook = {}
                // const workbook = XLSX.read(data, {type: 'array', cellText: false, cellDates: true});

                // The array of options for the tabs (if loading a tab to create a dataset) or otherwise the whole file is loaded
                // so that it can be stored in TRAC
                resolve(undefined)
                // resolve(convertArrayToOptions(workbook.SheetNames, false))

            } else {
                resolve(undefined)
            }
        }

        reader.onerror = (event: ProgressEvent<FileReader>) => {

            resolve( event.target?.error ?? undefined)
        }

        reader.readAsArrayBuffer(file);
    })

    return excelTabs
}

self.onmessage = async (e: MessageEvent<{ file: File}>) => {

    // The message passes in the variables needed for the doProcessing function
    const {data: {file}} = e

    // Execute the processing
    const processedData = await getExcelTabs(file)

    // Return the result
    self.postMessage(processedData)
}