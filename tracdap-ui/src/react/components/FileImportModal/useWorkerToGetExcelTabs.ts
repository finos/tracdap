import {Action as FileImportModalAction} from "./FileImportModal";
import {Option} from "../../../types/types_general";

export const workerToGetExcelTabs = ({file, onUpdate}: { file: File, onUpdate: (payload: FileImportModalAction) => void }) => {

    // See https://webpack.js.org/guides/web-workers/
    const worker = new Worker(new URL('./fileImportModalGetExcelTabs.ts', import.meta.url));

    onUpdate({type: "importStart"})

    // Send the function to be run by the worker the variables it needs
    worker.postMessage({file})

    // What to do when the worker completes
    worker.onmessage = (e: MessageEvent<DOMException | undefined | Option<string>[]>) => {

        const {data} = e

        // Wowzers we made it through
        if (Array.isArray(data)) {

            onUpdate({
                type: "excelLoaded",
                excelTabs: data ?? [],
                excelWorkbook: null
                // excelWorkbook: {Sheets: {}, Workbook: {}, SheetNames: []}
            })

        } else if (data === undefined) {

            onUpdate({
                type: "messages",
                messages: {error: "The selected file does not appear to be a valid Excel file."},
                status: "Not imported"
            })

        } else {

            // If the reader gives us an error we return that to here and detect it.
            if (data) {
                onUpdate({
                    type: "messages",
                    messages: {error: ["The web worker trying to load the data errored. The message was:", data.message]},
                    status: "Not imported"
                })
            } else {
                onUpdate({
                    type: "messages",
                    messages: {error: "An unknown error occurred while loading this file"},
                    status: "Not imported"
                })
            }
        }

        worker.terminate()
    }

    // What to do when the worker errors
    worker.onerror = (error) => {

        console.error(error)
        onUpdate({type: "messages", messages: {error: error.message}})
        worker.terminate()
    }
}