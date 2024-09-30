import {Action as FileImportModalAction} from "./FileImportModal";
import {DataValues, GuessedVariableTypes} from "../../../types/types_general";

export const workerToGetExcelTabData = ({file, tabName, onUpdate}: { file: File, tabName: string, onUpdate: (payload: FileImportModalAction) => void }) => {

    // See https://webpack.js.org/guides/web-workers/
    const worker = new Worker(new URL('./fileImportModalGetExcelTabData.ts', import.meta.url));

    // Send the function to be run by the worker the variables it needs
    worker.postMessage({file, tabName})

    // What to do when the worker completes
    worker.onmessage = (e: MessageEvent<DOMException | undefined | {numberOfRows: number, data: Record<string, Date | DataValues>[], guessedVariableTypes: GuessedVariableTypes}>) => {

        const {data} = e

        console.log(data)

        if (data === undefined) {

            onUpdate({
                type: "messages",
                messages: {error: "The selected file does not appear to be a valid Excel file."}
            })

        } else if (data instanceof DOMException) {

            // If the reader gives us an error we return that to here and detect it.
            if (data) {
                onUpdate({type: "messages", messages: {error: data.message}})
            } else {
                onUpdate({
                    type: "messages",
                    messages: {error: "An unknown error occurred while loading this file"}
                })
            }

        } else {



            // If the csv/xlsx has column headers but not data guessedVariableTypes will be an empty object
            // if (Object.keys(guessedVariableTypes).length === 0) {
            //
            //     updateFileImportState({
            //         type: "messages",
            //         messages: {warning: "No data was imported, no fields were found in the data"},
            //         status: "Not imported",
            //         canAbortImport: false
            //     })
            //
            //     return
            // }

             onUpdate({
                type: "parseComplete",
                importedSchema: [],
                data: data.data,
                guessedVariableTypes: data.guessedVariableTypes
            })

            onUpdate({
                type: "numberOfRows",
                numberOfRows: data.numberOfRows
            })
        }

        worker.terminate()
    }

    // What to do when the worker errors
    worker.onerror = (error) => {

        console.error(error)
        onUpdate({
                type: "messages",
                messages: {error: ["The web worker trying to load the data and guess the variable types errored. The message was:", error.message]},
                status: "Not imported"
            })
        worker.terminate()
    }
}