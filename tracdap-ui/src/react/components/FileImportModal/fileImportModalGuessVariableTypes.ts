/**
 * This contains functionality to execute processing of datasets loaded by the user using the UI in a web worker. This
 * uses a separate thread to the one the browser is using, so it does not cause the UI to freeze or become unresponsive.
 */

import {guessVariableTypes} from "../../utils/utils_schema_guessing";

self.onmessage = (e) => {

    // The message passes in the variables needed for the doProcessing function
    const {data: {data, fieldNames}} = e

    // Execute the processing
    const guessedVariableTypes = guessVariableTypes(data, fieldNames)

    // Return the result
    self.postMessage(guessedVariableTypes)
}