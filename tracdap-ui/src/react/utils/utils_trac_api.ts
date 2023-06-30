/**
 * A group of utilities using the TRAC API.
 * @category Utils
 * @module TracApiUtils
 */

import {convertObjectKeyToTagSelector, getDaysToExpiry} from "./utils_general";
import {convertMilliSecondsToHrMinSec, humanReadableFileSize} from "./utils_number";
import type {DataRow, FileSystemInfo, GetDatasetByTagResult, ImportedFileSchema, Option, StreamingDataResult, StreamingEventFunctions} from "../../types/types_general";
import differenceInMilliseconds from "date-fns/differenceInMilliseconds";
import {hasOwnProperty, isDefined} from "./utils_trac_type_chckers";
import {isObjectId, isObjectKey} from "./utils_string";
import {makeArrayOfObjectsUniqueByProperty, sortArrayBy} from "./utils_arrays";
import {normalizeRowToSchema} from "./utils_schema_guessing";
import {parse, type ParseLocalConfig, type ParseMeta} from "papaparse";
import {tracdap as trac} from "@finos/tracdap-web-api";

// transport "trac" enables streaming for data uploads and downloads
const dataTransport = trac.setup.transportForBrowser(trac.api.TracDataApi, {transport: "trac", compress: true});
// Use trac.setup to create an RPC connector pointed at your TRAC server
// The browser RPC connector will send all requests to the page origin server
const metaTransport = trac.setup.transportForBrowser(trac.api.TracMetadataApi);
const orchTransport = trac.setup.transportForBrowser(trac.api.TracOrchestratorApi);

// Create a TRAC API instance for the Metadata, Data and Orchestrator API. If the API has expired do not enable the orchApi
const dataApi = new trac.api.TracDataApi(dataTransport);
const metaApi = new trac.api.TracMetadataApi(metaTransport);
const orchApi = Boolean(getDaysToExpiry() < 0) ? undefined : new trac.api.TracOrchestratorApi(orchTransport);

/**
 * A function that makes an API request to TRAC to get a list of tenants. This function is run when
 * the application loads in the browser and is called by the {@link OnLoad} component.
 */
export const getTenants = async (): Promise<Option<string>[]> => {

    const tenantsResponse = await metaApi.listTenants(trac.api.ListTenantsRequest.create())

    // Because the Typescript definitions for the TenantInfo interface have the tenantCode and description as
    // possibly null or undefined we have to use forEach to ensure that the resultant options are all seen
    // as strings by Typescript
    const tenantOptions: Option<string>[] = []

    if (tenantsResponse.tenants != null) {

        tenantsResponse.tenants.forEach(tenant => {
            if (tenant.tenantCode != null) {
                tenantOptions.push({value: tenant.tenantCode, label: tenant.description || tenant.tenantCode})
            }
        })
    }

    return tenantOptions
}

/**
 * A function that makes an API request to TRAC to get the platform information. This information
 * includes the id of the TRAC installation, whether the installation is running in production.
 * This function is run when the application loads in the browser and is called by the {@link OnLoad}
 * component.
 */
export const getPlatformInfo = async (): Promise<trac.api.IPlatformInfoResponse> => {

    return await metaApi.platformInfo(trac.api.PlatformInfoRequest.create())
}

/**
 * An interface for the multipleSearchesBySingleClauses function.
 */
export interface MultipleSearchesByMultipleClauses {

    /**
     * Whether to include only objects with the 'show_in_search_results' tag set to
     * true.
     */
    includeOnlyShowInSearchResultsIsTrue: boolean
    /**
     * An optional key that can be associated with each search in the returned results, making it easier to
     * process the results.
     */
    keys?: string[]
    /**
     * The TRAC object type that is being searched for e.g. trac.ObjectType.DATA.
     */
    objectType: trac.ObjectType
    /**
     *  The ISO datetime to do the search as at, this allows you to go back to the application
     * as it was at a particular time.
     */
    searchAsOf: null | trac.metadata.IDatetimeValue
    /**
     * The TRAC tenant to do the search in.
     */
    tenant: string
    /**
     * An array of search terms to use to find objects in an array for the set of searches to execute.
     * Note that this is an array of search term arrays.
     */
    terms: trac.metadata.ISearchExpression[][]
}

/**
 * A function that makes multiple parallel searches in TRAC where each search consists of multiple 'and' clauses.
 * There is an option to exclude anything that does not have the 'show_in_search_results' attribute set.
 *
 * @param payload - The payload of information to search for.
 */
export const multipleSearchesByMultipleClauses = async (payload: MultipleSearchesByMultipleClauses): Promise<{ key?: string, results: trac.metadata.ITag[] }[]> => {

    const {
        includeOnlyShowInSearchResultsIsTrue,
        keys,
        objectType,
        searchAsOf,
        tenant,
        terms
    } = payload

    const hiddenCriteria = trac.metadata.SearchExpression.create({
        term: {
            attrName: "show_in_search_results",
            attrType: trac.BOOLEAN,
            operator: trac.SearchOperator.EQ,
            searchValue: {booleanValue: true}
        }
    })

    const searchPromiseArray = terms.map((searchTerms) => {

        // Put all the searches together into a single logical operator
        const logicalSearch = trac.metadata.SearchExpression.create({
            logical: {
                operator: trac.LogicalOperator.AND,
                // If show_in_search_results is true add in an additional expression to limit to only those with
                // this attribute set to true.
                expr: searchTerms.map(searchTerm => trac.metadata.SearchExpression.create(searchTerm)).concat(includeOnlyShowInSearchResultsIsTrue ? [hiddenCriteria] : [])
            }
        });

        return metaApi.search(trac.api.MetadataSearchRequest.create({
            tenant: tenant,
            searchParams: {
                searchAsOf: searchAsOf,
                objectType: objectType,
                search: logicalSearch
            }
        }))
    })

    return (await Promise.all(searchPromiseArray)).map((response, i) => ({key: keys?.[i], results: response.searchResult}))
}

/**
 * An interface for the searchByMultipleAndClauses function.
 */
export interface SearchByMultipleAndClauses {

    /**
     * Whether to include only objects with the 'show_in_search_results' tag set to
     * true.
     */
    includeOnlyShowInSearchResultsIsTrue: boolean
    /**
     * The limit of the results to return, this is set when the search results are being converted into options, and you want to
     * limit the number that come back due to performance issues.
     */
    maximumNumberOfOptionsReturned: number
    /**
     * The TRAC object type that is being searched for e.g. trac.ObjectType.DATA.
     */
    objectType: trac.ObjectType
    /**
     * The TRAC tenant to do the search in.
     */
    tenant: string
    /**
     *  The ISO datetime to do the search as at, this allows you to go back to the application
     * as it was at a particular time.
     */
    searchAsOf: null | trac.metadata.IDatetimeValue
    /**
     * An array of search terms to use to find objects.
     */
    terms: trac.metadata.ISearchExpression[]
}

/**
 * A function that makes a search in TRAC for objects with a certain set of user defined tags. There is an option to
 * exclude anything that does not have the show_in_search_results attribute set.
 *
 * @param payload - The payload of information to search for.
 */
export const searchByMultipleAndClauses = async (payload: SearchByMultipleAndClauses): Promise<trac.metadata.ITag[]> => {

    const {
        includeOnlyShowInSearchResultsIsTrue,
        maximumNumberOfOptionsReturned,
        objectType,
        searchAsOf,
        tenant,
        terms
    } = payload

    const hiddenCriteria = trac.metadata.SearchExpression.create({
        term: {
            attrName: "show_in_search_results",
            attrType: trac.BOOLEAN,
            operator: trac.SearchOperator.EQ,
            searchValue: {booleanValue: true}
        }
    })

    // Put all the searches together into a single logical operator
    const logicalSearch = trac.metadata.SearchExpression.create({
        logical: {
            operator: trac.LogicalOperator.AND,
            // If show_in_search_results is true add in an additional expression to limit to only those with
            // this attribute set to true.
            expr: terms.map(term => trac.metadata.SearchExpression.create(term)).concat(includeOnlyShowInSearchResultsIsTrue ? [hiddenCriteria] : [])
        }
    });

    const searchRequest = trac.api.MetadataSearchRequest.create({
        tenant: tenant,
        searchParams: {
            searchAsOf: searchAsOf,
            objectType: objectType,
            search: logicalSearch
        }
    })

    // Complete all the searches
    const results = (await metaApi.search(searchRequest)).searchResult

    return results.length > maximumNumberOfOptionsReturned ? results.slice(0, maximumNumberOfOptionsReturned) : results
}

/**
 * An interface for the searchByBusinessSegments function.
 */
export interface SearchByBusinessSegments {

    /**
     * The limit of the results to return, this is set when the search results are being converted into options, and you want to
     * limit the number that come back due to performance issues.
     */
    maximumNumberOfOptionsReturned: number
    /**
     * The TRAC object type that is being searched for e.g. trac.ObjectType.DATA.
     */
    objectType: trac.ObjectType
    /**
     * An array of options from the SelectOption component that define the business segments to search for.
     */
    options: Option<string>[]
    /**
     * The TRAC tenant to do the search in.
     */
    tenant: string
    /**
     * The ISO datetime to do the search as at, this allows you to go back to the application
     * as it was at a particular time.
     */
    searchAsOf: null | trac.metadata.IDatetimeValue
}

/**
 * A function that makes a search in TRAC for objects with a certain set of business segment tags set by the user.
 *
 * @param payload - The payload of information to search for.
 */
export const searchByBusinessSegments = async (payload: SearchByBusinessSegments): Promise<trac.metadata.ITag[]> => {

    const {
        maximumNumberOfOptionsReturned,
        objectType,
        tenant,
        options,
        searchAsOf
    } = payload

    const hiddenCriteria = trac.metadata.SearchExpression.create({
        term: {
            attrName: "show_in_search_results",
            attrType: trac.BOOLEAN,
            operator: trac.SearchOperator.EQ,
            searchValue: {booleanValue: true}
        }
    })

    // If the user has selected 'ALL' for a particular set of business segments then ignore that group.
    const searchCriteria = options.filter(option => option !== null && option.value !== null && option.value.toUpperCase() !== "ALL").map(option => {

        return (
            trac.metadata.SearchExpression.create({
                term: {
                    attrName: "business_segments",
                    attrType: trac.BasicType.STRING,
                    operator: trac.SearchOperator.EQ,
                    searchValue: {
                        type: {
                            basicType: trac.BasicType.STRING
                        },
                        stringValue: option.value
                    }
                }
            })
        )
    })

    const logicalSearch = trac.metadata.SearchExpression.create({
        logical: {
            operator: trac.LogicalOperator.AND,
            expr: searchCriteria.concat([hiddenCriteria])
        }
    });

    // Complete the search
    const results = (await metaApi.search(trac.api.MetadataSearchRequest.create({
        tenant: tenant,
        searchParams: {
            objectType,
            search: logicalSearch,
            searchAsOf
        }
    }))).searchResult

    return results.length > maximumNumberOfOptionsReturned ? results.slice(0, maximumNumberOfOptionsReturned) : results
}

/**
 * An interface for the importSchema function.
 */
export interface ImportSchema {
    /**
     * The attributes to add to the loaded schema.
     */
    attrs: trac.metadata.ITagUpdate[],
    /**
     * The fields to use to define the schema.
     */
    fields: trac.metadata.IFieldSchema[],
    /**
     * The selector for an existing schema to update rather than create a new schema. There are
     * restrictions in TRAC about whether a schema can be updated.
     */
    priorVersion?: trac.metadata.ITagSelector
    /**
     * The tenant to load the schema into.
     */
    tenant: string
}

/**
 * A function that imports a schema into TRAC from a JSON definition.
 *
 * @param payload - The details of what to load including attributes to add to the schema.
 */
export const importSchema = async (payload: ImportSchema): Promise<trac.metadata.ITagHeader> => {

    const {
        attrs,
        fields,
        priorVersion,
        tenant
    } = payload

    const schemaWriteRequest = trac.api.MetadataWriteRequest.create({
        tenant: tenant,
        definition: {
            objectType: trac.metadata.ObjectType.SCHEMA,
            schema: {
                partType: trac.metadata.PartType.PART_ROOT,
                schemaType: trac.metadata.SchemaType.TABLE,
                table: {fields}
            },
        },
        objectType: trac.metadata.ObjectType.SCHEMA,
        // This assumes we need to clear all the tags on the old version and then apply all the new tags
        tagUpdates: [{operation: trac.TagOperation.CLEAR_ALL_ATTR}, ...attrs],
        priorVersion: priorVersion
    })

    return await metaApi.createObject(schemaWriteRequest)
}

/**
 * An interface for the importCsvDataFromReference function.
 */
export interface ImportDataFromReference {

    /**
     * A set of attributes to attach to the dataset in TRAC.
     */
    attrs?: trac.metadata.ITagUpdate[],
    /**
     * A reference to the file to load, this is available when the user is selecting a file from a form input. We have
     * saved the reference to the file rather than read it into memory (which might cause the browser to crash).
     */
    file: File,
    /**
     * The number of rows in the dataset, if this is set then the 'onProgress' function will be run every 5% of the load completed.
     */
    numberOfRows?: number | string
    /**
     * A function to run when the loading completes.
     */
    onComplete?: Function
    /**
     * A function to run when the loading errors. But it won't right.
     */
    onError?: Function
    /**
     A function to run while the loading completes. This is triggered automatically at each 5% of the load provided the
     * 'numberOfRows' value is provided.
     */
    onProgress?: Function
    /**
     * An existing dataset tag selector that the new dataset should be created as a version of.
     */
    priorVersion?: trac.metadata.ITagSelector,
    /**
     * The schema to use for the dataset, the csv file may have different types for the variables when we load it, so we need
     * to convert the variables before loading.
     */
    schemaFields: trac.metadata.IFieldSchema[] | ImportedFileSchema[]
    /**
     * The metadata selector tag for a schema object that the user wants to use to assign as the schema of the data. When this
     * is provided TRAC will know what schema to apply, however because we need to convert the data as it is sent from the
     * CSV file reference we also need the 'schemaFields' argument to be able to convert the data. When schemaHeader is passed
     * to the function then the 'schemaFields' argument must be for the schema associated with the schema tag.
     */
    schemaHeader?: trac.metadata.ITagSelector
    /**
     * The TRAC tenant to do the search in.
     */
    tenant: string
}

/**
 * A function that loads a csv file into TRAC as a dataset. This is used for csvs since we can load very large
 * datasets by streaming them rather than loading them into memory and crashing the browser. This function combines
 * {@link https://www.papaparse.com|papaparse} with TRAC's native streaming functionality to make sure that the
 * whole dataset is never loaded into memory.
 *
 * @remarks
 * The payload includes a file blob, this is a reference to the local CSV file selected by the user to load. We do
 * not load the file into memory as that would defeat the benefit of streaming the file, instead we load directly
 * from the file reference.
 *
 * @param payload - The information needed to upload the data into TRAC.
 * @param eventFunctions - A set of functions that are executed as part of the streaming, these can be used to
 * provide feedback about the stream to the user interface.
 * @param messageSize - The maximum number of bytes for each message in the stream. When the file is being loaded
 * the data to send accumulates in a message and when this limit is reached the message is sent to the server.
 */
export const importCsvDataFromReference = async (payload: ImportDataFromReference, eventFunctions: void | StreamingEventFunctions, messageSize: number = 60000): Promise<StreamingDataResult> => {

    const {attrs, file, schemaFields, schemaHeader, tenant} = payload

    // The initial request that starts the stream
    const request0 = trac.api.DataWriteRequest.create({
        format: "text/json",
        tagUpdates: attrs,
        tenant: tenant
    });

    // If we have a 'schemaHeader' argument then the data should be loaded with the schema of the schema object. Otherwise, we
    // will use the 'schemaFields' argument to define the schema, this was originally the guessed schema.
    if (schemaHeader == undefined) {

        request0.schema = {
            partType: trac.metadata.PartType.PART_ROOT,
            schemaType: trac.metadata.SchemaType.TABLE,
            table: {fields: schemaFields}
        }

    } else {

        request0.schemaId = schemaHeader
    }

    // We have to add commas to the messages to the server, so we need to know when to start adding them
    let firstRow: boolean = true

    const useWebWorker = false;

    // Maximum message sze in bytes 1024 * 1024 is a Mb
    const maximumMessageSize = messageSize

    // Number of progress updates to give, 20 means we will update at each 5% of the rows processed
    const totalNumberOfProgressUpdates = 20

    // How many progress messages have been sent
    let progressMessagesSent = 0

    // Used to calculate the duration
    const startTime = new Date()

    // A running total of the number of rows / bytes processed
    let numberOfRowsProcessed = 0
    let numberOfBytesSent = 0;
    let numberOfBytesRead = 0;

    // Helper function for sending progress updates to the UI
    const sendProgressUpdate = () => {

        const duration = differenceInMilliseconds(new Date(), startTime)
        const durationString = duration < 1000 ? "less than 1 second" : convertMilliSecondsToHrMinSec(duration, true)

        // There is a bug in papa parse, if the parser is paused the read cursor does not update correctly
        // If we run in a web worker we don't need to pause/resume, so the number of bytes read is available

        // https://github.com/mholt/PapaParse/issues/321

        const progressMessage = useWebWorker
            ? `Sent ${numberOfRowsProcessed} rows in ${durationString} (${humanReadableFileSize(numberOfBytesRead)})`
            : `Sent ${numberOfRowsProcessed} rows in ${durationString}`

        eventFunctions?.onProgress({
            toDo: 0,
            completed: progressMessagesSent,
            message: progressMessage,
            duration: duration
        })
    }

    // Helper function for aggregating the contents of the local message buffer before sending
    function aggregateWriteRequest(buffer: Uint8Array[], bufferSize: number): trac.api.DataWriteRequest {

        const aggregate = new Uint8Array(bufferSize);
        let offset = 0;

        for (let i = 0; i < buffer.length; i++) {
            const chunk = buffer[i];
            aggregate.set(chunk, offset);
            offset += chunk.length;
        }

        return trac.api.DataWriteRequest.create({content: aggregate});
    }

    return new Promise((resolve: (value: StreamingDataResult) => void, reject: (reason?: Error | { message: string, row: number }[]) => void) => {

        // Run the start event function if provided
        eventFunctions?.onStart({completed: progressMessagesSent, message: "Starting transfer", toDo: typeof payload.numberOfRows === "number" ? totalNumberOfProgressUpdates : 0, duration: 0})

        let localBuffer: Uint8Array[] = [];
        let localBufferSize = 0;

        const encoder = new TextEncoder();
        const jsonStart = encoder.encode("[\n\t");
        const jsonEnd = encoder.encode("]");
        const jsonRowBreak = encoder.encode(",\n\t")

        localBuffer.push(jsonStart);
        localBufferSize += jsonStart.length;

        // Create the stream
        const stream = trac.setup.newStream(dataApi);
        let streamErrorFlag = false;

        // The promise is resolved when the stream is closed
        stream.createDataset(request0).then(result => {

            // Run the complete event function if provided, we do this here rather than in the 'complete' function because after the csv parses gets the last row
            // messages can have built up in TRAC, meaning that any message in the 'complete' function to say the process has completed is misleading. When the
            // resolve function is run the promise is complete, and it represents the true point as which we should message the user and free the UI.
            const duration = differenceInMilliseconds(new Date(), startTime)
            const durationString = duration < 1000 ? "less than 1 second" : convertMilliSecondsToHrMinSec(duration, true)
            eventFunctions?.onComplete({message: `Completed in ${durationString}`, toDo: totalNumberOfProgressUpdates, completed: totalNumberOfProgressUpdates, duration})

            resolve({tag: result, duration, numberOfRows: numberOfRowsProcessed})

            return

        }).catch(error => {

            console.error(error)

            streamErrorFlag = true;
            eventFunctions?.onError({message: "An error occurred", completed: progressMessagesSent, toDo: -1})
            reject(error);
        });

        // The config options for the PapaParse plugin are defined here, you can see the online
        // documentation at https://www.papaparse.com for more information. The delimiter option
        // is needed to ensure that single columns can be parsed into csv files.
        const papaOptions: ParseLocalConfig<DataRow, File> = {

            delimiter: ",",
            // dynamicTyping converts strings to their data types "2" becomes 2. We turn if off because we handle
            // the conversion in the application, there are some bugs parsing date times for example
            dynamicTyping: false,
            header: true,
            skipEmptyLines: true,
            worker: useWebWorker,

            // Step runs after every row is parsed
            step: ({meta, data: dataRow, errors}: { meta: ParseMeta, data: DataRow, errors: { message: string, row: number }[] }, parser): void => {

                // Check for errors in the upload stream
                // These are already handled so don't report the error twice, but do stop the parser
                if (streamErrorFlag) {
                    parser.abort();
                    return;
                }

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
                    // Stop the stream
                    stream.cancel();
                    // Reject the promise
                    reject(errors);

                    // Don't carry on, might not be needed
                    return
                }

                // Process the actual data in the row
                // Since these rows are going over the wire in JSON, get the dates as quoted strings
                const row = normalizeRowToSchema(dataRow, schemaFields, true)

                // Insert the row separator if required
                if (!firstRow) {
                    localBuffer.push(jsonRowBreak);
                    localBufferSize += jsonRowBreak.length;
                } else {
                    firstRow = false;
                }

                // Encode the row and push bytes into the buffer
                // It will be more efficient to aggregate buffers than strings
                // Also, we will know the exact size in bytes we are going to send

                const jsonRow = encoder.encode(JSON.stringify(row));
                localBuffer.push(jsonRow);
                localBufferSize += jsonRow.length;

                numberOfRowsProcessed += 1

                // When we hit the message size limit, we want to dispatch the contents of the buffer to TRAC
                // We can use the same break point for other processing, including UI updates

                // When the parser is running on the UI thread it hogs the event loop
                // To get around this we can pause the parser while we are dispatching messages
                // This will prevent the browser from freezing during large data uploads
                // It also has a significant impact on network performance in some browsers,
                // Notably Chrome where WebSockets events and message compression seem to run on the same event loop
                // These pauses allow frames to be dispatched when they are ready, rather than backing up in a queue

                // We can use the same break in processing for UI updates
                // This will reduce the number of updates issued (assuming the message size is relatively large)
                // It also means UI updates are issued when the event loop is not busy with parsing

                if (localBufferSize >= maximumMessageSize) {

                    // If the parser is running on the main thread, pause to allow processing of the message
                    if (!useWebWorker) {
                        parser.pause();
                        setTimeout(() => parser.resume());
                    }

                    // Aggregate the local buffer and send the message to TRAC
                    const msg = aggregateWriteRequest(localBuffer, localBufferSize);
                    stream.createDataset(msg);

                    // Update bytes processed tracking
                    // Bytes read does not work correctly with pause() / resume()
                    // https://github.com/mholt/PapaParse/issues/321
                    numberOfBytesSent += localBufferSize;
                    numberOfBytesRead = meta.cursor;

                    // Reset local buffer and counts
                    numberOfBytesSent += localBufferSize;
                    localBuffer = [];
                    localBufferSize = 0;

                    // Send progress to the UI
                    sendProgressUpdate();
                }
            },
            complete: (): void => {

                localBuffer.push(jsonEnd);
                localBufferSize += jsonEnd.length;

                const msg = aggregateWriteRequest(localBuffer, localBufferSize);
                stream.createDataset(msg);
                stream.end()

                numberOfBytesSent += localBufferSize;
                localBuffer = [];
                localBufferSize = 0;

                // Send a final progress update once all the messages are sent
                // This should show the correct number of rows / bytes, instead of whatever was in the last full message block
                // If the process completed abnormally (e.g. by abort()), then leave the error message up on the screen
                if (!streamErrorFlag)
                    sendProgressUpdate();

                console.log("LOG :: Completed streaming upload")
            },
            error: (error: Error): void => {

                console.error(error)

                // Run the error event function if provided
                streamErrorFlag = true;
                eventFunctions?.onError({message: "An error occurred", completed: progressMessagesSent, toDo: totalNumberOfProgressUpdates})
                stream.cancel();
                reject(error);
            }
        }

        // Now run the parsing of the csv using the options we specified
        // Note that we are passing the file reader file directly to papaparse so papaparse
        // handles the streaming right from the start, rather than loading from the reader
        // into memory and then processing that (which would be pointless)
        parse(file, papaOptions)
    })
}

/**
 * An interface for the importJsonDataFromStore function.
 */
export interface ImportJsonDataFromStore {

    /**
     * A set of attributes to attach to the dataset in TRAC.
     */
    attrs?: trac.metadata.ITagUpdate[],
    /**
     * The JSON data to  load.
     */
    data: DataRow[]
    /**
     * The number of rows in the dataset, if this is set then the 'onProgress' function will be run every 5% of the load completed.
     */
    numberOfRows?: number
    /**
     * A function to run when the loading completes.
     */
    onComplete?: Function
    /**
     * A function to run when the loading errors. But it won't right.
     */
    onError?: Function
    /**
     A function to run while the loading completes. This is triggered automatically at each 5% of the load provided the
     * 'numberOfRows' value is provided.
     */
    onProgress?: Function
    /**
     * An existing dataset tag selector that the new dataset should be created as a version of.
     */
    priorVersion?: trac.metadata.ITagSelector,
    /**
     * The schema to use for the dataset, the JSON loads from Excel will have already been converted when loading it using the
     * {@link FileImportModal} component. Existing schema's can be used as the reference.
     */
    schemaFields: trac.metadata.IFieldSchema[] | ImportedFileSchema[]
    /**
     * The metadata selector tag for a schema object that the user wants to use to assign as the schema of the data. When this
     * is provided TRAC will know what schema to apply.
     */
    schemaHeader?: trac.metadata.ITagSelector
    /**
     * The TRAC tenant to do the search in.
     */
    tenant: string
}

/**
 * A function that loads a JSON object into TRAC as a dataset. This is used for Excel files. although the data is
 * stored in memory we stream it to TRAC in order to make sure the file fits in the message size allowed.
 *
 * @remarks
 * Unlike {@link importCsvDataFromReference} this function passes the data to load as part of the payload, this
 * means that there is a limit to the size of the file that can be loaded using this function as it must fit into
 * the browser's memory.
 *
 * @param payload - The information needed to upload the data into TRAC.
 * @param eventFunctions - A set of functions that are executed as part of the streaming, these can be used to
 * provide feedback about the stream to the user interface.
 * @param messageSize - The maximum number of bytes for each message in the stream. When the file is being loaded
 * the data to send accumulates in a message and when this limit is reached the message is sent to the server.
 */
export const importJsonDataFromStore = async (payload: ImportJsonDataFromStore, eventFunctions: void | StreamingEventFunctions, messageSize: number = 60000): Promise<StreamingDataResult> => {

    const {attrs, data, schemaFields, schemaHeader, tenant} = payload

    // The initial request that starts the stream
    const request0 = trac.api.DataWriteRequest.create({

        tenant: tenant,
        format: "text/json",
        tagUpdates: attrs
    });

    // If we have a 'schemaHeader' argument then the data should be loaded with the schema of the schema object. Otherwise, we
    // will use the 'schemaFields' argument to define the schema, this was originally the guessed schema.
    if (schemaHeader == undefined) {

        request0.schema = {
            partType: trac.metadata.PartType.PART_ROOT,
            schemaType: trac.metadata.SchemaType.TABLE,
            table: {fields: schemaFields}
        }

    } else {

        request0.schemaId = schemaHeader
    }

    // Maximum message sze in bytes 1024 * 1024 is a Mb
    const maximumMessageSize = messageSize

    // An assumption about the bytes for each character, this allows us to estimate the message size
    const bytesPerCharacter = 1

    // Number of progress updates to give, 20 means we will update at each 5% of the rows processed
    const totalNumberOfProgressUpdates = 20

    // How many progress messages have been sent
    let progressMessagesSent = 0

    // How many characters to take for each message
    const numberOfCharactersPerMessage = Math.round(maximumMessageSize / bytesPerCharacter)

    // Used to calculate the duration
    const startTime = new Date()

    // A running total of the number of rows processed
    let numberOfChunksProcessed = 0

    let json = JSON.stringify(data)

    // Rows between each progress message
    const chunksBetweenProgressMessages = Math.ceil(json.length / numberOfCharactersPerMessage)

    return new Promise((resolve: (value: StreamingDataResult) => void, reject: (reason?: Error | { message: string, row: number }[]) => void) => {

        // Run the start event function if provided
        eventFunctions?.onStart({completed: progressMessagesSent, message: "Starting transfer", toDo: totalNumberOfProgressUpdates, duration: 0})

        // Create the stream
        const stream = trac.setup.newStream(dataApi);

        // The promise is resolved when the stream is closed
        stream.createDataset(request0).then((result) => {

                // Run the complete event function if provided, we do this here rather than in the 'complete' function because after the csv parses gets the last row
                // messages can have built up in TRAC, meaning that any message in the 'complete' function to say the process has completed is misleading. When the
                // resolve function is run the promise is complete, and it represents the true point as which we should message the user and free the UI.
                const duration = differenceInMilliseconds(new Date(), startTime)
                const durationString = duration < 1000 ? "less than 1 second" : convertMilliSecondsToHrMinSec(duration, true)
                eventFunctions?.onComplete({message: `Completed in ${durationString}`, toDo: totalNumberOfProgressUpdates, completed: totalNumberOfProgressUpdates, duration})

                resolve({tag: result, duration, numberOfRows: data.length})

                return
            }
        ).catch(reject)

        while (json.length !== 0) {

            // A count of the number of rows processed
            numberOfChunksProcessed = numberOfChunksProcessed + 1

            // Run the progress event function if provided, and we have decided to send
            if (numberOfChunksProcessed === ((progressMessagesSent + 1) * chunksBetweenProgressMessages)) {

                const duration = differenceInMilliseconds(new Date(), startTime)
                const durationString = duration < 1000 ? "less than 1 second" : convertMilliSecondsToHrMinSec(duration, true)
                progressMessagesSent = progressMessagesSent + 1
                eventFunctions?.onProgress({
                    toDo: totalNumberOfProgressUpdates,
                    completed: progressMessagesSent,
                    message: `${numberOfChunksProcessed}} in ${durationString}`,
                    duration: duration
                })
            }

            const dataChunk = json.slice(0, numberOfCharactersPerMessage)

            console.log("LOG :: Sending message")

            const msg = trac.api.DataWriteRequest.create({content: new TextEncoder().encode(dataChunk)});
            stream.createDataset(msg)

            // Set the chunk to the new message that was not sent
            json = json.slice(numberOfCharactersPerMessage)
        }

        console.log("LOG :: Completed streaming upload")

        stream.end()
        // Run the complete event function if provided
        const duration = differenceInMilliseconds(new Date(), startTime)
        const durationString = duration < 1000 ? "less than 1 second" : convertMilliSecondsToHrMinSec(duration, true)
        eventFunctions?.onComplete({message: `Completed in ${durationString}`, toDo: totalNumberOfProgressUpdates, completed: totalNumberOfProgressUpdates, duration})
    })
}

/**
 * An interface for the downloadDataAsStream function.
 */
export interface DownloadDataAsStream {

    /**
     * What type of data is needed to be returned. Note that "text/csv" requires a
     * conversion a blob to be exported as a csv.
     */
    format: "application/vnd.apache.arrow.stream" | "text/csv"
    /**
     * The maximum number of rows to return.
     */
    rowLimit?: number
    /**
     * The dataset metadata selector tag, so that we know what data to download.
     */
    tagSelector: trac.metadata.ITagSelector
    /**
     * The TRAC tenant to do the search in.
     */
    tenant: string
}

/**
 * A function that streams a dataset in TRAC into an Uint8Array that can then be downloaded as a csv
 * or converted into a JSON object. This is useful for very large datasets, however most of the time
 * you don't know what size a dataset is, so this should be used. The schema for the dataset is
 * also returned as part of the stream.
 *
 * @param payload - The information needed to download the data from TRAC.
 */
export const downloadDataAsStream = (payload: DownloadDataAsStream): Promise<{ schema: trac.metadata.ISchemaDefinition, content: Uint8Array }> => {

    const {format, tagSelector, tenant} = payload

    const request = trac.api.DataReadRequest.create({
        format,
        selector: tagSelector,
        tenant
    });

    return new Promise((resolve, reject) => {

        // Create a new stream before using a streaming operation
        const stream = trac.setup.newStream(dataApi);

        // This is what is going to aggregate the streaming messages
        const messages: trac.api.IDataReadResponse[] = [];

        // When messages come in, stash them until the stream is complete
        stream.on("data", (msg: trac.api.IDataReadResponse) => {
            messages.push(msg)
        });

        // Once the stream finishes we can aggregate the messages into a single response
        // then convert the content to a blob
        stream.on("end", () => {
            // We could convert the Uint8Array to a blob here rather than in the
            // downstream function but testing of that lead to significantly worse
            // performance in the browser
            // @ts-ignore
            resolve(trac.utils.aggregateStreamContent(messages));
        });

        stream.on("error", err => reject(err));

        // Start the actual request
        stream.readDataset(request).then()
    })
}

/**
 * An interface for the importModel function.
 */
export interface ImportModel {

    /**
     * The import model job definition, this includes the model path, entrypoint, repository
     * etc. as well as the attributes to add to the loaded model.
     */
    importDetails: trac.metadata.IImportModelJob
    /**
     * The tenant to load the schema into.
     */
    tenant: string
}

/**
 * A function that initiates a job that loads a model from the model repository defined in the TRAC config.
 *
 * @param payload - The details of what to load including attributes to add to the model.
 * @returns The submitted job status.
 */
export const importModel = async (payload: ImportModel): Promise<trac.api.JobStatus> => {

    const {importDetails, tenant} = payload

    const modelImportRequest = trac.api.JobRequest.create({
        tenant: tenant,
        job: {
            jobType: trac.JobType.IMPORT_MODEL,
            importModel: importDetails
        },
        // These attributes are not set in the setAttributesSceneStore but are hard coded
        jobAttrs: [
            {
                attrName: "key",
                value: {stringValue: "model_import", type: {basicType: trac.metadata.BasicType.STRING}}
            },
            {
                attrName: "name",
                value: {stringValue: `Model import`, type: {basicType: trac.metadata.BasicType.STRING}}
            },
            {
                attrName: "description",
                value: {
                    stringValue: importDetails.modelAttrs ? `Import of model with key "${importDetails.modelAttrs.find(attr => attr.attrName === "key")?.value?.stringValue}"` : `Import of model`,
                    type: {basicType: trac.metadata.BasicType.STRING}
                }
            },
            {
                attrName: "import_method",
                value: {stringValue: "user_interface", type: {basicType: trac.metadata.BasicType.STRING}}
            },
            {
                attrName: "show_in_search_results",
                value: {booleanValue: true, type: {basicType: trac.metadata.BasicType.BOOLEAN}}
            }
        ]
    });

    // Get the business_segments attribute from the model attributes
    const businessSegmentAttr = importDetails.modelAttrs?.find(modelAttr => modelAttr.attrName === "business_segments")

    // Copy the model business segment attribute to the model import job to align them and make
    // searching for jobs easier.
    if (businessSegmentAttr) {
        modelImportRequest.jobAttrs?.push(businessSegmentAttr)
    }

    // The orchApi is undefined when the app license has expired
    if (orchApi === undefined) throw new Error("The application is unavailable.")

    return await orchApi.submitJob(modelImportRequest);
}

/**
 * A function that takes a job request, applies the create function to it, then submits the job to
 * TRAC. We have this as a small utility here so that all the api calls are in one place.
 *
 * @param jobRequest - The job request to make.
 * @returns The submitted job status.
 */
export const submitJob = async (jobRequest: trac.api.IJobRequest): Promise<trac.api.IJobStatus> => {

    // The orchApi is undefined when the app license has expired
    if (orchApi === undefined) throw new Error("The application orchestration API is unavailable.")

    return orchApi.submitJob(trac.api.JobRequest.create(jobRequest))
}

/**
 * A function that takes an array of job requests, applies the create function to each, this creates a new
 * instance, then submits the jobs to TRAC.
 *
 * @remarks
 * This function uses Promise.allSettled rather than Promise.all which allows us to see if individual jobs failed.
 *
 * @param jobRequests - The array of job requests to make.
 * @returns The submitted job statuses.
 */
export const submitJobs = async (jobRequests: trac.api.IJobRequest[]): Promise<PromiseSettledResult<trac.api.IJobStatus>[]> => {

    // The orchApi is undefined when the app license has expired
    if (orchApi === undefined) throw new Error("The application is unavailable.")

    // A request to send all the job requests, Promise.allSettled allows requests to fail
    return Promise.allSettled(jobRequests.map(jobRequest => orchApi.submitJob(trac.api.JobRequest.create(jobRequest))))
}

/**
 * An interface for the importFlow function.
 */
export interface ImportFlow {

    /**
     * The attributes to add to the flow.
     */
    attrs: trac.metadata.ITagUpdate[]
    /**
     * The flow definition to load into TRAC.
     */
    flow: trac.metadata.IFlowDefinition
    /**
     * An existing flow tag selector that the new flow should be created as a version of.
     */
    priorVersion?: trac.metadata.ITagSelector
    /**
     * The tenant to load the schema into.
     */
    tenant: string
}

/**
 * A function that initiates a job that loads a flow from a local json file. The job is
 * first validated to check that it is well-formed. This updates an existing flow.
 *
 * @param payload - The details of what to load including attributes to add to the flow.
 */
export const importFlow = async (payload: ImportFlow): Promise<trac.metadata.ITagHeader> => {

    const {attrs, flow, priorVersion, tenant} = payload

    const flowWriteRequest = trac.api.MetadataWriteRequest.create({
        definition: {objectType: trac.metadata.ObjectType.FLOW, flow: flow},
        objectType: trac.metadata.ObjectType.FLOW,
        priorVersion,
        // This assumes we need to clear all the tags on the old version and then apply all the new tags
        tagUpdates: [{operation: trac.TagOperation.CLEAR_ALL_ATTR}, ...attrs],
        tenant
    })

    return await metaApi.createObject(flowWriteRequest)
}

/**
 * An interface for the isModelLoaded function.
 */
export interface IsModelLoaded {

    /**
     * The information about the model to search for.
     */
    modelDetails: Pick<trac.metadata.IImportModelJob, "repository" | "path" | "entryPoint" | "version">
    /**
     *  The ISO datetime to do the search as at, this allows you to go back to the application
     * as it was at a particular time.
     */
    searchAsOf: null | trac.metadata.IDatetimeValue
    /**
     * Whether to search for the same commit that the user is loading from. If true then loading the model from the
     * same commit will be forbidden but loading the same model from a different commit is allowed. When false loading
     * the same model from any commit is not allowed (not used here).
     */
    searchByCommit: boolean
    /**
     * The TRAC tenant to do the search in.
     */
    tenant: string
}

/**
 * A function that makes a search in TRAC for a model with the same GitHub parameters (model entry point and commit for example)
 * as the model that the user is trying to/has just loaded. If a version of the model is found then the user is not
 * allowed to upload the same model.
 *
 * @param payload - The payload of model information to search for.
 */
export const isModelLoaded = async (payload: IsModelLoaded): Promise<trac.metadata.ITag[]> => {

    const {modelDetails, searchAsOf, searchByCommit, tenant} = payload

    const tracModelRepositoryCriteria = trac.metadata.SearchExpression.create({
        term: {
            attrName: "trac_model_repository",
            attrType: trac.STRING,
            operator: trac.SearchOperator.EQ,
            searchValue: {stringValue: modelDetails.repository}
        }
    });

    const tracModelEntryPointCriteria = trac.metadata.SearchExpression.create({
        term: {
            attrName: "trac_model_entry_point",
            attrType: trac.STRING,
            operator: trac.SearchOperator.EQ,
            searchValue: {stringValue: modelDetails.entryPoint}
        }
    });

    const tracModelPathCriteria = trac.metadata.SearchExpression.create({
        term: {
            attrName: "trac_model_path",
            attrType: trac.STRING,
            operator: trac.SearchOperator.EQ,
            // For files in the root of the repo the path needs to be a '.'
            searchValue: {stringValue: (modelDetails.path?.trim() === "" ? "." : modelDetails.path)}
        }
    });

    const tracModelVersionCriteria = trac.metadata.SearchExpression.create({
        term: {
            attrName: "trac_model_version",
            attrType: trac.STRING,
            operator: trac.SearchOperator.EQ,
            searchValue: {stringValue: modelDetails.version}
        }
    });

    const logicalSearch = trac.metadata.SearchExpression.create({
        logical: {
            operator: trac.LogicalOperator.AND,
            expr: [
                tracModelEntryPointCriteria,
                tracModelPathCriteria,
                tracModelRepositoryCriteria
            ]
        }
    });

    // Add in the search by commit if needed
    if (searchByCommit) logicalSearch.logical?.expr?.push(tracModelVersionCriteria)

    // Execute the search
    const result = await metaApi.search(trac.api.MetadataSearchRequest.create({
        searchParams: {
            searchAsOf,
            objectType: trac.ObjectType.MODEL,
            search: logicalSearch
        },
        tenant
    }))

    return result.searchResult
}

/**
 * An interface for the searchForAttachedFile function.
 */
export interface SearchForAttachedFile {

    /**
     * The object key the attached document relates to. So if you loaded a model governance document
     * then this is the model ID.
     */
    attachedObjectKey: string
    /**
     *  The ISO datetime to do the search as at, this allows you to go back to the application
     * as it was at a particular time.
     */
    searchAsOf: null | trac.metadata.IDatetimeValue
    /**
     * The TRAC tenant to do the search in.
     */
    tenant: string
}

/**
 * A function that searches for documents in TRAC that are registered to a particular object.
 *
 * @remarks
 * The 'attachedObjectKey' value in the payload could be an object ID or an object key (an object key includes
 * the specific object version).
 *
 * @param payload - The information about what to search for.
 */
export const searchForAttachedFile = async (payload: SearchForAttachedFile): Promise<trac.metadata.ITag[]> => {

    const {attachedObjectKey, searchAsOf, tenant} = payload

    const attachedObjectCriteria = trac.metadata.SearchExpression.create({
        term: {
            attrName: "attached_objects",
            attrType: trac.STRING,
            operator: trac.SearchOperator.NE,
            searchValue: {stringValue: attachedObjectKey}
        }
    });

    const hiddenCriteria = trac.metadata.SearchExpression.create({
        term: {
            attrName: "show_in_search_results",
            attrType: trac.BOOLEAN,
            operator: trac.SearchOperator.EQ,
            searchValue: {booleanValue: true}
        }
    });

    const logicalSearch = trac.metadata.SearchExpression.create({
        logical: {
            operator: trac.LogicalOperator.AND,
            expr: [
                attachedObjectCriteria,
                hiddenCriteria
            ]
        }
    });

    // TODO this should search across all versions of the object, if the object key is searched for then this will be specific
    // Execute the search
    const results = await metaApi.search(trac.api.MetadataSearchRequest.create({
        searchParams: {
            searchAsOf,
            objectType: trac.ObjectType.FILE,
            search: logicalSearch
        },
        tenant
    }))

    return results.searchResult
}

/**
 * An interface for the isObjectLoadedByLocalFileDetails function.
 */
export interface IsObjectLoadedByLocalFileDetails {

    /**
     * The local file information to search for in TRAC.
     */
    fileDetails: Pick<FileSystemInfo, "sizeInBytes" | "fileName" | "lastModifiedTracDate">
    /**
     * The TRAC object type to search for.
     */
    objectType: trac.metadata.ObjectType
    /**
     *  The ISO datetime to do the search as at, this allows you to go back to the application
     * as it was at a particular time.
     */
    searchAsOf: null | trac.metadata.IDatetimeValue
    /**
     * The TRAC tenant to do the search in.
     */
    tenant: string
}

/**
 * A function that makes a search in TRAC for an object loaded from a local file with the same file parameters (file
 * name and modified date for example) as the dataset that the user is trying to/has just loaded. If a version of the
 * object is found then the user can be restricted in loading up the same item again.
 *
 * @param payload - The file information to search for as well as the other search parameters.
 */
export const isObjectLoadedByLocalFileDetails = async (payload: IsObjectLoadedByLocalFileDetails): Promise<trac.metadata.ITag[]> => {

    const {fileDetails, objectType, searchAsOf, tenant} = payload

    const dataFileNameCriteria = trac.metadata.SearchExpression.create({
        term: {
            attrName: "original_file_name",
            attrType: trac.STRING,
            operator: trac.SearchOperator.EQ,
            searchValue: {stringValue: fileDetails.fileName}
        }
    });

    const dataFileSizeCriteria = trac.metadata.SearchExpression.create({
        term: {
            attrName: "original_file_size",
            attrType: trac.INTEGER,
            operator: trac.SearchOperator.EQ,
            searchValue: {integerValue: fileDetails.sizeInBytes}
        }
    });

    const dataFileLastModifiedCriteria = trac.metadata.SearchExpression.create({
        term: {
            attrName: "original_file_modified_date",
            attrType: trac.DATETIME,
            operator: trac.SearchOperator.EQ,
            // For files in the root of the repo the path needs to be a '.'
            searchValue: {datetimeValue: {isoDatetime: fileDetails.lastModifiedTracDate}}
        }
    });

    const logicalSearch = trac.metadata.SearchExpression.create({
        logical: {
            operator: trac.LogicalOperator.AND,
            expr: [
                dataFileNameCriteria,
                dataFileSizeCriteria,
                dataFileLastModifiedCriteria
            ]
        }
    });

    // Execute the search
    const result = await metaApi.search(trac.api.MetadataSearchRequest.create({
        searchParams: {
            searchAsOf,
            objectType,
            search: logicalSearch
        },
        tenant
    }))

    return result.searchResult
}

/**
 * An interface for the getMatchingSchemasByNumberOfFields function.
 */
export interface GetMatchingSchemasByNumberOfFields {

    /**
     * Whether the search is for schemas with exactly the number of fields defined by the
     * numberOfFields parameter or whether results with fewer fields should also be returned.
     * This is because when loading a dataset you can load using schemas where only a subset
     * are defined, this will cause TRAC to remove the additional fields when saving the dataset.
     */
    canBeFewer: boolean
    /**
     * The number of fields that the schemas should have.
     */
    numberOfFields: number
    /**
     *  The ISO datetime to do the search as at, this allows you to go back to the application
     * as it was at a particular time.
     */
    searchAsOf: null | trac.metadata.IDatetimeValue
    /**
     * The TRAC tenant to do the search in.
     */
    tenant: string
}

/**
 * A function that searches for schemas in TRAC that have a particular number of fields, there is an option to also return
 * schemas with fewer fields.
 *
 * @remarks
 * The 'number_of_fields' attribute is added when creating schemas using the user interface, it is not created by TRAC for you
 * automatically. This function is used when loading data, the user interface tries to find schemas that match the data by first
 * searching for schemas with the right number of fields.
 *
 * @param payload - The information about what number of fields to search for and additional search parameters.
 */
export const getMatchingSchemasByNumberOfFields = async (payload: GetMatchingSchemasByNumberOfFields): Promise<trac.metadata.ITag[]> => {

    const {canBeFewer, numberOfFields, searchAsOf, tenant} = payload

    const hiddenCriteria = trac.metadata.SearchExpression.create({
        term: {
            attrName: "show_in_search_results",
            attrType: trac.BOOLEAN,
            operator: trac.SearchOperator.EQ,
            searchValue: {booleanValue: true}
        }
    })

    const numberOfFieldsCriteria = trac.metadata.SearchExpression.create({
        term: trac.metadata.SearchTerm.create({
            attrName: "number_of_fields",
            attrType: trac.INTEGER,
            // We can look for schemas that have fewer fields than the number requested, for example when matching schemas to datasets where the datasets may have more variables than required
            operator: canBeFewer ? trac.SearchOperator.LE : trac.SearchOperator.EQ,
            searchValue: {integerValue: numberOfFields}
        })
    });

    // Put all the searches together into a single logical operator
    const logicalSearch = trac.metadata.SearchExpression.create({
        logical: {
            operator: trac.LogicalOperator.AND,
            expr: [hiddenCriteria, numberOfFieldsCriteria]
        }
    });

    // Execute the search
    return (await metaApi.search(trac.api.MetadataSearchRequest.create({
        tenant,
        searchParams: {
            objectType: trac.ObjectType.SCHEMA,
            search: logicalSearch,
            searchAsOf
        }
    }))).searchResult
}

/**
 * An interface for the getModelFromImportJob function.
 */
export interface GetModelFromImportJob {

    /**
     * The object key of the job to get the model from.
     */
    objectKey: string
    /**
     *  The ISO datetime to do the search as at, this allows you to go back to the application
     * as it was at a particular time.
     */
    searchAsOf: null | trac.metadata.IDatetimeValue
    /**
     * The TRAC tenant to do the search in.
     */
    tenant: string
}

/**
 * A function that searches for the model in TRAC imported by a particular job. The model metadata is not
 * stored in the job definition.
 *
 * @param payload - The job object key and additional search parameters.
 */
export const getModelFromImportJob = async (payload: GetModelFromImportJob): Promise<trac.metadata.ITag[]> => {

    const {objectKey, searchAsOf, tenant} = payload

    const termSearch = trac.metadata.SearchExpression.create({

        term: trac.metadata.SearchTerm.create({

            attrName: "trac_update_job",
            attrType: trac.STRING,
            operator: trac.SearchOperator.EQ,
            searchValue: {stringValue: objectKey}
        })
    });

    // Execute the search
    const result = await metaApi.search(trac.api.MetadataSearchRequest.create({
        searchParams: {
            objectType: trac.ObjectType.MODEL,
            search: termSearch,
            searchAsOf
        },
        tenant
    }))

    return result.searchResult
}

/**
 * An interface for the updateTag function.
 */
export interface UpdateTag {

    /**
     * An existing object to apply the update to.
     */
    priorVersion: trac.metadata.ITagSelector
    /**
     * The changes to the object's existing attributes to apply. These include the action to take
     * for each attribute e.g. delete or create etc.
     */
    tagUpdates: trac.metadata.ITagUpdate[]
    /**
     * The TRAC tenant to do the update in.
     */
    tenant: string
}

/**
 * A function that updates an object's tag in TRAC.
 *
 * @param payload - The details of the tag updates to apply, the object to apply it to.
 */
export const updateTag = async (payload: UpdateTag): Promise<trac.metadata.ITagHeader> => {

    const {tagUpdates, priorVersion, tenant} = payload

    const metadataWriteRequest = trac.api.MetadataWriteRequest.create({

        tenant,
        objectType: priorVersion.objectType,
        // This assumes we need to clear all the tags on the old version and then apply all the new tags
        tagUpdates,
        priorVersion
    });

    return await metaApi.updateTag(metadataWriteRequest)
}

/**
 * An interface for the getLatestTag function.
 */
export interface GetLatestTag {

    /**
     * The medata tag to get the latest version of.
     */
    tag: trac.metadata.ITag
    /**
     * The TRAC tenant to do the request in.
     */
    tenant: string
}

/**
 * A function that gets an object's latest tag from TRAC. This is useful if you have a tag,
 * but you need to find out if it is the latest version.
 *
 * @param payload - The details of the tag to check.
 */
export const getLatestTag = async (payload: GetLatestTag): Promise<trac.metadata.ITag> => {

    const {tag, tenant} = payload

    let tagSelector: trac.metadata.ITagSelector = {
        objectType: tag.header?.objectType,
        objectId: tag.header?.objectId,
        latestTag: true,
        objectVersion: tag.header?.objectVersion
    }

    // A request to get the metadata for an object by its header tag
    const metadataRequest = trac.api.MetadataReadRequest.create({
        selector: tagSelector,
        tenant
    })

    return await metaApi.readObject(metadataRequest)
}

/**
 * An interface for the getObjectsFromUserDefinedObjectIds function.
 */
export interface GetObjectsFromUserDefinedObjectIds {

    /**
     * The user entered object ID or object key to get the metadata for, this can be a list of
     * comma delimited items.
     */
    inputValue: string
    /**
     * The TRAC object type to search for.
     */
    objectType: trac.metadata.ObjectType
    /**
     * Whether the user can load one or multiple items in one go.
     * @defaultValue 'one'
     */
    oneOrMany: "one" | "many"
    /**
     *  The ISO datetime to do the search as at, this allows you to go back to the application
     * as it was at a particular time.
     */
    searchAsOf: null | trac.metadata.IDatetimeValue
    /**
     * The TRAC tenant to do the search in.
     */
    tenant: string
}

/**
 * A wrapper function that runs when the user enters text into either the SelectOption or SelectValue components,
 * it attempts to get the object metadata for the items in the string or strings. Both the SelectOption or SelectValue
 * components have additional logic around this function that deal with the messaging. The objectType prop enforces
 * what type of object can be loaded as an option.
 *
 * @param payload - The information required to identify the object metadata to fetch.
 */
export const getObjectsFromUserDefinedObjectIds = async (payload: GetObjectsFromUserDefinedObjectIds): Promise<{
    notAnObjectIdOrKey: string[],
    notCorrectObjectType: trac.metadata.ITagSelector[],
    foundTags: trac.metadata.ITag[],
    notFoundTags: trac.metadata.ITagSelector[],
    suggestedTagForNotFound: undefined | trac.metadata.ITag
}> => {

    const {inputValue, objectType, oneOrMany = "one", searchAsOf, tenant} = payload

    // If the user is allowed to load several objects at once then we split the input string into an array, we split
    // on spaces and commas
    let inputValueArray = oneOrMany === "many" ? inputValue.split(/[,\s]+/) : [inputValue]

    // Forgive the user if they have spaces around the string, remove blanks
    inputValueArray = inputValueArray.map(inputValue => inputValue.trim()).filter(inputValue => inputValue !== "")

    // Have we been given any invalid object IDs or object keys to use?
    const inputIsNotAnObjectIdOrKeyArray = inputValueArray.filter(inputValue => !isObjectId(inputValue) && !isObjectKey(inputValue))

    // Remove any badly formed object IDs or keys
    inputValueArray.filter(inputValue => isObjectId(inputValue) || isObjectKey(inputValue))

    // We only run the API query if the object type in the key is the same as the type in the objectType prop.
    // We do not allow the loading of objects different to what might already be in the options list
    let tagSelectorsArray = inputValueArray.map(inputValue => {

        const inputIsAnObjectId = isObjectId(inputValue)

        if (inputIsAnObjectId) {

            // This is when the user has pasted a valid object ID as the created option. In this case
            // we also need the objectType to be able to see if it is a valid ID and also go and get the
            // metadata for it. This is because when getting an object's metadata the objectType is a
            // required part of the request. This method will only get the latest version of the object.

            return {
                objectId: inputValue,
                objectType: objectType,
                latestObject: true,
                latestTag: true,
                objectAsOf: searchAsOf,
                tagAsOf: searchAsOf
            }

        } else if (isObjectKey(inputValue)) {

            // This is when the user has pasted a valid object key as the created/searched for option. In this case
            // we also don't need the objectType as it's defined in the key, but we only want to activate
            // this mode when the prop is set. In this case we need to break the object key into its parts
            // which contain the object type, object ID and the version to get
            return convertObjectKeyToTagSelector(inputValue, searchAsOf)

        } else {

            return undefined
        }

    }).filter(isDefined)

    // Now we will only process object keys where the object type matches what the SelectOption or SelectValue
    // component is set up to fetch

    // Have we been object keys for the right type?
    const inputIsNotCorrectObjectType = tagSelectorsArray.filter(tagSelector => objectType !== tagSelector?.objectType)

    // Remove any keys for the wrong type
    tagSelectorsArray = tagSelectorsArray.filter(tagSelector => objectType === tagSelector?.objectType)

    // So now we have removed invalid object Ids and object keys and then those where the object type for the
    // object key does not match the desired type (object IDs have not yet been checked for their type), so we
    // can now start to make the API calls.

    // For each object we are going to get we fail silently and record which ones don't exist
    const promiseArray = tagSelectorsArray.map(tagSelector =>

        // We are not using the metadata store owned by the applicationStore because it does not
        // handle tag selectors that use latestObject = true or latestTag = true.
        metadataReadRequest({tenant, tagSelector: tagSelector})
    )

    // A request to get the metadata for an object by the selector tag, Promise.allSettled allows requests to fail
    const metadataBatchSettledResponses: PromiseSettledResult<trac.metadata.ITag>[] = await Promise.allSettled(promiseArray)

    // The indices of the requests that failed
    const failedIndices: number[] = metadataBatchSettledResponses.map((response, i) => response.status === "rejected" ? i : undefined).filter(isDefined)

    // This is a record of which object IDs and Object keys do not exist in TRAC
    const doNotExist: trac.metadata.ITagSelector[] = tagSelectorsArray.filter((_, i) => failedIndices.includes(i))

    // The response.status == "fulfilled" check resolves the response to a PromiseFulfilledResult type that has a value property
    const metadataBatchResponses: trac.metadata.ITag[] = metadataBatchSettledResponses.map(response => response.status == "fulfilled" && hasOwnProperty(response, "value") ? response.value : undefined).filter(isDefined)

    let notFoundLatestTag: undefined | trac.metadata.ITag = undefined

    // Do we have any failed requests that we for a specific version - earlier versions might exist
    // we only do this if the user requests a single object
    if (inputValueArray.length === 1 && doNotExist.some(tagSelector => tagSelector.latestObject !== true)) {

        try {

            // Convert the tagSelector by version number to one just asking for the latest
            const newTagSelector: trac.metadata.ITagSelector = {

                objectId: doNotExist[0].objectId,
                objectType: objectType,
                latestObject: true,
                latestTag: true,
                objectAsOf: searchAsOf,
                tagAsOf: searchAsOf
            }

            // Tags that use 'latest' in the selector are not saved in the metadataStore, so we can just
            // use the regular call
            notFoundLatestTag = await metadataReadRequest({tenant, tagSelector: newTagSelector})

        } catch (err) {

            console.error(err)
        }
    }

    return ({
        notAnObjectIdOrKey: inputIsNotAnObjectIdOrKeyArray,
        notCorrectObjectType: inputIsNotCorrectObjectType,
        foundTags: metadataBatchResponses,
        notFoundTags: doNotExist,
        suggestedTagForNotFound: notFoundLatestTag
    })
}







export const metadataReadRequest = async ({
                                              tenant,
                                              tagSelector
                                          }: { tenant: string, tagSelector: trac.metadata.ITagSelector }): Promise<trac.metadata.ITag> => {

    // A request to get the metadata for an object by its header tag
    const metadataRequest = trac.api.MetadataReadRequest.create({
        tenant: tenant,
        selector: tagSelector
    })

    return metaApi.readObject(metadataRequest)
}

export const metadataBatchRequest = async ({
                                               tenant,
                                               tags
                                           }: { tenant: string, tags: (null | undefined | trac.metadata.ITagHeader)[] }) => {

    // A request to get the metadata for an object by its header tag
    const metadataRequest = trac.api.MetadataBatchRequest.create({
        tenant: tenant,
        // Remove undefined headers
        selector: tags.filter(isDefined),
    })

    return await metaApi.readBatch(metadataRequest)
}

export const saveFile = async (tenant: string, {
    content,
    mimeType,
    name,
    attrs,
    priorVersion,
    size,
}: { content: Uint8Array, mimeType: string, name: string, attrs: trac.metadata.ITagUpdate[], priorVersion?: trac.metadata.TagSelector, size?: number }): Promise<trac.metadata.ITagHeader> => {

    const fileWriteRequest = trac.api.FileWriteRequest.create({

        tenant: tenant,
        content,
        mimeType,
        name,
        tagUpdates: attrs,
        priorVersion,
        size
    });

    console.log("Submitting file for upload ");

    return dataApi.createFile(fileWriteRequest)
}

/**
 * A function that does its best to get a list of usernames from TRAC to use to allow people to search for
 * objects created by users. There is no API endpoint for getting all the names, so we do the next best
 * thing of making some requests for objects and reducing this down to a set of unique options.
 *
 * @param searchAsOf - The date time to do the search as of.
 * @param tenant - The tenant to make the API request to.
 */
export const getUserNames = async (searchAsOf: null | trac.metadata.IDatetimeValue, tenant: string): Promise<Option<string> []> => {

    // This is what we are going to return
    let userOptions: Option<string>[] = []

    // The array of objects that we are going to get the last n of to compile down to the list of users
    const objectTypesBackEnd: trac.ObjectType[] = [trac.ObjectType.MODEL, trac.ObjectType.FLOW,]

    // Get the names of users likely to be loading up items for people to use
    const backEndSearchPromiseArray = objectTypesBackEnd.map((objectType) => {

        return metaApi.search(trac.api.MetadataSearchRequest.create({
            tenant: tenant,
            searchParams: {
                searchAsOf: searchAsOf,
                objectType: objectType
            }
        }))
    })

    const backEndSearchResponses = await Promise.all(backEndSearchPromiseArray)

    backEndSearchResponses.forEach(backEndSearchResponse => {

        backEndSearchResponse.searchResult.map(tag => {

            if (tag.attrs?.trac_create_user_id.stringValue && tag.attrs?.trac_create_user_name.stringValue) {
                userOptions.push({value: tag.attrs.trac_create_user_id.stringValue, label: tag.attrs.trac_create_user_name.stringValue})
            }
        })
    })

    const excludeIds = trac.metadata.SearchExpression.create({
        term: {
            attrName: "trac_create_user_id",
            attrType: trac.STRING,
            operator: trac.SearchOperator.IN,
            searchValue: {
                "arrayValue": {
                    "items": [...new Set(userOptions.map(option => option.value))].map(id => ({stringValue: id}))
                }
            }
        }
    })

    // Put all the searches together into a single logical operator
    const logicalSearch = trac.metadata.SearchExpression.create({
        logical: {
            operator: trac.LogicalOperator.NOT,
            // If show_in_search_results is true add in an additional expression to limit to only those with
            // this attribute set to true.
            expr: [excludeIds]
        }
    });

    const objectTypesFrontEnd: trac.ObjectType[] = [trac.ObjectType.DATA, trac.ObjectType.JOB]

    // Get the names of users likely to be loading up items for people to use
    const frontEndSearchPromiseArray = objectTypesFrontEnd.map((objectType) => {

        return metaApi.search(trac.api.MetadataSearchRequest.create({
            tenant: tenant,
            searchParams: {
                objectType: objectType,
                search: logicalSearch,
                searchAsOf: searchAsOf
            }
        }))
    })

    const frontEndSearchResponses = await Promise.all(frontEndSearchPromiseArray)

    frontEndSearchResponses.forEach(frontEndSearchResponse => {

        frontEndSearchResponse.searchResult.map(tag => {

            if (tag.attrs?.trac_create_user_id.stringValue && tag.attrs?.trac_create_user_name.stringValue) {
                userOptions.push({value: tag.attrs.trac_create_user_id.stringValue, label: tag.attrs.trac_create_user_name.stringValue})
            }
        })
    })

    return sortArrayBy(makeArrayOfObjectsUniqueByProperty(userOptions, "value"), "label")
}


/**
 * A function that makes a TRAC API call to get the metadata and data for a small dataset using the object's
 * TRAC metadata header.
 *
 * @example
 * When this function is called Typescript generics are used to pass the type of the key property (mostly this will be a
 * string type but you can use specific string unions) and the type for the dataset that is returned (mostly DataRow).
 * We do this so that the types for the returned values are asserted, and you don't have to do your own type guarding or
 * assertions outside this function. For example
 * ```ts
 * const result = getSmallDatasetByTag<UiEditableDatasetKeys, UiEditableRow>(header, {tenant})
 * ```
 *
 * @param header - The dataset's metadata header.
 * @param tenant - The tenant to make the API request to.
 * @param format - The format of the dataset to get from the API.
 * @deprecated
 */
export const getSmallDatasetByTag = async <T, U>(header: trac.metadata.ITagHeader, {
    tenant,
    format = "text/json"
}: { tenant: string, format?: string }): Promise<GetDatasetByTagResult<T, U>> => {

    // Define the object that we are going to return
    let results: GetDatasetByTagResult<T, U> = {
        data: [],
        tag: trac.metadata.Tag.create(),
        schema: []
    }

    // A request to get the metadata for an object by its header tag
    const metadataRequest = trac.api.MetadataReadRequest.create({
        tenant: tenant,
        selector: header
    })

    // A request to get the data for a dataset by its header tag
    const dataRequest = trac.api.DataReadRequest.create({
        tenant: tenant,
        selector: header,
        format: format
    })

    // Execute the metadata and data calls in parallel and store the required information in results
    await Promise.all([

        metaApi.readObject(metadataRequest).then(response => results.tag = response),

        dataApi.readSmallDataset(dataRequest).then(response => {

            if (response.content === null) {
                throw new Error("The content of the response is null")
            }
            const text = new TextDecoder().decode(response.content)
            results.data = JSON.parse(text)

            // Typescript requires us to check against null to not get an error
            if (response.schema == null) {
                throw new TypeError(`The schema for dataset ${header.objectId} version ${header.objectVersion} is null`)
            }

            results.schema = response.schema.table?.fields || []
        })
    ])

    // Get the dataset key if set
    const key = results?.tag?.attrs?.key?.stringValue as T | undefined
    if (key) results.key = key

    return results
}

export const readSmallDataset = async <U extends void | Object = DataRow, T extends string = "text/json">(payload: { format?: T, key?: string, tagSelector: trac.metadata.ITagSelector, tenant: string }): Promise<{ data: T extends "text/json" ? U[] : string, schema: trac.metadata.IFieldSchema[], key?: string }> => {

    const {
        format = "text/json",
        key,
        tagSelector,
        tenant
    } = payload

    // A request to get the data for a dataset by its selector tag
    const dataRequest = trac.api.DataReadRequest.create({
        tenant: tenant,
        selector: tagSelector,
        format: format
    })

    return dataApi.readSmallDataset(dataRequest).then(response => {

        if (response.content === null) {
            throw new Error("The content of the response is null")
        }

        const text = new TextDecoder().decode(response.content)

        return {
            data: format === "text/json" ? JSON.parse(text) : text,
            schema: response.schema?.table?.fields || [],
            key
        }
    })
}

/**
 * A function that initiates a job that loads a dataset from a local csv or xlsx file. The job is
 * first validated to check that it is well-formed. This updates an existing dataset.
 * @param tenant - The tenant to load the model into.
 * @param payload - The details of what to load including attributes to add to the dataset.
 */
export const importDataFromJson = async ({
                                             tenant,
                                             attrs,
                                             data,
                                             priorVersion,
                                             schema

                                         }: { tenant: string, attrs?: trac.metadata.ITagUpdate[], data: DataRow[], priorVersion?: trac.metadata.ITagSelector, schema: trac.metadata.ITagSelector | trac.metadata.IFieldSchema[] }): Promise<trac.metadata.ITagHeader> => {

    // Encode the data for the API to save it in TRAC
    const json = JSON.stringify(data);

    const dataWriteRequest = trac.api.DataWriteRequest.create({

        tenant: tenant,
        format: "text/json",
        content: new TextEncoder().encode(json),
        priorVersion: priorVersion
    });

    if (attrs) {
        // This assumes that if we are provided with attributes we need to clear all the tags on the old version and then
        // apply all the new tags. If you are updating data but don't provide attrs then the existing ones won't be updated
        dataWriteRequest.tagUpdates = [{operation: trac.TagOperation.CLEAR_ALL_ATTR}, ...attrs]
    }

    // If we have a schema which is the array of field definitions then we add a different property than if the user
    // has set a schema object to use, in which case we use its metadata tag
    if (Array.isArray(schema)) {

        dataWriteRequest.schema = {
            partType: trac.metadata.PartType.PART_ROOT,
            schemaType: trac.metadata.SchemaType.TABLE,
            table: {fields: schema}
        }

    } else {

        dataWriteRequest.schemaId = schema
    }

    console.log("Submitting data import");

    if (priorVersion) {
        return await dataApi.updateSmallDataset(dataWriteRequest)
    } else {
        //return await dataApi.createDataset(dataWriteRequest)
        return await dataApi.createSmallDataset(dataWriteRequest)
    }
}