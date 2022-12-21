/*
 * Copyright 2022 Accenture Global Solutions Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {tracdap} from 'tracdap-web-api';
import fs from 'fs';

// Create the Data API
const transportOptions = {transport: "trac", debug: false};
const transport = tracdap.setup.transportForTarget(tracdap.api.TracDataApi, "http", "localhost", 8080, transportOptions);
const dataApi = new tracdap.api.TracDataApi(transport);

// Location of a large file to use as an example
const LARGE_CSV_FILE = "../../../tracdap-services/tracdap-svc-data/src/test/resources/large_csv_data_100000.csv";


async function saveStreamingData(csvData) {

    // Schema definition for the data we want to save
    // You can also use a pre-loaded schema ID
    const schema = tracdap.metadata.SchemaDefinition.create({

        schemaType: tracdap.SchemaType.TABLE,
        table: {
            fields: [
                { fieldName: "boolean_field", fieldType: tracdap.BOOLEAN },
                { fieldName: "integer_field", fieldType: tracdap.INTEGER },
                { fieldName: "float_field", fieldType: tracdap.FLOAT },
                { fieldName: "decimal_field", fieldType: tracdap.DECIMAL },
                { fieldName: "string_field", fieldType: tracdap.STRING },
                { fieldName: "date_field", fieldType: tracdap.DATE },
                { fieldName: "datetime_field", fieldType: tracdap.DATETIME }
            ]
        }
    });

    // Create a request object to save the data, this is the first message that will be sent
    // It is just like createSmallDataset, but without the content
    const request0 = tracdap.api.DataWriteRequest.create({

        tenant: "ACME_CORP",
        schema: schema,
        format: "text/csv",

        tagUpdates: [
            { attrName: "schema_type", value: { stringValue: "large_test_data" } },
            { attrName: "business_division", value: { stringValue: "ACME Test Division" } },
            { attrName: "description", value: { stringValue: "A streaming sample data set" } },
        ]
    });

    // The upload stream is set up as a promise
    // The stream will run until it either completes or fails, and the result will come back on the promise

    return new Promise((resolve, reject) => {

        // You have to call newStream before using a streaming operation
        // This is needed so events on different streams don't get mixed up
        // TRAC will not let you run two streams on the same instance
        const stream = tracdap.setup.newStream(dataApi);

        // To start the upload, call the API method as normal with your first request object
        // The success / failure of this call is passed back through resolve/reject on the promise

        stream.createDataset(request0)
            .then(resolve)
            .catch(reject);

        // Now handle the events on you data stream, by forwarding them to the API stream
        // In this example, csvDAta is a stream of chunks loaded from the file system
        // Each chunk needs to be wrapped in a message, by setting the "content" field
        // All the other fields in the message should be left blank

        csvData.on('data', chunk => {
            const msg = tracdap.api.DataWriteRequest.create({content: chunk});
            stream.createDataset(msg)
        });

        // Make sure to forward the complete and error signals on the input stream as well
        // This will make sure the stream is either finished or cancelled
        // And let errors be sent back on the promise

        csvData.on('close', () => stream.end());
        csvData.on('error', () => stream.end(true));
    });
}


function loadStreamingData(dataId) {

    // Ask for the dataset in CSV format so we can easily count the rows
    const request = tracdap.api.DataReadRequest.create({

        tenant: "ACME_CORP",
        selector: dataId,
        format: "text/csv"
    });

    // In this example, the result of the download stream is aggregated into a single message
    // To display data in the browser, the entire dataset must be loaded in memory
    // Aggregating the data and returning a single promise keeps the streaming logic contained

    // Just like the upload method, set up the stream operation as a promise

    return new Promise((resolve, reject) => {

        // You have to call newStream before using a streaming operation
        const stream = tracdap.setup.newStream(dataApi);

        // Hold the responses here until the stream is complete
        const messages = []

        // Make an initial call to start the download stream
        stream.readDataset(request).catch(reject);

        // When messages come in, stash them until the stream is complete
        stream.on("data", msg => messages.push(msg));

        // Once the stream finishes we can aggregate the messages into a single response
        stream.on("end", () => {
            const response = tracdap.utils.aggregateResponse(messages);
            resolve(response);
        });

        // Handle the error signal to make sure errors are reported back through the promise
        stream.on("error", reject);
    });
}

export async function main() {

    console.log("Start streaming upload");

    const csvStream = fs.createReadStream(LARGE_CSV_FILE);
    const dataId = await saveStreamingData(csvStream);

    console.log("Uploaded dataset, here is the ID:")
    console.log(JSON.stringify(dataId, null, 2));

    console.log("Start streaming download");

    const dataResult = await loadStreamingData(dataId);

    const schema = dataResult.schema;
    const csvData = new TextDecoder().decode(dataResult.content);
    const csvRows = csvData.split("\n").length - 1;  // quick row count, minus one row for the header!

    console.log(`Data download has been aggregated, there are ${csvRows} rows, here is the schema:`)
    console.log(JSON.stringify(schema, null, 2));
}

