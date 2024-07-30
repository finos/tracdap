/*
 * Copyright 2024 Accenture Global Solutions Limited
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

const metaTransport = tracdap.setup.transportForTarget(tracdap.api.TracMetadataApi, "http", "localhost", 8080);
const metaApi = new tracdap.api.TracMetadataApi(metaTransport);

import {saveStreamingData, loadStreamingData} from './streaming.js';
import fs from "fs";

async function createInvalidObject() {

    // Set up an invalid schema definition
    const schema = tracdap.metadata.SchemaDefinition.create({

        schemaType: tracdap.SchemaType.TABLE,
        table: {
            fields: [
                { fieldName: "valid_field", fieldType: tracdap.STRING, businessKey: true, label: "Valid field" },
                { fieldName: "invalid_field_2", fieldType: tracdap.FLOAT, categorical: true }
            ]
        }
    });

    // Set up an invalid write request, with the bad schema and an invalid tag attribute
    const badRequest = tracdap.api.MetadataWriteRequest.create({

        tenant: "ACME_CORP",
        objectType: tracdap.ObjectType.SCHEMA,

        definition: {
            objectType: tracdap.ObjectType.SCHEMA,
            schema: schema
        },

        tagUpdates: [
            { attrName: "%%%business_division", value: { stringValue: "WIDGET_SALES" } }
        ]

    });

    console.log("Try to create an invalid objects...");

    // Handle an error in the API call using futures
    await metaApi.createObject(badRequest)
        .then(_ => {})  // handle response
        .catch(error => {
            // handle error
            console.log("There was a problem creating the object: " + error.message);
            console.log("gRPC status code: " + error.code);
        });

    // Handle the same error using callback-style API calls
    await metaApi.createObject(badRequest, (error, _) => {
        if (error != null) {
            // handle error
            console.log("There was a problem creating the object: " + error.message);
            console.log("gRPC status code: " + error.code);
        }
        else {
            // handle response
        }
    });

    // Handle an error in the API call using futures
    await metaApi.createObject(badRequest)
        .then(_ => {})  // handle response
        .catch(error => {
            // Get structured error details
            const details = tracdap.utils.getErrorDetails(error);
            console.log("There was a problem creating the object: " + details.message);
            console.log("gRPC status code: " + details.code);
            // Show more detailed information
            details.items.forEach(item => {
                console.log(item.location + ": " + item.detail + " (" + item.category + ")");
            })
        });
}

async function badStreamingUpload() {

    console.log("Start a bad streaming upload...");

    const csvStream = fs.createReadStream("data/bad_data.csv");

    // See saveStreamingData() in streaming.js
    // For details of how errors are handled in the stream

    return saveStreamingData(csvStream)
        .then(_ => {})  // Handle success
        .catch(error => {
            const details = tracdap.utils.getErrorDetails(error);
            console.log(details.message);
            details.items.forEach(item => console.log(item.detail))
        });
}

async function badStreamingDownload() {

    console.log("Start a bad streaming download...");

    // Random DATA ID, does not exist
    const dataId = tracdap.metadata.TagSelector.create({
        objectType: tracdap.ObjectType.DATA,
        objectId: "ace71340-07fa-42ac-9dd4-ceb2ee8eca19",
        objectVersion: 1,
        latestTag: true
    });

    // See loadStreamingData() in streaming.js
    // For details of how errors are handled in the stream

    return loadStreamingData(dataId)
        .then(_ => {})  // Handle success
        .catch(error => {
            const details = tracdap.utils.getErrorDetails(error);
            console.log(details.message);
            details.items.forEach(item => console.log(item.detail))
        });
}

export async function main() {

    console.log("Error handling examples");

    await createInvalidObject();
    await badStreamingUpload();
    await badStreamingDownload();

    console.log("All done");
}
