/*
 * Copyright 2021 Accenture Global Solutions Limited
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

import {searchForSchema} from './metadata_mojo.js';
import {loadFromDisk} from './util.js';

// Create the Data API
const dataTransport = tracdap.setup.transportForTarget(tracdap.api.TracDataApi, "http", "localhost", 8080);
const dataApi = new tracdap.api.TracDataApi(dataTransport);


export function saveDataToTrac(schemaId, csvData) {

    const request = tracdap.api.DataWriteRequest.create({

        tenant: "ACME_CORP",

        schemaId: schemaId,

        format: "text/csv",
        content: csvData,

        tagUpdates: [
            { attrName: "schema_type", value: { stringValue: "customer_records" } },
            { attrName: "business_division", value: { stringValue: "WIDGET_SALES" } },
            { attrName: "description", value: { stringValue: "A month-end snapshot of customer accounts" } },
        ]
    });

    return dataApi.createSmallDataset(request).then(dataId => {

        console.log(`Created dataset ${dataId.objectId} version ${dataId.objectVersion}`);

        return dataId;
    });
}

export function loadDataFromTrac(dataId) {

    // Ask for the dataset in JSON format
    const request = tracdap.api.DataReadRequest.create({

        tenant: "ACME_CORP",

        selector: dataId,
        format: "text/json"
    });

    return dataApi.readSmallDataset(request).then(response => {

        // Decode JSON into an array of Objects
        const text = new TextDecoder().decode(response.content);
        const data = JSON.parse(text);

        return {schema: response.schema, data: data};
    });
}

export function displayTable(schema, data) {

    const fields = schema.table.fields;

    const accessor = (data_, row, col) => {
        const fieldName = fields[col].fieldName;
        return data_[row][fieldName];
    }

    renderTable(schema, data, accessor);
}

export function renderTable(schema, data, accessor) {

    const CELL_WIDTH = 30;

    const nCols = schema.table.fields.length;

    function printCell(col, cellValue) {

        process.stdout.write(`${cellValue}`.padEnd(CELL_WIDTH))

        if (col < nCols - 1)
            process.stdout.write(" | ")
        else
            process.stdout.write("\n")
    }

    process.stdout.write("\n");

    for (let col = 0; col < nCols; col++)
        printCell(col, schema.table.fields[col].fieldName)

    for (let col = 0; col < nCols; col++)
        printCell(col, "".padStart(CELL_WIDTH, "-"))

    for (let row = 0; row < data.length; row++) {
        for (let col = 0; col < nCols; col++) {

            const cellValue = accessor(data, row, col);
            printCell(col, cellValue);
        }
    }

    process.stdout.write("\n");
}

export function updateData(originalData) {

    const originalRecord = originalData[0];
    const originalLimit = originalRecord["credit_limit"];

    const newLimit = parseFloat(originalLimit) + 2000.00;
    const newRecord = {...originalRecord, credit_limit: newLimit};

    const newData = [...originalData];
    newData[0] = newRecord;

    return newData;
}

function saveDataFromMemory(schemaId, originalDataId, newData) {

    // Encode array of JavaScript objects as JSON
    const json = JSON.stringify(newData);
    const bytes = new TextEncoder().encode(json);

    const request = tracdap.api.DataWriteRequest.create({

        tenant: "ACME_CORP",

        // The original version that is being updated
        priorVersion: originalDataId,

        // Schema, format and content are provided as normal
        schemaId: schemaId,
        format: "text/json",
        content: bytes,

        // Existing tags are retained during updates
        // Use tag updates if tags need to be added, removed or altered
        tagUpdates: [
            { attrName: "change_description", value: { stringValue: "Increase limit for customer A36456" } }
        ]
    });

    return dataApi.updateSmallDataset(request).then(dataId => {

        console.log(`Updated dataset ${dataId.objectId} to version ${dataId.objectVersion}`);

        return dataId;
    });
}

export async function main() {

    console.log("Looking for a schema to use...")
    const schemaId = await searchForSchema();
    const csvData = await loadFromDisk("data/customer_data.csv");

    const dataId = await saveDataToTrac(schemaId, csvData);
    const {schema, data} = await loadDataFromTrac(dataId);

    displayTable(schema, data);

    const newData = updateData(data);

    await saveDataFromMemory(schemaId, dataId, newData);
}
