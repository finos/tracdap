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

import {trac} from 'trac-web-api';

import {searchForSchema} from './metadata_mojo';
import {loadFromDisk} from './util';

// Create the Data API
const dataApiRpcImpl = trac.setup.rpcImplForTarget(trac.api.TracDataApi, "http", "localhost", 8080);
const dataApi = new trac.api.TracDataApi(dataApiRpcImpl);


export function saveDataToTrac(schemaId, csvData) {

    const request = trac.api.DataWriteRequest.create({

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
    const request = trac.api.DataReadRequest.create({

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

export function renderTable(schema, data, accessor) {

    const CELL_WIDTH = 10;

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

export async function main() {

    console.log("Looking for a schema to use...")
    const schemaId = await searchForSchema();
    const csvData = await loadFromDisk("data/customer_data.csv");

    const dataId = await saveDataToTrac(schemaId, csvData);

    const {schema, data} = await loadDataFromTrac(dataId);

    const accessor = (table, row, col) => {
        const fieldName = schema.table.fields[col].fieldName;
        return table[row][fieldName];
    }

    renderTable(schema, data, accessor);
}
