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

import {searchForSchema} from './metadata_mojo';
import {saveDataToTrac, renderTable} from './using_data';
import {loadFromDisk} from './util';

import * as arrow from 'apache-arrow';

// Create the Data API
const dataTransport = tracdap.setup.transportForTarget(tracdap.api.TracDataApi, "http", "localhost", 8080);
const dataApi = new tracdap.api.TracDataApi(dataTransport);


function loadArrowData(dataId) {

    // Ask for the dataset in Arrow IPC stream format
    const request = tracdap.api.DataReadRequest.create({

        tenant: "ACME_CORP",

        selector: dataId,
        format: "application/vnd.apache.arrow.stream"
    });

    return dataApi.readSmallDataset(request).then(response => {

        // Create an Arrow table directly from the binary content of the response
        const table = new arrow.tableFromIPC(response.content);
        table.length = table.numRows;

        return {schema: response.schema, data: table};
    })
}

export async function main() {

    console.log("Looking for a schema to use...")
    const schemaId = await searchForSchema();

    const csvData = await loadFromDisk("data/customer_data.csv");
    const dataId = await saveDataToTrac(schemaId, csvData);

    const {schema, data} = await loadArrowData(dataId);

    console.log("Displaying arrow data")

    const accessor = (table, row, col) => table.getChildAt(col).get(row);

    renderTable(schema, data, accessor);

    console.log("Displaying JavaScript data")

    const jsData = data.toArray();
    const jsAccessor = (table, row, col) => {
        const fieldName = schema.table.fields[col].fieldName;
        return table[row][fieldName];
    }

    renderTable(schema, jsData, jsAccessor);
}
