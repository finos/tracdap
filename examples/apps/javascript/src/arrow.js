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

import {searchForSchema} from './hello_world';
import {saveDataToTrac} from './using_data';
import {loadFromDisk} from './util';

import * as arrow from 'apache-arrow';

// Create the Data API
const dataApiRpcImpl = trac.setup.rpcImplForTarget(trac.api.TracDataApi, "http", "localhost", 8080);
const dataApi = new trac.api.TracDataApi(dataApiRpcImpl);


function loadArrowData(dataId) {

    // Ask for the dataset in Arrow IPC stream format
    const request = trac.api.DataReadRequest.create({

        tenant: "ACME_CORP",

        selector: dataId,
        format: "application/vnd.apache.arrow.stream"
    });

    return dataApi.readSmallDataset(request).then(response => {

        // Create an Arrow table directly from the binary content of the response
        return arrow.Table.from([response.content]);
    })
}

function displayTable(table) {

    console.log();
    console.log("Field names: ", table.schema.fields.map(f => f.name));
    console.log("Field types: ", table.schema.fields.map(f => f.type.toString()));
    console.log("First row: ", table.get(0).toString());
    console.log("First column: ", table.getColumn("field_one").toArray());
    console.log("Second column: ", table.getColumn("field_two").toArray());
    console.log();

    const accessor = (row, col) => table.getColumnAt(col).get(row);

    const CELL_WIDTH = 10;

    function printCell(col, cellValue) {

        process.stdout.write(`${cellValue}`.padEnd(CELL_WIDTH))

        if (col < table.numCols - 1)
            process.stdout.write("| ")
        else
            process.stdout.write("\n")
    }

    for (let row = 0; row < table.count(); row++) {
        for (let col = 0; col < table.numCols; col++) {
            const cellValue = accessor(row, col);
            printCell(col, cellValue);
        }
    }
}

function filterTable(table) {

    const filter1 = arrow.predicate.col("field_one").eq("hello");
    const filter2 = arrow.predicate.col("field_two").gt(0.5);
    const filter = arrow.predicate.and(filter1, filter2);

    const df = new arrow.DataFrame(table);

    return df.filter(filter);
}

export async function main() {

    console.log("Looking for a schema to use...")
    const schemaId = await searchForSchema();

    const csvData = await loadFromDisk("data/customer_data.csv");
    const dataId = await saveDataToTrac(schemaId, csvData);

    const arrowTable = await loadArrowData(dataId);
    displayTable(arrowTable);

    const filtered = filterTable(arrowTable);
    displayTable(filtered);
}
