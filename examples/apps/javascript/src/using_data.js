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

import {searchForSchema} from './hello_world.js';
import {loadFromDisk} from './util.js';

// Create the Data API
const dataApiRpcImpl = trac.setup.rpcImplForTarget(trac.api.TracDataApi, "http", "localhost", 8080);
const dataApi = new trac.api.TracDataApi(dataApiRpcImpl);


function saveDataToTrac(schemaId, csvData) {

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
    })

    return dataApi.createSmallDataset(request).then(header => {

        console.log(`Created dataset ${header.objectId} version ${header.objectVersion}`);

        return header;
    });
}

function loadDataFromTrac(dataId) {

    const request = trac.api.DataReadRequest.create({

        tenant: "ACME_CORP",

        selector: dataId,
        format: "text/json"
    });

    return dataApi.readSmallDataset(request).then(response => {

        const nFields = response.schema.table.fields.length;

        console.log(`Retrieved dataset ${dataId.objectId} version ${dataId.objectVersion}`);
        console.log(`Schema contains ${nFields} fields`);

        const text = new TextDecoder().decode(response.content);
        return JSON.parse(text);
    })
}


export async function main() {

    console.log("Looking for a schema to use...")
    const schemaId = await searchForSchema();

    const csvData = await loadFromDisk("data/customer_data.csv");
    const dataId = await saveDataToTrac(schemaId, csvData);

    const dataset = await loadDataFromTrac(dataId);

    console.log(JSON.stringify(dataset, null, 2));
}
