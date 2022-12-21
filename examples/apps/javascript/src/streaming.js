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

import {loadFromDisk} from './util';
import fs from 'fs';

// Create the Data API
const rpcOptions = {transport: "trac", debug: true};
const dataRpc = tracdap.setup.rpcImplForTarget(tracdap.api.TracDataApi, "http", "localhost", 8080, rpcOptions);
const dataApi = new tracdap.api.TracDataApi(dataRpc);

const LARGE_CSV_FILE = "../../../tracdap-services/tracdap-svc-data/src/test/resources/large_csv_data_100000.csv";


async function saveStreamingData(csvData) {

    console.log("Start streaming upload");

    // Build the schema definition we want to save
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

    return new Promise((resolve, reject) => {

        const stream = tracdap.setup.newStream(dataApi);

        const saveResult = stream.createDataset(request0)
            .then(result => resolve(result))
            .catch(e => reject(e));
        // .catch(err => console.log(("CLIENT CODE: Got error: " + err.message)));

        let chunkCount = 0

        csvData.on('data', chunk => {
            chunkCount += 1;
            stream.createDataset(tracdap.api.DataWriteRequest.create({content: chunk}))
        });
        csvData.on('close', () => {console.log("EOS, total chunks = " + chunkCount); stream.end();});
        // csvData.on('error', e => stream.end());

        // stream.createDataset(data1)
        //stream.end(true);
        // stream.createDataset(data2)
        // stream.createDataset(data3)
        // stream.end();

        // stream.on("data", e => {
        //     console.log(JSON.stringify(e));
        // });

        // return saveResult;

    });
}


function loadStreamingData(dataId) {

    // Ask for the dataset in Arrow IPC stream format
    const request = tracdap.api.DataReadRequest.create({

        tenant: "ACME_CORP",

        selector: dataId,
        format: "text/csv"
    });

    return new Promise((resolve, reject) => {

        const stream = tracdap.setup.newStream(dataApi);

        let allText = ""

        stream.on("data", response => {

            console.log("CLIENT CODE: Got a message");
            if (response.content) {
                const text = new TextDecoder().decode(response.content);
                allText += text;
                //console.log(text);
            }
            else {
                console.log("no content");
            }
        });

        stream.on("end", () => {

            console.log(`Got ${allText.split("\n").length} lines`);
            resolve(allText);
        });
        stream.on("error", reject);

        stream.readDataset(request).then(msg0 => {



        })

        // stream.on("end", () => {
        //
        //     console.log("CLIENT CODE: Got end of stream");
        //     resolve();
        // })
        //
        // stream.on("error", err => {
        //
        //     console.log("CLIENT CODE: Got an error: " + err.status + ", " + err.message);
        //     reject(err);
        // })

        return stream
    });
}

export async function main() {

    console.log("Looking for a schema to use...")
    const csvData = fs.createReadStream(LARGE_CSV_FILE);  // await loadFromDisk(LARGE_CSV_FILE);

    const dataId = await saveStreamingData(csvData);

    console.log(JSON.stringify(dataId));

    // const x = tracdap.metadata.TagHeader.decode(dataId);
    // console.log(JSON.stringify(x));

    await loadStreamingData(dataId);
    await loadStreamingData(dataId);
    await loadStreamingData(dataId);
    await loadStreamingData(dataId);
    await loadStreamingData(dataId);
    await loadStreamingData(dataId);
    await loadStreamingData(dataId);

    const csvData2 = fs.createReadStream(LARGE_CSV_FILE);  // await loadFromDisk(LARGE_CSV_FILE);
    const dataId2 = await saveStreamingData(csvData2);


    await loadStreamingData(dataId2);

    console.log("All done");
}

