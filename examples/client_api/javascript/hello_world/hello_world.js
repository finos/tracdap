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

// To run these examples outside of a browser, XMLHttpRequest is required
import xhr2 from 'xhr2';
global.XMLHttpRequest = xhr2.XMLHttpRequest;


// Use trac.setup to create an RPC instance pointed at your TRAC server
// For code that will run in the browser, use rpcImplForBrowser to direct requests to the origin server
const metaApiRpcImpl = trac.setup.rpcImplForTarget(trac.api.TracMetadataApi, "http", "localhost", 8080);

// Create the TRAC API
const metaApi = new trac.api.TracMetadataApi(metaApiRpcImpl);


function createSchema() {

    // Build the schema definition we want to save

    const schema = trac.metadata.SchemaDefinition.create({

        schemaType: trac.SchemaType.TABLE,
        table: {
            fields: [
                {
                    fieldName: "field_one",
                    fieldType: trac.STRING
                },
                {
                    fieldName: "field_two",
                    fieldType: trac.FLOAT
                },
            ]
        }
    });

    // Build a request object, to save the schema with some informational tags

    const request = trac.api.MetadataWriteRequest.create({

        tenant: "ACME_CORP",
        objectType: trac.ObjectType.SCHEMA,

        definition: {
            objectType: trac.ObjectType.SCHEMA,
            schema: schema
        },

        tagUpdates: [
            { attrName: "schema_type", value: { stringValue: "custom_records" } },
            { attrName: "description", value: { stringValue: "A month-end snapshot of customer accounts" } },
        ]

    });

    // Call createObject on the metadata API

    return metaApi.createObject(request)
}

async function main() {

    console.log("Creating schema...")

    await createSchema()
        .then(tag => {console.log("Schema ID: " + JSON.stringify(tag)); return tag;})
        .catch(err => console.log("Error: " + JSON.stringify(err)));
}

main();
