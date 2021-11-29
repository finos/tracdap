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
            { attrName: "schema_type", value: { stringValue: "customer_records" } },
            { attrName: "business_division", value: { stringValue: "WIDGET_SALES" } },
            { attrName: "description", value: { stringValue: "A month-end snapshot of customer accounts" } },
        ]

    });

    // Call createObject on the metadata API

    return metaApi.createObject(request)
}

function searchForSchemas() {

    const request = trac.api.MetadataSearchRequest.create({

        tenant: "ACME_CORP",
        searchParams: {

            objectType: trac.ObjectType.SCHEMA,

            search: { logical: {

                operator: trac.LogicalOperator.AND,
                expr: [

                    { term: {

                        attrName: "schema_type",
                        attrType: trac.STRING,
                        operator: trac.SearchOperator.EQ,
                        searchValue: { stringValue: "customer_records" }
                    }},

                    { term: {

                        attrName: "business_division",
                        attrType: trac.STRING,
                        operator: trac.SearchOperator.IN,
                        searchValue: { arrayValue: {
                            items: [
                                { stringValue: "WIDGET_SALES" },
                                { stringValue: "WIDGET_SERVICES" },
                                { stringValue: "NON_WIDGET_ACTIVITIES" }
                            ]
                        }}
                    }},
                ]

            } }
        }
    })

    return metaApi.search(request);
}

function loadTag(tagHeader) {

    const request = trac.api.MetadataReadRequest.create({

        tenant: "ACME_CORP",
        selector: tagHeader
    });

    return metaApi.readObject(request);
}

async function main() {

    try {

        console.log("Creating schema...")

        const objectHeader = await createSchema();

        console.log("New schema ID: " + JSON.stringify(objectHeader, null, 2));
        console.log("Searching for schemas...")

        const searchResponse = await searchForSchemas();

        console.log(`Found ${searchResponse.searchResult.length} matching schemas`);
        console.log("Loading the first matching schema...")

        const firstMatch = searchResponse.searchResult[0];
        const schemaTag = await loadTag(firstMatch.header);

        console.log(JSON.stringify(schemaTag, null, 2));
    }
    catch (err) {

        if (err.hasOwnProperty("message"))
            console.log(err.message);
        else
            console.log(JSON.stringify(err));
    }
}

main();
