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

// Use trac.setup to create an RPC instance pointed at your TRAC server
// For code that will run in the browser, use rpcImplForBrowser to direct requests to the origin server
const metaApiRpcImpl = trac.setup.rpcImplForTarget(trac.api.TracMetadataApi, "http", "localhost", 8080);

// Create the TRAC API
const metaApi = new trac.api.TracMetadataApi(metaApiRpcImpl);


export function createSchema() {

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

    return metaApi.createObject(request).then(header => {

        console.log("New schema created: " + JSON.stringify(header, null, 2));

        return header;
    })
}

export function loadTag(tagHeader) {

    const request = trac.api.MetadataReadRequest.create({

        tenant: "ACME_CORP",
        selector: tagHeader
    });

    return metaApi.readObject(request);
}

export function searchForSchema() {

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
                                { stringValue: "WIDGET_RND_ACTIVITIES" }
                            ]
                        }}
                    }},
                ]

            } }
        }
    })

    return metaApi.search(request).then(response => {

        const nResults = response.searchResult.length;

        if (nResults === 0)
            throw new Error("No matching search results");

        console.log(`Got ${nResults} search result(s), picking the first one`)

        return response.searchResult[0].header;
    });
}

export async function main() {

    try {

        console.log("Creating schema...")

        await createSchema();

        console.log("Searching for schemas...")

        const schemaId = await searchForSchema();

        console.log("Loading the first matching schema...")

        const schemaTag = await loadTag(schemaId);

        console.log(JSON.stringify(schemaTag, null, 2));
    }
    catch (err) {

        if (err.hasOwnProperty("message"))
            console.log(err.message);
        else
            console.log(JSON.stringify(err));
    }
}
