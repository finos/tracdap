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

// Use tracdap.setup to create an RPC transport pointed at your TRAC server
// For code that will run in the browser, use transportForBrowser to direct requests to the origin server
const metaTransport = tracdap.setup.transportForTarget(tracdap.api.TracMetadataApi, "http", "localhost", 8080);

// Create a TRAC API instance for the Metadata API
const metaApi = new tracdap.api.TracMetadataApi(metaTransport);


export function createSchema() {

    // Build the schema definition we want to save
    const schema = tracdap.metadata.SchemaDefinition.create({

        schemaType: tracdap.SchemaType.TABLE,
        table: {
            fields: [
                {
                    fieldName: "customer_id", fieldType: tracdap.STRING, businessKey: true,
                    label: "Unique customer account number"
                },
                {
                    fieldName: "customer_type", fieldType: tracdap.STRING, categorical: true,
                    label: "Is the customer an individual, company, govt agency or something else"
                },
                {
                    fieldName: "customer_name", fieldType: tracdap.STRING,
                    label: "Customer's common name"
                },
                {
                    fieldName: "account_open_date", fieldType: tracdap.DATE,
                    label: "Date the customer account was opened"
                },
                {
                    fieldName: "credit_limit", fieldType: tracdap.DECIMAL,
                    label: "Ordinary credit limit on the customer account, in USD"
                }
            ]
        }
    });

    // Set up a metadata write request, to save the schema with some informational tags
    const request = tracdap.api.MetadataWriteRequest.create({

        tenant: "ACME_CORP",
        objectType: tracdap.ObjectType.SCHEMA,

        definition: {
            objectType: tracdap.ObjectType.SCHEMA,
            schema: schema
        },

        tagUpdates: [
            { attrName: "schema_type", value: { stringValue: "customer_records" } },
            { attrName: "business_division", value: { stringValue: "WIDGET_SALES" } },
            { attrName: "description", value: { stringValue: "A month-end snapshot of customer accounts" } },
        ]
    });

    // Use the metadata API to create the object
    return metaApi.createObject(request).then(schemaId => {

        console.log(`Created schema ${schemaId.objectId} version ${schemaId.objectVersion}`);

        return schemaId;
    });
}

export function loadTag(tagHeader) {

    const request = tracdap.api.MetadataReadRequest.create({

        tenant: "ACME_CORP",
        selector: tagHeader
    });

    return metaApi.readObject(request);
}

export async function main() {

    console.log("Creating a schema...");

    const schemaId = await createSchema();

    console.log("Loading the schema...");

    const schemaTag = await loadTag(schemaId);

    console.log(JSON.stringify(schemaTag, null, 2));
}
