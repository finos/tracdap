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

import {loadTag} from "./hello_world.js";

const metaTransport = tracdap.setup.transportForTarget(tracdap.api.TracMetadataApi, "http", "localhost", 8080);
const metaApi = new tracdap.api.TracMetadataApi(metaTransport);


export function searchForSchema() {

    const searchRequest = tracdap.api.MetadataSearchRequest.create({

        tenant: "ACME_CORP",
        searchParams: {

            objectType: tracdap.ObjectType.SCHEMA,

            search: {  term: {

                attrName: "schema_type",
                attrType: tracdap.STRING,
                operator: tracdap.SearchOperator.EQ,
                searchValue: { stringValue: "customer_records" }
            }}
        }
    })

    return findFirst(searchRequest);
}

export function findFirst(searchRequest) {

    return metaApi.search(searchRequest).then(response => {

        const nResults = response.searchResult.length;

        if (nResults === 0)
            throw new Error("No matching search results");

        console.log(`Got ${nResults} search result(s), picking the first one`);

        return response.searchResult[0].header;
    });
}

export function logicalSearch() {

    const schemaTypeCriteria = tracdap.metadata.SearchExpression.create({
        term: {
            attrName: "schema_type",
            attrType: tracdap.STRING,
            operator: tracdap.SearchOperator.EQ,
            searchValue: { stringValue: "customer_records" }
        }
    });

    const businessDivisionCriteria = tracdap.metadata.SearchExpression.create({
        term: {
            attrName: "business_division",
            attrType: tracdap.STRING,
            operator: tracdap.SearchOperator.IN,
            searchValue: { arrayValue: {
                items: [
                    { stringValue: "WIDGET_SALES" },
                    { stringValue: "WIDGET_SERVICES" },
                    { stringValue: "WIDGET_RND_ACTIVITIES" }
                ]
            }}
        }
    });

    const logicalSearch = tracdap.metadata.SearchExpression.create({
        logical: {
            operator: tracdap.LogicalOperator.AND,
            expr: [
                schemaTypeCriteria,
                businessDivisionCriteria
            ]
        }
    });

    const searchRequest = tracdap.api.MetadataSearchRequest.create({

        tenant: "ACME_CORP",
        searchParams: {
            objectType: tracdap.ObjectType.SCHEMA,
            search: logicalSearch
        }
    });

    return findFirst(searchRequest);
}

export async function main() {

    console.log("Searching for schemas...")

    const schemaId = await searchForSchema();

    console.log("Loading the first matching schema...")

    const schemaTag = await loadTag(schemaId);

    console.log(JSON.stringify(schemaTag, null, 2));

    console.log("Running logical search expression...")

    const schemaId2 = await logicalSearch();

    console.log(`Logical search found schema ID ${schemaId2.objectId} version ${schemaId2.objectVersion}`);
}
