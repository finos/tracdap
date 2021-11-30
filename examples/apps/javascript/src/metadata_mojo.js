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

import {loadTag} from "./hello_world";

const metaApiRpcImpl = trac.setup.rpcImplForTarget(trac.api.TracMetadataApi, "http", "localhost", 8080);
const metaApi = new trac.api.TracMetadataApi(metaApiRpcImpl);


export function searchForSchema() {

    const searchRequest = trac.api.MetadataSearchRequest.create({

        tenant: "ACME_CORP",
        searchParams: {

            objectType: trac.ObjectType.SCHEMA,

            search: {  term: {

                attrName: "schema_type",
                attrType: trac.STRING,
                operator: trac.SearchOperator.EQ,
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

    const schemaTypeCriteria = trac.metadata.SearchExpression.create({
        term: {
            attrName: "schema_type",
            attrType: trac.STRING,
            operator: trac.SearchOperator.EQ,
            searchValue: { stringValue: "customer_records" }
        }
    });

    const businessDivisionCriteria = trac.metadata.SearchExpression.create({
        term: {
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
        }
    });

    const logicalSearch = trac.metadata.SearchExpression.create({
        logical: {
            operator: trac.LogicalOperator.AND,
            expr: [
                schemaTypeCriteria,
                businessDivisionCriteria
            ]
        }
    });

    const searchRequest = trac.api.MetadataSearchRequest.create({

        tenant: "ACME_CORP",
        searchParams: {
            objectType: trac.ObjectType.SCHEMA,
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
