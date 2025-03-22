/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
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
const adminTransport = tracdap.setup.transportForTarget(tracdap.api.TracAdminApi, "http", "localhost", 8080);

// Create a TRAC API instance for the admin API
const adminApi = new tracdap.api.TracAdminApi(adminTransport);


export function addStorage() {

    // Use the admin API to create a storage resource

    const pwd = process.cwd();

    const request = tracdap.api.ConfigWriteRequest.create({

        tenant: "ACME_CORP",
        configClass: "trac_resources",
        configKey: "STORAGE1",

        definition: {
            objectType: tracdap.ObjectType.RESOURCE,
            resource: {
                protocol: "LOCAL",
                properties: {
                    rootPath: `${pwd}/data/storage1`
                }
            }
        }
    });

    return adminApi.createConfigObject(request).then(resource => {

        console.log(`Created storage resource ${resource.configKey} version ${resource.configVersion}`);

        return resource;
    });
}

export async function main() {

    console.log("Adding a storage resource for STORAGE1...");

    const resource = await addStorage();

    console.log(JSON.stringify(resource, null, 2));
}
