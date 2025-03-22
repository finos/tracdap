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

import fs from "fs";


async function createDirectory(storageDir) {

    return new Promise((resolve, reject) => {

        fs.mkdir(storageDir, {recursive: true}, (err, data) => {

            if (err)
                reject(err);
            else
                resolve(data);
        });
    });
}

async function addStorage(storageDir) {

    // Use the admin API to create a storage resource

    const request = tracdap.api.ConfigWriteRequest.create({

        tenant: "ACME_CORP",
        configClass: "trac_resources",
        configKey: "STORAGE1",

        definition: {
            objectType: tracdap.ObjectType.RESOURCE,
            resource: {
                resourceType: tracdap.ResourceType.INTERNAL_STORAGE,
                protocol: "LOCAL",
                properties: {
                    rootPath: storageDir
                }
            }
        }
    });

    return adminApi.createConfigObject(request).then(resource => {

        console.log(`Created storage resource ${resource.entry.configKey} version ${resource.entry.configVersion}`);
        console.log(`Storage path is ${request.definition.resource.properties["rootPath"]}`)

        return resource;
    });
}

export async function main() {

    console.log("Adding a storage resource for STORAGE1...");

    const storageDir = `${process.cwd()}/data/storage1`;
    await createDirectory(storageDir);

    const resource = await addStorage(storageDir);

    console.log(JSON.stringify(resource, null, 2));
}
