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


// To run these examples outside of a browser, XMLHttpRequest is required
import xhr2 from 'xhr2';
global.XMLHttpRequest = xhr2.XMLHttpRequest;


const ALL_EXAMPLES = [
    "hello_world",
    "metadata_mojo",
    "using_data",
    "arrow"
]

async function runExample(exampleName) {

    console.log("Running example: " + exampleName);

    const module = `./src/${exampleName}.js`
    const example = await import(module);

    await example.main();
}

(async () => {

    try {

        const examples = process.argv.length > 2
            ? process.argv.slice(2)
            : ALL_EXAMPLES;

        for (let i = 0; i < examples.length; i++) {

            const exampleName = examples[i];
            await runExample(exampleName);
        }
    }
    catch (err) {

        if (err.hasOwnProperty("message"))
            console.log(err.message);
        else
            console.log(JSON.stringify(err));

        // Ensure errors escape so the process is marked as failed
        throw err;
    }

})()
