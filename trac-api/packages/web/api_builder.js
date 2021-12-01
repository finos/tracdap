/*
 * Copyright 2020 Accenture Global Solutions Limited
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

const fs = require('fs');
const path = require('path');
const pbjs = require('protobufjs/cli/pbjs');
const pbts = require('protobufjs/cli/pbts');


const packages = {
    'trac/metadata': '../../trac-metadata/src/main/proto',
    'trac/api': '../../trac-services/src/main/proto'
};

const mapping = require('./api_mapping.js');

const jsOutFile = "trac.js";
const tsOutFile = "trac.d.ts"

const pbjsArgs = [
    "--target", "static-module",
    "--wrap", "./wrapper.js",
    "--root", "trac",
    "--force-number",
    "--path", './build',
    "--out", jsOutFile
];

const pbtsArgs = [
    "-o", tsOutFile,
    jsOutFile
]


function copyProto() {

    fs.mkdirSync('./build', {recursive: true});

    for (const [pkg, pkgDir] of Object.entries(packages)) {

        const srcDir = path.join(pkgDir, pkg);
        const dstDir = path.join('./build', pkg);

        fs.mkdirSync(dstDir, {recursive: true});

        fs.readdirSync(srcDir).forEach(file => {

            const srcProto = path.join(srcDir, file);
            const dstProto = path.join(dstDir, file);

            fs.copyFileSync(srcProto, dstProto);

            pbjsArgs.push(pkg + '/' + file);
        })
    }
}

function generateMapping(mappingDict, valuePrefix) {

    let mappingCode = "{\n";

    for (const [mappingKey, mappingValue] of Object.entries(mappingDict)) {

        mappingCode += "\t\t\"" + mappingKey + "\": " + valuePrefix + mappingValue + ",\n";
    }

    mappingCode += "\t}";

    return mappingCode;
}

function generateImportMapping(mappingDict) {

    let mappingCode = "\n";

    for (const [mappingKey, mappingValue] of Object.entries(mappingDict)) {

        mappingCode += "\timport " + mappingKey + " = " + mappingValue + ";\n";
    }

    return mappingCode;
}

function writeApiMapping(methodTypes, apiMapping) {

    fs.readFile(jsOutFile, 'utf8', function (err, jsCode) {

        if (err) {
            return console.log(err);
        }

        jsCode = jsCode.replace(/\$METHOD_TYPE_MAPPING/g, methodTypes);
        jsCode = jsCode.replace(/\$API_MAPPING/g, apiMapping);

        fs.writeFile(jsOutFile, jsCode, 'utf8', function (err) {
            if (err) return console.log(err);
        });
    });
}

function writeImportMapping(importMapping) {

    fs.readFile(tsOutFile, 'utf8', function (err, tsCode) {

        if (err) {
            return console.log(err);
        }

        const setupLocation = tsCode.indexOf("namespace setup {")
        const insertPoint = tsCode.lastIndexOf("\n", tsCode.lastIndexOf("\n", setupLocation) - 1);

        tsCode = [
            tsCode.slice(0, insertPoint),
            importMapping,
            tsCode.slice(insertPoint)
        ].join("\n");

        fs.writeFile(tsOutFile, tsCode, 'utf8', function (err) {
            if (err) return console.log(err);
        });
    });
}

function main() {

    copyProto();

    pbjs.main(pbjsArgs, () => {

        const methodTypes = generateMapping(mapping.methodTypes, "");
        const apiMapping = generateMapping(mapping.apiMapping, "$root.");
        writeApiMapping(methodTypes, apiMapping);
    });

    pbts.main(pbtsArgs, () => {

        const importMapping = generateImportMapping(mapping.apiMapping);
        writeImportMapping(importMapping);
    });
}

main();
