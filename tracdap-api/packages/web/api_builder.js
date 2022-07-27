/*
 * Copyright 2022 Accenture Global Solutions Limited
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
const pbjs = require('protobufjs-cli/pbjs');
const pbts = require('protobufjs-cli/pbts');


const packages = {
    'tracdap/metadata': '../../tracdap-metadata/src/main/proto',
    'tracdap/api': '../../tracdap-services/src/main/proto'
};

const mapping = require('./api_mapping.js');

const jsOutFile = "tracdap.js";
const tsOutFile = "tracdap.d.ts"

const pbjsArgs = [
    "--target", "static-module",
    "--wrap", "./wrapper.js",
    "--root", "tracdap",
    "--force-number",               // Use native JavaScript numbers for numeric types
    "--null-defaults",              // Set optional fields to null if they are not present
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

function generateJsMapping(mappingDict, valuePrefix, indent) {

    let mappingCode = "{\n";

    for (const [mappingKey, mappingValue] of Object.entries(mappingDict)) {

        mappingCode += "\t".repeat(indent) + "\t\"" + mappingKey + "\": " + valuePrefix + mappingValue + ",\n";
    }

    mappingCode += "\t".repeat(indent) +"}";

    return mappingCode;
}

function generateEnumMapping(mappingDict) {

    let mappingCode = "\n";

    for (const [mappingKey, mappingValue] of Object.entries(mappingDict)) {

        mappingCode += "\texport import " + mappingKey + " = " + mappingValue + ";\n";
    }

    return mappingCode;
}

function generateBasicTypeMapping(mappingDict) {

    let mappingCode = "\n";

    for (const [mappingKey, mappingValue] of Object.entries(mappingDict)) {

        mappingCode += "\tconst " + mappingKey + " = " + mappingValue + ";\n";
    }

    return mappingCode;
}

function writeJsSubstitutions(methodTypes, apiMapping) {

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

function writeTsSubstitutions(importMapping) {

    fs.readFile(tsOutFile, 'utf8', function (err, tsCode) {

        if (err) {
            return console.log(err);
        }

        // Write substitutions for enum and basic type mappings
        // Enums and basic types are mapped into the top level tracdap namespace

        const setupLocation = tsCode.indexOf("namespace setup {")
        const insertPoint = tsCode.lastIndexOf("\n", tsCode.lastIndexOf("\n", setupLocation) - 1);

        tsCode = [
            tsCode.slice(0, insertPoint),
            importMapping,
            tsCode.slice(insertPoint)
        ].join("\n");

        // Write substitution for $SERVICE_TYPE
        // This is to get around a gap in the JSDoc generator for .d.ts files
        // It cannot handle typedef statements using "typeof", which contain a space

        // This could (should) be replaced with a more general approach,
        // and maintain a set of variable substitutions in api_mapping.js

        const serviceInsertPoint = tsCode.indexOf("$SERVICE_TYPE")

        tsCode = [
            tsCode.slice(0, serviceInsertPoint),
            "typeof $protobuf.rpc.Service",
            tsCode.slice(serviceInsertPoint + "$SERVICE_TYPE".length)
        ].join("");

        fs.writeFile(tsOutFile, tsCode, 'utf8', function (err) {
            if (err) return console.log(err);
        });
    });
}


function main() {

    copyProto();

    pbjs.main(pbjsArgs, () => {

        const methodTypes = generateJsMapping(mapping.methodTypes, "", 2);
        const apiMappingDict = {...mapping.enumMapping, ...mapping.basicTypeMapping};
        const apiMapping = generateJsMapping(apiMappingDict, "$root.", 1);
        writeJsSubstitutions(methodTypes, apiMapping);
    });

    pbts.main(pbtsArgs, () => {

        const enumMapping = generateEnumMapping(mapping.enumMapping);
        const basicTypeMapping = generateBasicTypeMapping(mapping.basicTypeMapping);
        const tsMapping = enumMapping + basicTypeMapping;
        writeTsSubstitutions(tsMapping);
    });
}

main();
