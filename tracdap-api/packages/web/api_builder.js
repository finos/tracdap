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
    "--null-semantics",             // Make nullability in TS / JSDoc respect optional semantics
    "--path", './build',
    "--out", jsOutFile
];

const pbtsArgs = [
    "-o", tsOutFile,
    jsOutFile
]

const PUBLIC_API_BUILD = true;
const PUBLIC_API_EXCLUSIONS = [
    /.*[/\\]internal$/,
    /.*_trusted\.proto$/
]

function isPublicApi(path) {

    return ! PUBLIC_API_EXCLUSIONS.some(exclusion => exclusion.test(path));
}

function copyProto() {

    fs.mkdirSync('./build', {recursive: true});

    for (const [pkg, pkgDir] of Object.entries(packages)) {

        const srcDir = path.join(pkgDir, pkg);
        const dstDir = path.join('./build', pkg);

        fs.mkdirSync(dstDir, {recursive: true});

        copyProtoDir(srcDir, dstDir, pkg);
    }
}

function copyProtoDir(srcDir, dstDir, pkg) {

    fs.readdirSync(srcDir).forEach(file => {

        const srcFile = path.join(srcDir, file);
        const dstFile = path.join(dstDir, file);
        const pkgFile = path.join(pkg, file);

        if (PUBLIC_API_BUILD && !isPublicApi(pkgFile)) {
            console.log(`Excluding non-public API [${pkgFile}]`)
        }
        else if (fs.lstatSync(srcFile).isDirectory()) {
            const childPkg = path.join(pkg, file)
            fs.mkdirSync(dstFile, {recursive: true});
            copyProtoDir(srcFile, dstFile, childPkg);
        }
        else {
            fs.copyFileSync(srcFile, dstFile);
            pbjsArgs.push(pkgFile);
        }
    });
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

    pbjs.main(pbjsArgs, (err, _) => {

        if (err)
            throw err;

        const methodTypes = generateJsMapping(mapping.methodTypes, "", 2);
        const apiMappingDict = {...mapping.enumMapping, ...mapping.basicTypeMapping};
        const apiMapping = generateJsMapping(apiMappingDict, "$root.", 1);
        writeJsSubstitutions(methodTypes, apiMapping);
    });

    pbts.main(pbtsArgs, (err, _) => {

        if (err)
            throw err;

        const enumMapping = generateEnumMapping(mapping.enumMapping);
        const basicTypeMapping = generateBasicTypeMapping(mapping.basicTypeMapping);
        const tsMapping = enumMapping + basicTypeMapping;
        writeTsSubstitutions(tsMapping);
    });
}

main();
