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


const packages = {
    'trac/metadata': '../../trac-metadata/src/main/proto',
    'trac/api': '../../trac-services/src/main/proto'
};

const args = [
    "--target", "static-module",
    "--root", "trac.metadata",
    "--wrap", "commonjs",
    "--force-number",
    "--path", './build',
    "--out", "trac.js"
];

fs.mkdirSync('./build', {recursive: true});

for (const [pkg, pkgDir] of Object.entries(packages)) {

    const srcDir = path.join(pkgDir, pkg);
    const dstDir = path.join('./build', pkg);

    fs.mkdirSync(dstDir, {recursive: true});

    fs.readdirSync(srcDir).forEach(file => {

        const srcProto = path.join(srcDir, file);
        const dstProto = path.join(dstDir, file);

        fs.copyFileSync(srcProto, dstProto);

        args.push(pkg + '/' + file);
    })

}

pbjs.main(args);
