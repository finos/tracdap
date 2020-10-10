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

const protoDir = '../..//trac-metadata/src/main/proto';

const packages = [
    'trac/metadata'
];

fs.mkdirSync('build', {recursive: true});

packages.forEach(pkg => {

    const pkgInDir = path.join(protoDir, pkg);
    const pkgOutFile = path.join('build', path.basename(pkg) + ".js");

    const args = [
        "--target", "static-module",
        "--root", "trac.metadata",
        "--force-number",
        "--path", protoDir,
        "--out", pkgOutFile
    ];

    fs.readdirSync(pkgInDir).forEach(file => {

        args.push(pkg + '/' + file);
    })

    pbjs.main(args);
});
