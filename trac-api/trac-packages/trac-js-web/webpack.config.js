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

const path = require('path');

const config = {};
config.src_dir = path.resolve('./src')
config.npm_dir = path.resolve('./node_modules')
config.build_dir = path.resolve('./build');
config.dist_dir = path.resolve('./dist')


module.exports = (env, argv) => {

    const PROD = (argv.mode === 'production');

    return {

        entry: config.build_dir + '/metadata.js',

        output: {
            path: config.dist_dir,
            filename: PROD ? 'metadata.min.js' : 'metadata.js'
        },

        resolve: {
            modules: [
                config.src_dir,
                config.build_dir,
                config.npm_dir
            ],
            extensions: [
                '.js'
            ]
        },

        optimization: {
            minimize: PROD
        },

        devtool: "source-map"
    }
};
