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

package com.accenture.trac.common.storage.local;

import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.storage.IFileStorage;
import com.accenture.trac.common.storage.IStoragePlugin;

import java.util.List;
import java.util.Properties;


public class LocalStoragePlugin implements IStoragePlugin {

    private static final String PLUGIN_NAME = "LOCAL_STORAGE";
    private static final List<String> PROTOCOLS = List.of("LOCAL");

    public static final String CONFIG_ROOT_DIR = "rootDir";

    @Override
    public String name() {
        return PLUGIN_NAME;
    }

    @Override
    public List<String> protocols() {
        return PROTOCOLS;
    }

    @Override
    public IFileStorage createFileStorage(String storageKey, String protocol, Properties config) {

        if ("LOCAL".equals(protocol)) {
            return new LocalFileStorage(storageKey, config);
        }

        throw new EUnexpected();

    }
}
