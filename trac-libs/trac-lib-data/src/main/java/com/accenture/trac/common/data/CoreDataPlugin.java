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

package com.accenture.trac.common.data;

import com.accenture.trac.common.codec.ICodec;
import com.accenture.trac.common.codec.csv.CsvCodec;
import com.accenture.trac.common.exception.ETracInternal;
import com.accenture.trac.common.exception.EUnexpected;
import com.accenture.trac.common.plugin.PluginServiceInfo;
import com.accenture.trac.common.plugin.TracPlugin;
import com.accenture.trac.common.storage.IFileStorage;
import com.accenture.trac.common.storage.local.LocalFileStorage;

import java.util.List;
import java.util.Properties;


public class CoreDataPlugin extends TracPlugin {

    private static final String PLUGIN_NAME = "CORE_DATA";

    private static final String CSV_CODEC_NAME = "CSV_CODEC";
    private static final String JSON_CODEC_NAME = "JSON_CODEC";
    private static final String ARROW_STREAM_CODEC_NAME = "ARROW_STREAM";
    private static final String ARROW_FILE_CODEC_NAME = "ARROW_FILE";
    private static final String LOCAL_FILE_STORAGE_NAME = "LOCAL_STORAGE";

    private static final List<PluginServiceInfo> psi = List.of(
            new PluginServiceInfo(PLUGIN_NAME, ICodec.class, ARROW_STREAM_CODEC_NAME, List.of("ARROW_STREAM", "application/vnd.apache.arrow.stream", "application/x-apache-arrow-stream")),
            new PluginServiceInfo(PLUGIN_NAME, ICodec.class, ARROW_FILE_CODEC_NAME, List.of("ARROW_FILE", "application/vnd.apache.arrow.file", "application/x-apache-arrow-file")),
            new PluginServiceInfo(PLUGIN_NAME, ICodec.class, CSV_CODEC_NAME, List.of("CSV", "text/csv")),
            new PluginServiceInfo(PLUGIN_NAME, ICodec.class, JSON_CODEC_NAME, List.of("JSON", "text/json")),
            new PluginServiceInfo(PLUGIN_NAME, IFileStorage.class, LOCAL_FILE_STORAGE_NAME, List.of("LOCAL", "file")));


    @Override
    public String pluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public List<PluginServiceInfo> serviceInfo() {
        return psi;
    }

    @Override @SuppressWarnings("unchecked")
    public <T> T createService(String serviceName, Properties properties) {

        switch (serviceName) {

            case CSV_CODEC_NAME: return (T) new CsvCodec();

            case JSON_CODEC_NAME:
            case ARROW_STREAM_CODEC_NAME:
            case ARROW_FILE_CODEC_NAME:
                throw new ETracInternal("Not implemented yet");

            case LOCAL_FILE_STORAGE_NAME: return (T) new LocalFileStorage(properties);

            default:
                throw new EUnexpected();
        }
    }
}
