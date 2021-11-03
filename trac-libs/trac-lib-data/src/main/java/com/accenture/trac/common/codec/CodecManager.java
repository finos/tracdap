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

package com.accenture.trac.common.codec;

import com.accenture.trac.common.data.DataBlock;
import com.accenture.trac.common.storage.IStoragePlugin;
import com.accenture.trac.common.storage.StorageManager;
import com.accenture.trac.metadata.SchemaDefinition;
import io.netty.buffer.ByteBuf;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.Flow;


public class CodecManager implements ICodecManager {

//    private final Logger log = LoggerFactory.getLogger(getClass());
//
//    private final Map<String, IFormatPlugin> plugins;
//    private final Map<String, StorageManager.StorageBackend> storage;
//
//    public CodecManager() {
//        this.plugins = new HashMap<>();
//        this.storage = new HashMap<>();
//    }
//
//    public void initFormatPlugins() {
//
//        log.info("Looking for format plugins...");
//
//        var availablePlugins = ServiceLoader.load(IFormatPlugin.class);
//
//        for (var plugin: availablePlugins) {
//
//            var discoveryMsg = String.format("Storage plugin: [%s] (protocols: %s)",
//                    plugin.name(),
//                    String.join(", ", plugin.protocols()));
//
//            log.info(discoveryMsg);
//
//            for (var protocol : plugin.protocols())
//                plugins.put(protocol, plugin);
//        }
//    }


    @Override
    public Flow.Processor<DataBlock, ByteBuf> getEncoder(
            String format, SchemaDefinition schema,
            Map<String, String> options) {

        return null;
    }

    @Override
    public Flow.Processor<ByteBuf, DataBlock> getDecoder(
            String format, SchemaDefinition schema,
            Map<String, String> options) {

        return null;
    }
}
