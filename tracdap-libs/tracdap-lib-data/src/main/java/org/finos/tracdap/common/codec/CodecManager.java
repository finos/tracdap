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

package org.finos.tracdap.common.codec;

import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EPluginNotAvailable;
import org.finos.tracdap.common.plugin.IPluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class CodecManager implements ICodecManager {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Map<String, ICodec> codecs;

    public CodecManager(IPluginManager plugins, ConfigManager configManager) {

        var protocols = plugins.availableProtocols(ICodec.class);
        var codecs = new HashMap<String, ICodec>();

        for (var protocol : protocols) {
            var codec = plugins.createService(ICodec.class, protocol, configManager);
            codecs.put(protocol.toLowerCase(), codec);
        }

        this.codecs = Collections.unmodifiableMap(codecs);
    }

    @Override
    public String getDefaultFileExtension(String format) {

        return getCodec(format).defaultFileExtension();
    }

    @Override
    public String getDefaultMimeType(String format) {

        return getCodec(format).defaultMimeType();
    }

    @Override
    public ICodec getCodec(String format) {

        var protocol = format.toLowerCase();

        if (codecs.containsKey(protocol)) {

            return codecs.get(protocol);
        }
        else {

            // Make a slightly prettier message that the regular plugin not available message
            // Since this error is likely to be user-facing
            var message = String.format("Data format not supported: [%s] (no suitable codec is available)", format);
            log.error(message);
            throw new EPluginNotAvailable(message);
        }
    }
}
