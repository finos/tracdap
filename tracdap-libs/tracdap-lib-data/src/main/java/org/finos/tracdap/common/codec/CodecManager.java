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

package org.finos.tracdap.common.codec;

import org.finos.tracdap.common.exception.EPluginNotAvailable;
import org.finos.tracdap.common.plugin.IPluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CodecManager implements ICodecManager {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final IPluginManager plugins;

    public CodecManager(IPluginManager plugins) {
        this.plugins = plugins;
    }

    @Override
    public ICodec getCodec(String format) {

        if (plugins.isServiceAvailable(ICodec.class, format))
            return plugins.createService(ICodec.class, format);

        else {

            // Make a slightly prettier message that the regular plugin not available message
            // Since this error is likely to be user-facing
            var message = String.format("Codec not available for data format: [%s]", format);
            log.error(message);
            throw new EPluginNotAvailable(message);
        }
    }
}
