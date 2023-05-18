/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.test.helpers;

import org.finos.tracdap.common.concurrent.ExecutionContext;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.storage.IFileStorage;
import org.finos.tracdap.common.storage.IStorageManager;
import org.finos.tracdap.config.PlatformConfig;

import io.netty.channel.EventLoopGroup;

import java.time.Duration;

import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.getResultOf;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.waitFor;


public class StorageTestHelpers {

    // A similar method could be used to create the storage prefix,
    // if storage implementations are checking exists() on startup

    public static void deleteStoragePrefix(
            ConfigManager config,
            PluginManager plugins,
            EventLoopGroup elg)
            throws Exception {

        // Assuming "prefix" is common across all bucket storage implementations
        // Create a storage instance with the prefix removed, then do rmdir on the prefix

        var platformConfig = config.loadRootConfigObject(PlatformConfig.class);
        var execCtx = new ExecutionContext(elg.next());

        for (var storageBucket : platformConfig.getStorage().getBucketsMap().entrySet()) {

            if (!storageBucket.getValue().containsProperties("prefix"))
                continue;

            var storageConfig = storageBucket.getValue()
                    .toBuilder()
                    .putProperties(IStorageManager.PROP_STORAGE_KEY, storageBucket.getKey())
                    .removeProperties("prefix")
                    .build();

            var prefix = storageBucket.getValue().getPropertiesOrThrow("prefix");

            try (var storage = plugins.createService(IFileStorage.class, storageConfig, config)) {

                storage.start(elg);

                var rmdir = storage.rmdir(prefix, execCtx);
                waitFor(Duration.ofSeconds(30), rmdir);
                getResultOf(rmdir);
            }
        }
    }

}
