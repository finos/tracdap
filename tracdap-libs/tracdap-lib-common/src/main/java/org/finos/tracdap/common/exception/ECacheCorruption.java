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

package org.finos.tracdap.common.exception;


/**
 * Thrown when a cache entry has become corrupt
 *
 * <p>This error indicates that an individual cache entry has become corrupt and is no longer
 * readable. TRAC will try to handle this error so that individual corrupt entries do not
 * pollute the cache and stop it from functioning. For cache query operations corrupt
 * entries will be returned with no value, cacheError() will return an ECacheCorruption error.
 * Calls to getEntry() will throw this exception if the entry is corrupt.
 *
 * <p>One cause of cache corruption is jobs in the cache that reference old class files.
 * This could happen after the upgrade of an executor plugin, if there are incompatible
 * changes in the executor state class. Although TRAC has error handling to clean up affected
 * jobs, executors should take care when updating their state classes to avoid this issue.</p>
 */
public class ECacheCorruption extends ECache {

    public ECacheCorruption(String message, Throwable cause) {
        super(message, cause);
    }

    public ECacheCorruption(String message) {
        super(message);
    }
}
