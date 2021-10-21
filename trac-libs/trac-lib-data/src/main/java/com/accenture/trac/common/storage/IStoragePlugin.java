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

package com.accenture.trac.common.storage;

import java.util.List;
import java.util.Properties;


/**
 * Interface for implementing storage plugins to use with the TRAC data service
 *
 * Storage plugins allow the TRAC platform to communicate with different backend data technologies.
 * This interface is for plugins used by the TRAC data service, which enables data access for
 * client applications. Separate plugins are needed to enable storage for the TRAC model runtime,
 * using the runtime language and plugin APIs.
 *
 * Storage plugins may support file storage, data storage or both. File storage provides classic
 * file operations, while data storage provides a SQL-like interface to insert, select and query
 * tabular data. Any storage plugin intended for production use must provide both interfaces, using
 * the data storage interface to push queries down to the underlying data stack. TRAC will select
 * either between the file and data interfaces for each operation depending on a number of factors,
 * such as dataset size and shape and the type of query, plugins should make no assumptions about
 * the code path that will be chosen.
 *
 * A common implementation of data storage is included with the data library, that implements the
 * data storage API on top of file storage using an internal engine. This provides the capabilities
 * of data storage in a local or sandbox setup. The common data storage implementation is not
 * sufficient for production use in most cases, plugins should provide a native implementation of
 * data storage.
 *
 * To implement a storage plugin, create a JAR containing a class that implements the IStoragePlugin
 * interface.
 *
 * Service providers are held up on Google's implementation of modules for gRPC.
 * Classic class path service providers work fine though.
 *
 * https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/ServiceLoader.html
 * https://github.com/grpc/grpc-java/issues/3522
 */
public interface IStoragePlugin {

    String name();
    List<String> protocols();

    IFileStorage createFileStorage(String storageKey, String protocol, Properties config);
}
