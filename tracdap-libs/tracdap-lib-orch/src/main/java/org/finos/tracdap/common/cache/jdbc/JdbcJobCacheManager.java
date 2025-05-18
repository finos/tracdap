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

package org.finos.tracdap.common.cache.jdbc;

import org.finos.tracdap.common.cache.IJobCache;
import org.finos.tracdap.common.cache.IJobCacheManager;
import org.finos.tracdap.common.db.JdbcDialect;

import javax.sql.DataSource;
import java.io.Serializable;


public class JdbcJobCacheManager implements IJobCacheManager {

    private final DataSource dataSource;
    private final JdbcDialect dialect;

    public JdbcJobCacheManager(DataSource dataSource, JdbcDialect dialect) {
        this.dataSource = dataSource;
        this.dialect = dialect;
    }

    @Override
    public <TValue extends Serializable> IJobCache<TValue>
    getCache(String cacheName, Class<TValue> cacheType) {

        return new JdbcJobCache<>(dataSource, dialect, cacheName);
    }
}
