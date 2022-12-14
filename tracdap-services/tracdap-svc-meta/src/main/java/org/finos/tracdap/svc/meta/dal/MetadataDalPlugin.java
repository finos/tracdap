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

package org.finos.tracdap.svc.meta.dal;

import org.finos.tracdap.common.db.JdbcSetup;
import org.finos.tracdap.common.exception.EUnexpected;
import org.finos.tracdap.common.plugin.PluginServiceInfo;
import org.finos.tracdap.common.plugin.TracPlugin;
import org.finos.tracdap.svc.meta.dal.jdbc.JdbcMetadataDal;

import java.util.List;
import java.util.Properties;


public class MetadataDalPlugin extends TracPlugin {

    private static final String PLUGIN_NAME = "CORE_METADATA";

    private static final String JDBC_METADATA_DAL = "JDBC_METADATA_DAL";

    private static final List<PluginServiceInfo> serviceInfo = List.of(
            new PluginServiceInfo(IMetadataDal.class, JDBC_METADATA_DAL, List.of("JDBC", "SQL")));

    @Override
    public String pluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public List<PluginServiceInfo> serviceInfo() {
        return serviceInfo;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T createService(String serviceName, Properties properties) {

        if (JDBC_METADATA_DAL.equals(serviceName)) {

            var dialect = JdbcSetup.getSqlDialect(properties);
            var datasource = JdbcSetup.createDatasource(properties);

            return (T) new JdbcMetadataDal(dialect, datasource);
        }

        // Should never happen, protected by PluginManager
        throw new EUnexpected();
    }
}
