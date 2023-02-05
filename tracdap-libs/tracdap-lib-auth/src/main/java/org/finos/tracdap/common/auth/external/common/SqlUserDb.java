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

package org.finos.tracdap.common.auth.external.common;

import org.finos.tracdap.common.auth.external.IUserDatabase;
import org.finos.tracdap.common.auth.external.UserDbRecord;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.db.JdbcSetup;
import org.finos.tracdap.common.exception.EAuthorization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.net.URI;
import java.sql.SQLException;
import java.util.Properties;


public class SqlUserDb implements IUserDatabase {

    public static SqlUserDb getUserDb(ConfigManager configManager, String dialect, String usersUrl) {

        var jdbcUrl = configManager.configRoot().relativize(URI.create(usersUrl)).getPath();

        var properties = new Properties();
        properties.setProperty("dialect", dialect);
        properties.setProperty("jdbcUrl", jdbcUrl);
        properties.setProperty("h2.user", "trac");
        properties.setProperty("h2.pass", "trac");
        properties.setProperty("h2.schema", "public");
        properties.setProperty("pool.size", "10");
        properties.setProperty("pool.overflow", "5");

        // var dialect = JdbcSetup.getSqlDialect(properties);
        var datasource = JdbcSetup.createDatasource(properties);

        return new SqlUserDb(datasource);
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final DataSource dataSource;

    public SqlUserDb(DataSource dataSource) {

        this.dataSource = dataSource;
    }

    @Override
    public UserDbRecord getUserDbRecord(String userId) {

        try(var conn = dataSource.getConnection()) {

            var query = "select user_id, user_name, password_hash from users where user_id = ?";
            var stmt = conn.prepareStatement(query);
            stmt.setString(1, userId);

            var result = stmt.executeQuery();

            if (!result.next())
                return null;

            if (!result.isLast()) {
                var message = "Corruption in user DB (duplicate user record)";
                log.error(message);
                throw new EAuthorization(message);
            }

            var userName = result.getString(2);
            var passwordHash = result.getString(3);

            return new UserDbRecord(userId, userName, passwordHash);
        }
        catch (SQLException e) {

            var message = "Error accessing user DB: " + e.getMessage();
            log.error(message);
            throw new EAuthorization(message);
        }
    }
}
