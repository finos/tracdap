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

package org.finos.tracdap.tools.secrets;


import org.finos.tracdap.common.config.ConfigKeys;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.db.JdbcSetup;
import org.finos.tracdap.common.exception.EAuthorization;
import org.finos.tracdap.config.PlatformConfig;

import javax.sql.DataSource;
import java.net.URI;
import java.sql.SQLException;
import java.util.Properties;

public class SqlUserManager implements IUserManager {

    private final ConfigManager configManager;

    public SqlUserManager(ConfigManager configManager) {
        this.configManager = configManager;
        this.configManager.prepareSecrets();
    }

    private DataSource createDatasource() {

        var config = configManager.loadRootConfigObject(PlatformConfig.class);
        var dialect = config.getConfigOrThrow(ConfigKeys.USER_DB_TYPE);
        var usersUrl = config.getConfigOrThrow(ConfigKeys.USER_DB_URL);

        var jdbcPath = configManager.resolveConfigFile((URI.create(usersUrl))).getPath();
        var jdbcUrl = jdbcPath + ";AUTO_SERVER=TRUE";

        var properties = new Properties();
        properties.setProperty("dialect", dialect);
        properties.setProperty("jdbcUrl", jdbcUrl);
        properties.setProperty("h2.user", "trac");
        properties.setProperty("h2.pass", "trac");
        properties.setProperty("h2.schema", "public");
        properties.setProperty("pool.size", "10");
        properties.setProperty("pool.overflow", "5");

        // var dialect = JdbcSetup.getSqlDialect(properties);
        return JdbcSetup.createDatasource(properties);
    }

    @Override
    public void initTracUsers() {

        var dataSource = createDatasource();

        try(var conn = dataSource.getConnection()) {

            var query = "create table if not exists users (" +
                    "user_id varchar(255) primary key, " +
                    "user_name varchar(max), " +
                    "password_hash varchar(max) not null)";

            var stmt = conn.prepareStatement(query);
            stmt.execute();
        }
        catch (SQLException e) {

            var message = "Error accessing user DB: " + e.getMessage();
            throw new EAuthorization(message);
        }
    }

    @Override
    public void addUser(String userId, String userName, String passwordHash) {

        var dataSource = createDatasource();

        try(var conn = dataSource.getConnection()) {

            var query = "insert into users(user_id, user_name, password_hash) values (?, ?, ?)";

            var stmt = conn.prepareStatement(query);
            stmt.setString(1, userId);
            stmt.setString(2, userName);
            stmt.setString(3, passwordHash);

            stmt.execute();
        }
        catch (SQLException e) {

            var message = "Error accessing user DB: " + e.getMessage();
            throw new EAuthorization(message);
        }
    }

    @Override
    public void deleteUser(String userId) {

        var dataSource = createDatasource();

        try(var conn = dataSource.getConnection()) {

            var query = "delete from users where user_id = ?";

            var stmt = conn.prepareStatement(query);
            stmt.setString(1, userId);

            stmt.execute();
        }
        catch (SQLException e) {

            var message = "Error accessing user DB: " + e.getMessage();
            throw new EAuthorization(message);
        }
    }
}
