/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.meta.dal.jdbc.dialects;

import com.accenture.trac.common.db.JdbcDialect;


public class MariaDbDialect extends MySqlDialect {

    // MariaDB dialect is based on the MySQL dialect.

    // MariaDB has a few differences that necessitate having separate DDL files.
    // This includes a separate DDL for the key mapping table. Error codes are
    // the same as MySQL so those can be reused.

    // It is also important to use the correct connectors for MySQL / MariaDB as
    // there are differences in the binary protocol. E.g. sub-second precision in
    // timestamps is implemented differently, using the wrong driver will truncate
    // fractional parts of a second.

    private static final String CREATE_KEY_MAPPING_FILE = "jdbc/mariadb/key_mapping.ddl";

    MariaDbDialect() {
        super(CREATE_KEY_MAPPING_FILE);
    }

    @Override
    public JdbcDialect dialectCode() {
        return JdbcDialect.MARIADB;
    }
}
