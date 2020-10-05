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

    // Re-use the MySQL dialect since error codes, syntax is all the same
    // Just return the MARIADB dialect code!

    // It is important the dialects are treated separately however in terms of drivers
    // MariaDB and MySQL have different implementations for sub-section precision in timestamps
    // Neither DB can read the sub-second part from the other's protocol

    @Override
    public JdbcDialect dialectCode() {
        return JdbcDialect.MARIADB;
    }
}
