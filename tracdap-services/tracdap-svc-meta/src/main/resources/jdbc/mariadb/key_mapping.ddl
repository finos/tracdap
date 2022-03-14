--  Copyright 2020 Accenture Global Solutions Limited
--
--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.


create temporary table key_mapping (

    pk bigint,

    id_hi bigint,
    id_lo bigint,

    -- MariaDB has some very strange default behaviour for timestamp fields!
    -- Unless a timestamp is explicitly declared as null-able, it is populated
    -- with the current time by default if no value is specified on insert.
    -- Even more strange, if no value is specified on update the value is updated
    -- to the current time, even if the field doesn't appear in the update statement
    -- at all! This behaviour can be disabled by either (1) explicitly declaring the
    -- field as null-able or (2) setting a default value without an "on update" clause.

    -- https://mariadb.com/kb/en/timestamp/

    fk bigint,
    ver int,
    as_of timestamp (6) null,
    is_latest boolean,

    mapping_stage int,
    ordering int
);
