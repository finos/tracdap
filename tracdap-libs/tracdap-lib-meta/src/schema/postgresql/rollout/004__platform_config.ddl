--  Licensed to the Fintech Open Source Foundation (FINOS) under one or
--  more contributor license agreements. See the NOTICE file distributed
--  with this work for additional information regarding copyright ownership.
--  FINOS licenses this file to you under the Apache License, Version 2.0
--  (the "License"); you may not use this file except in compliance with the
--  License. You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.


create table platform_config (

    config_class varchar (256) not null,
    config_key varchar (256) not null,

    config_version int not null,
    config_timestamp timestamp (6) not null,
    config_deleted boolean not null,

    config_value bytea not null,

    constraint pk_platform_config primary key (config_class, config_key)
);
