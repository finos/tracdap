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


create table config_entry (

    tenant_id smallint not null,
    config_pk bigint not null auto_increment,

    config_class varchar(256) not null,
    config_key varchar (256) not null,

    config_version int not null,
    config_timestamp timestamp (6) not null,
    config_superseded timestamp (6) null,
    config_is_latest boolean not null,
    config_deleted boolean not null,

    meta_format int not null,
    meta_version int not null,
    details blob not null,

    constraint pk_config primary key (config_pk),
    constraint fk_config_tenant foreign key (tenant_id) references tenant (tenant_id)
);

create unique index idx_config_unq on config_entry (tenant_id, config_class, config_key, config_version);
