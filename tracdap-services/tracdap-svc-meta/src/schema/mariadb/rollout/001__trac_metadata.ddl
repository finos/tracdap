--  Copyright 2022 Accenture Global Solutions Limited
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


create table tenant (

    tenant_id smallint not null,
    tenant_code varchar(16) not null,

    -- MariaDB does not allow primary key constraints to be named
    -- Supplying a name creates warnings in the deploy logs
    constraint primary key (tenant_id),
    constraint unq_tenant unique (tenant_code)
);


create table object_id (

    tenant_id smallint not null,
    object_pk bigint not null auto_increment,

    object_type varchar(16) not null,
    object_id_hi bigint not null,
    object_id_lo bigint not null,

    -- MariaDB does not allow primary key constraints to be named
    -- Supplying a name creates warnings in the deploy logs
    constraint primary key (object_pk),
    constraint fk_object_tenant foreign key (tenant_id) references tenant (tenant_id)
);

create unique index idx_object_unq on object_id (tenant_id, object_id_hi, object_id_lo);


create table object_definition (

    tenant_id smallint not null,
    definition_pk bigint not null auto_increment,

    -- MariaDB has some very strange default behaviour for timestamp fields!
    -- Unless a timestamp is explicitly declared as null-able, it is populated
    -- with the current time by default if no value is specified on insert.
    -- Even more strange, if no value is specified on update the value is updated
    -- to the current time, even if the field doesn't appear in the update statement
    -- at all! This behaviour can be disabled by either (1) explicitly declaring the
    -- field as null-able or (2) setting a default value without an "on update" clause.

    -- https://mariadb.com/kb/en/timestamp/

    object_fk bigint not null,
    object_version int not null,
    object_timestamp timestamp (6) not null default 0,
    object_superseded timestamp (6) null,
    object_is_latest boolean not null,

    definition blob not null,

    -- MariaDB does not allow primary key constraints to be named
    -- Supplying a name creates warnings in the deploy logs
    constraint primary key (definition_pk),
    constraint fk_definition_object foreign key (object_fk) references object_id (object_pk),
    constraint fk_definition_tenant foreign key (tenant_id) references tenant (tenant_id)
);

create unique index idx_definition_unq on object_definition (tenant_id, object_fk, object_version);


create table tag (

    tenant_id smallint not null,
    tag_pk bigint not null auto_increment,

    -- MariaDB has some very strange default behaviour for timestamp fields!
    -- Unless a timestamp is explicitly declared as null-able, it is populated
    -- with the current time by default if no value is specified on insert.
    -- Even more strange, if no value is specified on update the value is updated
    -- to the current time, even if the field doesn't appear in the update statement
    -- at all! This behaviour can be disabled by either (1) explicitly declaring the
    -- field as null-able or (2) setting a default value without an "on update" clause.

    -- https://mariadb.com/kb/en/timestamp/

    definition_fk bigint not null,
    tag_version int not null,
    tag_timestamp timestamp (6) not null default 0,
    tag_superseded timestamp (6) null,
    tag_is_latest boolean not null,

    -- Duplicate fields from object ID/definition tables so they are available for searching
    object_type varchar(16) not null,

    -- MariaDB does not allow primary key constraints to be named
    -- Supplying a name creates warnings in the deploy logs
    constraint primary key (tag_pk),
    constraint fk_tag_definition foreign key (definition_fk) references object_definition (definition_pk),
    constraint fk_tag_tenant foreign key (tenant_id) references tenant (tenant_id)
);

create unique index idx_tag_unq on tag (tenant_id, definition_fk, tag_version);


create table tag_attr (

    tenant_id smallint not null,
    tag_fk bigint not null,

    attr_name varchar(256) not null,
    attr_type varchar(16) not null,
    attr_index int not null,

    attr_value_boolean boolean null,
    attr_value_integer bigint null,
    attr_value_float double null,
    attr_value_string varchar(4096) null,
    attr_value_decimal decimal (31, 10) null,
    attr_value_date date null,
    attr_value_datetime timestamp (6) null,

    constraint fk_attr_tag foreign key (tag_fk) references tag (tag_pk),
    constraint fk_attr_tenant foreign key (tenant_id) references tenant (tenant_id)
);

create unique index idx_attr_unq on tag_attr (tenant_id, tag_fk, attr_name, attr_index);
