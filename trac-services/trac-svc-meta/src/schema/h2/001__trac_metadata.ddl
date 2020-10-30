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


create table tenant (

    tenant_id smallint not null,
    tenant_code varchar(16) not null,

    constraint pk_tenant primary key (tenant_id),
    constraint unq_tenant unique (tenant_code)
);


create table object_id (

    tenant_id smallint not null,
    object_pk bigint not null auto_increment,

    object_type varchar(16) not null,
    object_id_hi bigint not null,
    object_id_lo bigint not null,

    constraint pk_object primary key (object_pk),
    constraint fk_object_tenant foreign key (tenant_id) references tenant (tenant_id)
);

create unique index idx_object_unq on object_id (tenant_id, object_id_hi, object_id_lo);


create table object_definition (

    tenant_id smallint not null,
    definition_pk bigint not null auto_increment,

    object_fk bigint not null,
    object_version int not null,
    object_timestamp timestamp (6) not null,

    object_superseded timestamp (6) null,
    object_is_latest boolean not null,

    definition blob not null,

    constraint pk_definition primary key (definition_pk),
    constraint fk_definition_object foreign key (object_fk) references object_id (object_pk),
    constraint fk_definition_tenant foreign key (tenant_id) references tenant (tenant_id)
);

create unique index idx_definition_unq on object_definition (tenant_id, object_fk, object_version);


create table tag (

    tenant_id smallint not null,
    tag_pk bigint not null auto_increment,

    definition_fk bigint not null,
    tag_version int not null,
    tag_timestamp timestamp (6) not null,

    tag_superseded timestamp (6) null,
    tag_is_latest boolean not null,

    -- Make object type available for searching without a join
    object_type varchar(16) not null,

    constraint pk_tag primary key (tag_pk),
    constraint fk_tag_definition foreign key (definition_fk) references object_definition (definition_pk),
    constraint fk_tag_tenant foreign key (tenant_id) references tenant (tenant_id)
);

create unique index idx_tag_unq on tag (tenant_id, definition_fk, tag_version);


create table tag_attr (

    tenant_id smallint not null,

    tag_fk bigint not null,
    attr_name varchar(256) not null,
    attr_index int not null,

    attr_type varchar(16) not null,
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


create table latest_version (

    tenant_id smallint not null,
    object_fk bigint not null,
    latest_definition_pk bigint not null,

    constraint pk_latest_ver primary key (object_fk),
    constraint fk_latest_ver_object foreign key (object_fk) references object_id (object_pk),
    constraint fk_latest_ver_definition foreign key (latest_definition_pk) references object_definition (definition_pk),
    constraint fk_latest_ver_tenant foreign key (tenant_id) references tenant (tenant_id)
);


create table latest_tag (

    tenant_id smallint not null,
    definition_fk bigint not null,
    latest_tag_pk bigint not null,

    constraint pk_latest_tag primary key (definition_fk),
    constraint fk_latest_tag_definition foreign key (definition_fk) references object_definition (definition_pk),
    constraint fk_latest_tag_tag foreign key (latest_tag_pk) references tag (tag_pk),
    constraint fk_latest_tag_tenant foreign key (tenant_id) references tenant (tenant_id)
);
