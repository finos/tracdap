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
/

create table object_id (

    tenant_id smallint not null,
    object_pk number(19) not null,

    object_type varchar(16) not null,
    object_id_hi number(19) not null,
    object_id_lo number(19) not null,

    constraint pk_object primary key (object_pk),
    constraint fk_object_tenant foreign key (tenant_id) references tenant (tenant_id)
);

create unique index idx_object_unq on object_id (tenant_id, object_id_hi, object_id_lo);

create sequence object_id_sequence;

create trigger object_id_insert
    before insert on object_id
    for each row
begin
    select object_id_sequence.nextval
    into :new.object_pk
    from dual;
end;
/


create table object_definition (

    tenant_id smallint not null,
    definition_pk number(19) not null,

    object_fk number(19) not null,
    object_version int not null,
    object_timestamp timestamp(6) not null,
    object_superseded timestamp(6) null,
    object_is_latest number(1) not null,

    definition blob not null,

    constraint pk_definition primary key (definition_pk),
    constraint fk_definition_object foreign key (object_fk) references object_id (object_pk),
    constraint fk_definition_tenant foreign key (tenant_id) references tenant (tenant_id)
);

create unique index idx_definition_unq on object_definition (tenant_id, object_fk, object_version);

create sequence object_definition_sequence;

create trigger object_definition_insert
    before insert on object_definition
    for each row
begin
    select object_definition_sequence.nextval
    into :new.definition_pk
    from dual;
end;
/


create table tag (

    tenant_id smallint not null,
    tag_pk number(19) not null,

    definition_fk number(19) not null,
    tag_version int not null,
    tag_timestamp timestamp(6) not null,
    tag_superseded timestamp(6) null,
    tag_is_latest number(1) not null,

    -- Duplicate fields from object ID/definition tables so they are available for searching
    object_type varchar(16) not null,

    constraint pk_tag primary key (tag_pk),
    constraint fk_tag_definition foreign key (definition_fk) references object_definition (definition_pk),
    constraint fk_tag_tenant foreign key (tenant_id) references tenant (tenant_id)
);

create unique index idx_tag_unq on tag (tenant_id, definition_fk, tag_version);

create sequence tag_sequence;

create trigger tag_insert
    before insert on tag
    for each row
begin
    select tag_sequence.nextval
    into :new.tag_pk
    from dual;
end;
/


create table tag_attr (

    tenant_id smallint not null,
    tag_fk number(19) not null,

    attr_name varchar(256) not null,
    attr_type varchar(16) not null,
    attr_index int not null,

    attr_value_boolean number(1) null,
    attr_value_integer number(19) null,
    attr_value_float double precision null,
    attr_value_string varchar2(4000) null,
    attr_value_decimal decimal (31, 10) null,
    attr_value_date date null,
    attr_value_datetime timestamp (6) null,

    constraint fk_attr_tag foreign key (tag_fk) references tag (tag_pk),
    constraint fk_attr_tenant foreign key (tenant_id) references tenant (tenant_id)
);

create unique index idx_attr_unq on tag_attr (tenant_id, tag_fk, attr_name, attr_index);
/


-- For Oracle, create the key mapping table at deploy time as a global temp table
create global temporary table key_mapping (

    pk number(19),

    id_hi number(19),
    id_lo number(19),

    fk number(19),
    ver int,
    as_of timestamp (6),
    is_latest number(1),

    mapping_stage int,
    ordering int
)
on commit delete rows;
/
