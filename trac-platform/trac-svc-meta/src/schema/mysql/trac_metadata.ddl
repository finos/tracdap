
create table tenant (

    tenant_id smallint not null,
    tenant_code varchar(16) not null,

    constraint pk_tenant primary key (tenant_id)
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


create table object_definition (

    tenant_id smallint not null,
    definition_pk bigint not null auto_increment,

    object_fk bigint not null,
    object_version int not null,
    definition blob not null,

    constraint pk_definition primary key (definition_pk),
    constraint fk_definition_object foreign key (object_fk) references object_id (object_pk),
    constraint fk_definition_tenant foreign key (tenant_id) references tenant (tenant_id)
);


create table tag (

    tenant_id smallint not null,
    tag_pk bigint not null auto_increment,

    definition_fk bigint not null,
    tag_version int not null,

    constraint pk_tag primary key (tag_pk),
    constraint fk_tag_definition foreign key (definition_fk) references object_definition (definition_pk),
    constraint fk_tag_tenant foreign key (tenant_id) references tenant (tenant_id)
);


create table tag_attr (

    tenant_id smallint not null,

    tag_fk bigint not null,

    attr_name varchar(255) not null,
    attr_value_bool boolean null,
    attr_value_integer bigint null,
    attr_value_float double null,
    attr_value_decimal decimal null,
    attr_value_string varchar(4096) null,
    attr_value_date date null,
    attr_value_datetime timestamp null,
    attr_value_datetime_zone varchar(16) null,

    constraint fk_attr_tag foreign key (tag_fk) references tag (tag_pk),
    constraint fk_attr_tenant foreign key (tenant_id) references tenant (tenant_id)
);
