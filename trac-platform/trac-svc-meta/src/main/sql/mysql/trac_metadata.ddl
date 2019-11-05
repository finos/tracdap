
create table tenant (

    tenant_pk smallint not null,
    tenant_code varchar(16) not null,

    constraint pk_tenant primary key (tenant_pk)
);

create unique index idx_tenant_code on tenant (tenant_code);
