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


create table cache_entry (

    entry_pk bigint not null auto_increment,

    cache_name varchar(256) not null,
    entry varchar(256) not null,
    revision int not null,
    status varchar(256) null,

    encoded_value blob null,

    constraint entry_pk primary key (entry_pk)
);

create unique index cache_entry_unq_idx on cache_entry (cache_name, entry);


create table cache_ticket (

    ticket_pk bigint not null auto_increment,

    cache_name varchar(256) not null,
    entry varchar(256) not null,
    revision int not null,

    entry_fk bigint null,

    grant_time timestamp (6) not null default 0,
    expiry_time timestamp (6) not null default 0,

    constraint ticket_pk primary key (ticket_pk),
    constraint ticket_fk_entry foreign key (entry_fk) references cache_entry (entry_pk) on delete set null
);

create unique index cache_ticket_unq_idx on cache_ticket (cache_name, entry, revision);
