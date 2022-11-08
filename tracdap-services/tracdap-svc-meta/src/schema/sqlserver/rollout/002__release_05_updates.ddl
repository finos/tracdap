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


-- Adding tenant description to the tenant table
-- This is needed as part of the listTenants() API
alter table tenant add description varchar(4096) null;


-- Record metadata format and version in the object definition table
alter table object_definition add meta_format int null;
alter table object_definition add meta_version int null;
go;  -- Apply alterations before attempting the update
update object_definition set meta_format = 1 where true;
update object_definition set meta_version = 1 where true;
alter table object_definition alter column meta_format int not null;
alter table object_definition alter column meta_version int not null;
