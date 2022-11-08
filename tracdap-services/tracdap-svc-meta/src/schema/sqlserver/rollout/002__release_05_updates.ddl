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
-- SQL Server does not apply table alterations without the 'go' command, which breaks in the deploy tool
-- Workaround for now is to set a column default (a value is always inserted by the metadata service)
alter table object_definition add meta_format int not null default(1);
alter table object_definition add meta_version int not null default(1);
