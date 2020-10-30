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


create temporary table key_mapping (

    -- Primary key, this is the target key being looked up
    -- It will be used for loading records
    pk bigint,

    -- ID fields used to look up object PKs by object ID
    id_hi bigint,
    id_lo bigint,

    -- FK, version, as-of and latest for mapping definitions and tags
    -- This is the FK of the parent object, plus criteria from a selector
    fk bigint,
    ver int,
    as_of timestamp (6),
    is_latest boolean,

    -- Mapping stage separates mapping operations within a transaction
    mapping_stage int,

    -- Ordering to ensure all stages of a load read object parts in the same order
    ordering int
);
