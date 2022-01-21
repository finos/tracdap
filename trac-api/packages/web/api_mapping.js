/*
 * Copyright 2021 Accenture Global Solutions Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


// These mappings could in theory be discovered by examining the proto descriptors
// Method types lists everything that isn't a unary call
// API mapping makes enums and basic types available in the root namespace for convenience

const methodTypes = {
    "createDataset": "grpc.MethodType.CLIENT_STREAMING",
    "updateDataset": "grpc.MethodType.CLIENT_STREAMING",
    "readDataset": "grpc.MethodType.SERVER_STREAMING",
    "createFilet": "grpc.MethodType.CLIENT_STREAMING",
    "updateFile": "grpc.MethodType.CLIENT_STREAMING",
    "readFile": "grpc.MethodType.SERVER_STREAMING",
};


const enumMapping = {
    "BasicType": "trac.metadata.BasicType",
    "ObjectType": "trac.metadata.ObjectType",
    "SchemaType": "trac.metadata.SchemaType",
    "PartType": "trac.metadata.PartType",
    "FlowNodeType": "trac.metadata.FlowNodeType",
    "JobType": "trac.metadata.JobType",
    "CopyStatus": "trac.metadata.CopyStatus",
    "IncarnationStatus": "trac.metadata.IncarnationStatus",
    "SearchOperator": "trac.metadata.SearchOperator",
    "LogicalOperator": "trac.metadata.LogicalOperator",
    "TagOperation": "trac.metadata.TagOperation"
}

const basicTypeMapping = {
    "BOOLEAN": "trac.metadata.BasicType.BOOLEAN",
    "INTEGER": "trac.metadata.BasicType.INTEGER",
    "FLOAT": "trac.metadata.BasicType.FLOAT",
    "DECIMAL": "trac.metadata.BasicType.DECIMAL",
    "STRING": "trac.metadata.BasicType.STRING",
    "DATE": "trac.metadata.BasicType.DATE",
    "DATETIME": "trac.metadata.BasicType.DATETIME"
}


module.exports.methodTypes = methodTypes;
module.exports.enumMapping = enumMapping;
module.exports.basicTypeMapping = basicTypeMapping;
