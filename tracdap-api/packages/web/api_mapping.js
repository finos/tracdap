/*
 * Copyright 2022 Accenture Global Solutions Limited
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
    "followJob": "grpc.MethodType.SERVER_STREAMING",
};


const enumMapping = {
    "BasicType": "tracdap.metadata.BasicType",
    "ObjectType": "tracdap.metadata.ObjectType",
    "SchemaType": "tracdap.metadata.SchemaType",
    "PartType": "tracdap.metadata.PartType",
    "FlowNodeType": "tracdap.metadata.FlowNodeType",
    "JobType": "tracdap.metadata.JobType",
    "CopyStatus": "tracdap.metadata.CopyStatus",
    "IncarnationStatus": "tracdap.metadata.IncarnationStatus",
    "SearchOperator": "tracdap.metadata.SearchOperator",
    "LogicalOperator": "tracdap.metadata.LogicalOperator",
    "TagOperation": "tracdap.metadata.TagOperation",
    "JobStatusCode": "tracdap.metadata.JobStatusCode"
}

const basicTypeMapping = {
    "BOOLEAN": "tracdap.metadata.BasicType.BOOLEAN",
    "INTEGER": "tracdap.metadata.BasicType.INTEGER",
    "FLOAT": "tracdap.metadata.BasicType.FLOAT",
    "DECIMAL": "tracdap.metadata.BasicType.DECIMAL",
    "STRING": "tracdap.metadata.BasicType.STRING",
    "DATE": "tracdap.metadata.BasicType.DATE",
    "DATETIME": "tracdap.metadata.BasicType.DATETIME"
}


module.exports.methodTypes = methodTypes;
module.exports.enumMapping = enumMapping;
module.exports.basicTypeMapping = basicTypeMapping;
