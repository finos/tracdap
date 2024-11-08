#  Licensed to the Fintech Open Source Foundation (FINOS) under one or
#  more contributor license agreements. See the NOTICE file distributed
#  with this work for additional information regarding copyright ownership.
#  FINOS licenses this file to you under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with the
#  License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import typing as tp
import http
import io

import tracdap.rt.config as cfg
import tracdap.rt.exceptions as ex

# Import storage interfaces
import tracdap.rt.ext.plugins as plugins
from tracdap.rt.ext.storage import *

from pyarrow import fs as afs

# Set of common helpers across the core plugins (do not reference rt._impl)
from . import _helpers


try:
    # AWS SDK
    import boto3
    import botocore.response
    import botocore.exceptions as aws_ex
    __aws_available = True
except ImportError:
    boto3 = None
    botocore = None
    aws_ex = None
    __aws_available = False


class AwsStorageProvider(IStorageProvider):

    BUCKET_PROPERTY = "bucket"
    PREFIX_PROPERTY = "prefix"
    REGION_PROPERTY = "region"
    ENDPOINT_PROPERTY = "endpoint"

    CREDENTIALS_PROPERTY = "credentials"
    CREDENTIALS_DEFAULT = "default"
    CREDENTIALS_STATIC = "static"

    ACCESS_KEY_ID_PROPERTY = "accessKeyId"
    SECRET_ACCESS_KEY_PROPERTY = "secretAccessKey"

    RUNTIME_FS_PROPERTY = "runtimeFs"
    RUNTIME_FS_AUTO = "auto"
    RUNTIME_FS_ARROW = "arrow"
    RUNTIME_FS_BOTO3 = "boto3"
    RUNTIME_FS_DEFAULT = RUNTIME_FS_AUTO

    ARROW_CLIENT_ARGS = {
        REGION_PROPERTY: "region",
        ENDPOINT_PROPERTY: "endpoint_override",
        ACCESS_KEY_ID_PROPERTY: "access_key",
        SECRET_ACCESS_KEY_PROPERTY: "secret_key"
    }

    BOTO_CLIENT_ARGS = {
        REGION_PROPERTY: "region_name",
        ENDPOINT_PROPERTY: "endpoint_url",
        ACCESS_KEY_ID_PROPERTY: "aws_access_key_id",
        SECRET_ACCESS_KEY_PROPERTY: "aws_secret_access_key"
    }

    def __init__(self, properties: tp.Dict[str, str]):

        self._log = _helpers.logger_for_object(self)
        self._properties = properties

        self._runtime_fs = _helpers.get_plugin_property(
            properties, self.RUNTIME_FS_PROPERTY) \
            or self.RUNTIME_FS_DEFAULT

    def has_arrow_native(self) -> bool:
        if self._runtime_fs == self.RUNTIME_FS_ARROW:
            return True
        elif self._runtime_fs == self.RUNTIME_FS_AUTO:
            return afs.S3FileSystem is not None
        else:
            return False

    def has_file_storage(self) -> bool:
        if self._runtime_fs == self.RUNTIME_FS_BOTO3:
            return True
        elif self._runtime_fs == self.RUNTIME_FS_AUTO:
            return afs.S3FileSystem is None
        else:
            return False

    def get_arrow_native(self) -> afs.SubTreeFileSystem:

        s3fs_args = self.setup_client_args(self.ARROW_CLIENT_ARGS)
        s3fs = afs.S3FileSystem(**s3fs_args)

        bucket = _helpers.get_plugin_property(self._properties, self.BUCKET_PROPERTY)
        prefix = _helpers.get_plugin_property(self._properties, self.PREFIX_PROPERTY)

        if bucket is None or len(bucket.strip()) == 0:
            message = f"Missing required config property [{self.BUCKET_PROPERTY}] for S3 storage"
            self._log.error(message)
            raise ex.EConfigParse(message)

        root_path = f"{bucket}/{prefix}" if prefix else bucket

        return afs.SubTreeFileSystem(root_path, s3fs)

    def get_file_storage(self) -> IFileStorage:

        client_args = self.setup_client_args(self.BOTO_CLIENT_ARGS)
        client_args["service_name"] = "s3"

        config = cfg.PluginConfig()
        config.protocol = "S3"
        config.properties = self._properties

        return S3ObjectStorage(config, client_args)

    def setup_client_args(self, key_mapping: tp.Dict[str, str]) -> tp.Dict[str, tp.Any]:

        client_args = dict()

        region = _helpers.get_plugin_property(self._properties, self.REGION_PROPERTY)
        endpoint = _helpers.get_plugin_property(self._properties, self.ENDPOINT_PROPERTY)

        if region is not None:
            region_key = key_mapping[self.REGION_PROPERTY]
            client_args[region_key] = region

        if endpoint is not None:
            endpoint_key = key_mapping[self.ENDPOINT_PROPERTY]
            client_args[endpoint_key] = endpoint

        credentials = self.setup_credentials(key_mapping)
        client_args.update(credentials)

        return client_args

    def setup_credentials(self, key_mapping: tp.Dict[str, str]):

        mechanism = _helpers.get_plugin_property(self._properties, self.CREDENTIALS_PROPERTY)

        if mechanism is None or len(mechanism) == 0 or mechanism.lower() == self.CREDENTIALS_DEFAULT:
            self._log.info(f"Using [{self.CREDENTIALS_DEFAULT}] credentials mechanism")
            return dict()

        if mechanism.lower() == self.CREDENTIALS_STATIC:

            access_key_id = _helpers.get_plugin_property(self._properties, self.ACCESS_KEY_ID_PROPERTY)
            secret_access_key = _helpers.get_plugin_property(self._properties, self.SECRET_ACCESS_KEY_PROPERTY)

            self._log.info(
                f"Using [{self.CREDENTIALS_STATIC}] credentials mechanism, " +
                f"access key id = [{access_key_id}]")

            access_key_id_arg = key_mapping[self.ACCESS_KEY_ID_PROPERTY]
            secret_access_key_arg = key_mapping[self.SECRET_ACCESS_KEY_PROPERTY]

            return {
                access_key_id_arg: access_key_id,
                secret_access_key_arg: secret_access_key}

        message = f"Unrecognised credentials mechanism: [{mechanism}]"
        self._log.error(message)
        raise ex.EStartup(message)


if __aws_available:
    plugins.PluginManager.register_plugin(IStorageProvider, AwsStorageProvider, ["S3"])


# ----------------------------------------------------------------------------------------------------------------------
# CUSTOM IMPLEMENTATION FOR S3 STORAGE
# ----------------------------------------------------------------------------------------------------------------------

# This is the old implementation that was used before Arrow native was made available
# It is likely to be removed in a future release


class S3ObjectStorage(IFileStorage):

    # This is a quick implementation of IFileStorage on S3 using the boto3 AWS SDK

    def __init__(self, config: cfg.PluginConfig, client_args: dict):

        self._log = _helpers.logger_for_object(self)

        self._properties = config.properties
        self._bucket = _helpers.get_plugin_property(self._properties, AwsStorageProvider.BUCKET_PROPERTY)
        self._prefix = _helpers.get_plugin_property(self._properties, AwsStorageProvider.PREFIX_PROPERTY) or ""

        if self._bucket is None or len(self._bucket.strip()) == 0:
            message = f"Missing required config property [{AwsStorageProvider.BUCKET_PROPERTY}] for S3 storage"
            self._log.error(message)
            raise ex.EConfigParse(message)

        self._client = boto3.client(**client_args)

    def exists(self, storage_path: str) -> bool:

        try:
            self._log.info(f"EXISTS [{storage_path}]")

            object_key = self._resolve_path(storage_path)
            self._client.head_object(Bucket=self._bucket, Key=object_key)
            return True

        except aws_ex.ClientError as error:
            aws_code = error.response['Error']['Code']
            if aws_code == str(http.HTTPStatus.NOT_FOUND.value):  # noqa
                return False
            raise ex.EStorageRequest(f"Storage error: {str(error)}") from error

    def size(self, storage_path: str) -> int:

        try:
            self._log.info(f"SIZE [{storage_path}]")

            object_key = self._resolve_path(storage_path)
            response = self._client.head_object(Bucket=self._bucket, Key=object_key)
            return response['ContentLength']

        except aws_ex.ClientError as error:
            raise ex.EStorageRequest(f"Storage error: {str(error)}") from error

    def stat(self, storage_path: str) -> FileStat:

        self._log.info(f"STAT [{storage_path}]")

        name = storage_path.split("/")[-1]

        if self.exists(storage_path):

            # Only OBJECTS can support stat atm
            # Handling for directories needs to be changed, as part of refactor onto object storage
            size = self.size(storage_path)
            return FileStat(name, FileType.FILE, storage_path, size)

        else:

            self.ls(storage_path)
            return FileStat(name, FileType.DIRECTORY, storage_path, 0)

    def ls(self, storage_path: str, recursive: bool = False) -> tp.List[FileStat]:

        self._log.info(f"LS [{storage_path}]")

        prefix = self._resolve_path(storage_path) + "/"

        response = self._client.list_objects_v2(
            Bucket=self._bucket,
            Prefix=prefix,
            Delimiter="/")

        keys = []

        if "Contents" not in response and "CommonPrefixes" not in response:
            raise ex.EStorageRequest(f"Storage prefix not found: [{storage_path}]")

        if "Contents" in response:
            for entry in response["Contents"]:
                raw_key = entry["Key"]
                if raw_key == prefix:
                    continue
                key = raw_key.replace(prefix, "")
                size = entry["Size"]
                mtime = entry["LastModified "]
                stat = FileStat(key, FileType.FILE, raw_key, size, mtime=mtime)
                keys.append(stat)

        if "CommonPrefixes" in response:
            for raw_prefix in response["CommonPrefixes"]:
                common_prefix = raw_prefix.replace(prefix, "")
                stat = FileStat(common_prefix, FileType.DIRECTORY, raw_prefix, 0)
                keys.append(stat)

        return keys

    def mkdir(self, storage_path: str, recursive: bool = False):

        self._log.info(f"MKDIR [{storage_path}]")

        # No-op in object storage
        pass

    def rm(self, storage_path: str):

        try:
            self._log.info(f"RM [{storage_path}]")

            object_key = self._resolve_path(storage_path)
            self._client.delete_object(Bucket=self._bucket, Key=object_key)

        except aws_ex.ClientError as error:
            raise ex.EStorageRequest(f"Storage error: {str(error)}") from error

    def rmdir(self, storage_path: str):

        raise RuntimeError("RMDIR (recursive) not available for S3 storage")

    def read_bytes(self, storage_path: str) -> bytes:

        self._log.info(f"READ BYTES [{storage_path}]")

        body = self._read_impl(storage_path)
        return body.read()

    def read_byte_stream(self, storage_path: str) -> tp.BinaryIO:

        self._log.info(f"READ BYTE STREAM [{storage_path}]")

        data = self.read_bytes(storage_path)
        return io.BytesIO(data)

    def _read_impl(self, storage_path: str):

        try:

            object_key = self._resolve_path(storage_path)
            response = self._client.get_object(Bucket=self._bucket, Key=object_key)
            return response['Body']

        except aws_ex.ClientError as error:
            raise ex.EStorageRequest(f"Storage error: {str(error)}") from error

    def write_bytes(self, storage_path: str, data: bytes):

        try:
            self._log.info(f"WRITE BYTES [{storage_path}]")

            object_key = self._resolve_path(storage_path)

            self._client.put_object(
                Bucket=self._bucket,
                Key=object_key,
                Body=data)

        except aws_ex.ClientError as error:
            raise ex.EStorageRequest(f"Storage error: {str(error)}") from error

    def write_byte_stream(self, storage_path: str) -> tp.BinaryIO:

        self._log.info(f"WRITE BYTE STREAM [{storage_path}]")

        return self._AwsWriteBuf(self, storage_path)

    class _AwsWriteBuf(io.BytesIO):

        def __init__(self, storage, storage_path):
            super().__init__()
            self._storage = storage
            self._storage_path = storage_path
            self._written = False

        def close(self):
            if not self._written:
                self.seek(0)
                data = self.read()
                self._storage.write_bytes(self._storage_path, data)
                self._written = True

    def _resolve_path(self, storage_path: str) -> str:

        if self._prefix is None or self._prefix.strip() == "":
            return storage_path

        separator = "" if self._prefix.endswith("/") else "/"
        full_path = self._prefix + separator + storage_path

        return full_path[1:] if full_path.startswith("/") else full_path
