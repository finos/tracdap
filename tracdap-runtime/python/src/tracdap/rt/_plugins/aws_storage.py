#  Copyright 2022 Accenture Global Solutions Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import http
import io

import botocore.response

from tracdap.rt.ext.storage import *

import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.util as _util

# AWS SDK
import boto3
import botocore.exceptions as _aws_ex  # noqa


class S3ObjectStorage(IFileStorage):

    # This is a quick implementation of IFileStorage on S3 using the boto3 AWS SDK

    # TODO: Migrate IFileStorage interface to use object storage as the primary concept
    # It is much easier to express objects on a file system than vice versa
    # This change must also be made in the Java code

    # TODO: Switch to using Apache Arrow file system interface
    # Arrow already has implementations for AWS, GCP, HDFS and local files
    # The arrow interface also allows extension with fsspec, to support Azure blob storage or custom implementations

    # https://arrow.apache.org/docs/python/filesystems.html

    REGION_PROPERTY = "region"
    BUCKET_PROPERTY = "bucket"
    PATH_PROPERTY = "path"

    ACCESS_KEY_ID_PROPERTY = "accessKeyId"
    SECRET_ACCESS_KEY_PROPERTY = "secretAccessKey"

    def __init__(self, config: _cfg.PluginConfig, options: dict = None):

        self._log = _util.logger_for_object(self)

        self._config = config
        self._options = options

        self._region = config.properties[self.REGION_PROPERTY]
        self._bucket = config.properties[self.BUCKET_PROPERTY]
        self._rootPath = config.properties[self.PATH_PROPERTY] if self.PATH_PROPERTY in config.properties else ""

        access_key_id = config.properties[self.ACCESS_KEY_ID_PROPERTY]
        secret_access_key = config.properties[self.SECRET_ACCESS_KEY_PROPERTY]

        self.__client = boto3.client(
            service_name="s3",
            region_name=self._region,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key)

    def exists(self, storage_path: str) -> bool:

        try:
            self._log.info(f"EXISTS [{storage_path}]")

            object_key = self._resolve_path(storage_path)
            self.__client.head_object(Bucket=self._bucket, Key=object_key)
            return True

        except _aws_ex.ClientError as error:
            _aws_code = error.response['Error']['Code']
            if _aws_code == str(http.HTTPStatus.NOT_FOUND.value):  # noqa
                return False
            raise _ex.EStorageRequest(f"Storage error: {str(error)}") from error

    def size(self, storage_path: str) -> int:

        try:
            self._log.info(f"SIZE [{storage_path}]")

            object_key = self._resolve_path(storage_path)
            response = self.__client.head_object(Bucket=self._bucket, Key=object_key)
            return response['ContentLength']

        except _aws_ex.ClientError as error:
            _aws_code = error.response['Error']['Code']
            raise _ex.EStorageRequest(f"Storage error: {str(error)}") from error

    def stat(self, storage_path: str) -> FileStat:

        self._log.info(f"STAT [{storage_path}]")

        if self.exists(storage_path):

            # Only OBJECTS can support stat atm
            # Handling for directories needs to be changed, as part of refactor onto object storage
            size = self.size(storage_path)
            return FileStat(FileType.FILE, size)

        else:

            self.ls(storage_path)
            return FileStat(FileType.DIRECTORY, 0)

    def ls(self, storage_path: str) -> tp.List[str]:

        self._log.info(f"LS [{storage_path}]")

        prefix = self._resolve_path(storage_path) + "/"

        response = self.__client.list_objects_v2(
            Bucket=self._bucket,
            Prefix=prefix,
            Delimiter="/")

        keys = []

        if "Contents" not in response and "CommonPrefixes" not in response:
            raise _ex.EStorageRequest(f"Storage prefix not found: [{storage_path}]")

        if "Contents" in response:
            for entry in response["Contents"]:
                raw_key = entry["Key"]
                if raw_key == prefix:
                    continue
                key = raw_key.replace(prefix, "")
                keys.append(key)

        if "CommonPrefixes" in response:
            for raw_prefix in response["CommonPrefixes"]:
                common_prefix = raw_prefix.replace(prefix, "")
                keys.append(common_prefix)

        return keys

    def mkdir(self, storage_path: str, recursive: bool = False, exists_ok: bool = False):

        self._log.info(f"MKDIR [{storage_path}]")

        # No-op in object storage
        pass

    def rm(self, storage_path: str, recursive: bool = False):

        try:
            self._log.info(f"RM [{storage_path}]")

            if recursive:
                raise RuntimeError("RM (recursive) not available for S3 storage")

            object_key = self._resolve_path(storage_path)
            self.__client.delete_object(Bucket=self._bucket, Key=object_key)

        except _aws_ex.ClientError as error:
            _aws_code = error.response['Error']['Code']
            raise _ex.EStorageRequest(f"Storage error: {str(error)}") from error

    def read_bytes(self, storage_path: str) -> bytes:

        self._log.info(f"READ BYTES [{storage_path}]")

        body = self._read_impl(storage_path)
        return body.read()

    def read_byte_stream(self, storage_path: str) -> tp.BinaryIO:

        self._log.info(f"READ BYTE STREAM [{storage_path}]")

        data = self.read_bytes(storage_path)
        return io.BytesIO(data)

    def _read_impl(self, storage_path: str) -> botocore.response.StreamingBody:

        try:

            object_key = self._resolve_path(storage_path)
            response = self.__client.get_object(Bucket=self._bucket, Key=object_key)
            return response['Body']

        except _aws_ex.ClientError as error:
            _aws_code = error.response['Error']['Code']
            raise _ex.EStorageRequest(f"Storage error: {str(error)}") from error

    def write_bytes(self, storage_path: str, data: bytes, overwrite: bool = False):

        try:
            self._log.info(f"WRITE BYTES [{storage_path}]")

            object_key = self._resolve_path(storage_path)

            self.__client.put_object(
                Bucket=self._bucket,
                Key=object_key,
                Body=data)

        except _aws_ex.ClientError as error:
            _aws_code = error.response['Error']['Code']
            raise _ex.EStorageRequest(f"Storage error: {str(error)}") from error

    def write_byte_stream(self, storage_path: str, overwrite: bool = False) -> tp.BinaryIO:

        self._log.info(f"WRITE BYTE STREAM [{storage_path}]")

        return self._AwsWriteBuf(self, storage_path, overwrite)

    class _AwsWriteBuf(io.BytesIO):

        def __init__(self, storage, storage_path, overwrite: bool):
            super().__init__()
            self._storage = storage
            self._storage_path = storage_path
            self._overwrite = overwrite
            self._written = False

        def close(self):
            if not self._written:
                self.seek(0)
                data = self.read()
                self._storage.write_bytes(self._storage_path, data, self._overwrite)
                self._written = True

    # TODO: These methods can be removed from the interface, they are not needed
    # (storage layer only needs to work in binary mode)

    def read_text(self, storage_path: str, encoding: str = 'utf-8') -> str:
        raise RuntimeError("READ (text mode) not available for S3 storage")

    def read_text_stream(self, storage_path: str, encoding: str = 'utf-8') -> tp.TextIO:
        raise RuntimeError("READ (text mode) not available for S3 storage")

    def write_text(self, storage_path: str, data: str, encoding: str = 'utf-8', overwrite: bool = False):
        raise RuntimeError("WRITE (text mode) not available for S3 storage")

    def write_text_stream(self, storage_path: str, encoding: str = 'utf-8', overwrite: bool = False) -> tp.TextIO:
        raise RuntimeError("WRITE (text mode) not available for S3 storage")

    def _resolve_path(self, storage_path: str) -> str:

        if self._rootPath is None or self._rootPath.strip() == "":
            return storage_path

        separator = "" if self._rootPath.endswith("/") else "/"
        full_path = self._rootPath + separator + storage_path

        return full_path[1:] if full_path.startswith("/") else full_path
