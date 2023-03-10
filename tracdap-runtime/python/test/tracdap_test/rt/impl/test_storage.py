#  Copyright 2022 Accenture Global Solutions Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License")
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

import datetime as dt
import decimal
import math
import pathlib
import tempfile
import time
import unittest
import sys
import random
import copy

import pyarrow as pa

import tracdap.rt.config as _cfg
import tracdap.rt.metadata as _meta
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.data as _data  # noqa
import tracdap.rt._impl.storage as _storage  # noqa
import tracdap.rt._impl.util as _util  # noqa


_ROOT_DIR = pathlib.Path(__file__).parent \
    .joinpath("../../../../../..") \
    .resolve()

_TEST_DATA_DIR = _ROOT_DIR \
    .joinpath("tracdap-libs/tracdap-lib-test/src/main/resources/sample_data")

_util.configure_logging()


class FileStorageTestSuite:

    # >>> Test suite for IFileStorage - file system operations, functional tests
    #
    # These tests are implemented purely in terms of the IFileStorage interface. The test suite can be run for
    # any storage implementation and a valid storage implementation must pass this test suite.
    #
    # NOTE: To test a new storage implementation, inherit from the suite and implement setUp()
    # to provide a storage implementation.
    #
    # Storage implementations may also wish to supply their own tests that use native APIs to set up and control
    # tests. This can allow for finer grained control, particularly when testing corner cases and error conditions.

    # Unit test implementation for local storage is in LocalFileStorageTest

    assertEqual = unittest.TestCase.assertEqual
    assertTrue = unittest.TestCase.assertTrue
    assertFalse = unittest.TestCase.assertFalse
    assertIsNotNone = unittest.TestCase.assertIsNotNone
    assertRaises = unittest.TestCase.assertRaises

    def __init__(self):
        self.storage: _storage.IFileStorage = None  # noqa
    
    # ------------------------------------------------------------------------------------------------------------------
    # EXISTS
    # ------------------------------------------------------------------------------------------------------------------

    def test_exists_dir(self):

        self.storage.mkdir("test_dir", False)

        dir_present = self.storage.exists("test_dir")
        dir_not_present = self.storage.exists("other_dir")

        self.assertTrue(dir_present)
        self.assertFalse(dir_not_present)

    def test_exists_file(self):

        self.make_small_file("test_file.txt")
    
        file_present = self.storage.exists("test_file.txt")
        file_not_present = self.storage.exists("other_file.txt")
    
        self.assertTrue(file_present)
        self.assertFalse(file_not_present)

    def test_exists_empty_file(self):

        self.make_file("test_file.txt", b"")
    
        empty_file_exist = self.storage.exists("test_file.txt")
    
        self.assertTrue(empty_file_exist)

    def test_exists_storage_root(self):

        # Storage root should always exist
    
        exists = self.storage.exists(".")
    
        self.assertTrue(exists)

    def test_exists_bad_paths(self):

        self.bad_paths(self.storage.exists)

    # ------------------------------------------------------------------------------------------------------------------
    # SIZE
    # ------------------------------------------------------------------------------------------------------------------

    def test_size_ok(self):

        content = "Content of a certain size\n".encode('utf-8')
        expected_size = len(content)

        self.make_file("test_file.txt", content)
    
        size = self.storage.size("test_file.txt")
    
        self.assertEqual(expected_size, size)

    def test_size_empty_file(self):

        self.make_file("test_file.txt", b"")
    
        size = self.storage.size("test_file.txt")
    
        self.assertEqual(0, size)

    def test_size_dir(self):

        self.storage.mkdir("test_dir", False)
        
        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.size("test_dir"))

    def test_size_missing(self):
            
        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.size("missing_file.txt"))

    def test_size_storage_root(self):

        # Storage root is a directory, size operation should fail with EStorageRequest

        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.size("."))

    def test_size_bad_paths(self):

        self.bad_paths(self.storage.size)
    
    # ------------------------------------------------------------------------------------------------------------------
    # STAT
    # ------------------------------------------------------------------------------------------------------------------

    def test_stat_file_ok(self):

        # Simple case - stat a file

        content = "Sample content for stat call\n".encode('utf-8')
        expected_size = len(content)
        
        self.storage.mkdir("some_dir", False)
        self.make_file("some_dir/test_file.txt", content)

        stat_result = self.storage.stat("some_dir/test_file.txt")

        self.assertEqual("some_dir/test_file.txt", stat_result.storagePath)
        self.assertEqual("test_file.txt", stat_result.fileName)
        self.assertEqual(_storage.FileType.FILE, stat_result.file_type)
        self.assertEqual(expected_size, stat_result.size)

    def test_stat_file_ctime(self):

        # For cloud storage buckets, it is likely that mtime is tracked but ctime is overwritten on updates
        # In this case, storage implementations may return a null ctime
        # If ctime is returned, then it must be valid
        
        test_start = dt.datetime.now(dt.timezone.utc)
        
        # On macOS (APFS), the stat ctime is rounded down to 1 second resolution,
        # even though the filesystem supports nanosecond precision (which is used for mtime and atime)
        # I am not sure if this is a bug in the JDK or a limitation in the underlying system calls
        # Either way, allowing a whole second of sleep should always mean the ctime is after test_start
        
        time.sleep(1.0)  # Let time elapse before/after the test calls

        self.make_small_file("test_file.txt")
        
        stat_result = self.storage.stat("test_file.txt")
        
        time.sleep(0.01)  # Let time elapse before/after the test calls
        test_finish = dt.datetime.now(dt.timezone.utc)
        
        self.assertTrue(stat_result.ctime is None or stat_result.ctime > test_start)
        self.assertTrue(stat_result.ctime is None or stat_result.ctime < test_finish)

    def test_stat_file_mtime(self):
    
        # All storage implementations must implement mtime for files

        test_start = dt.datetime.now(dt.timezone.utc)
        time.sleep(0.01)  # Let time elapse before/after the test calls

        self.make_small_file("test_file.txt")

        stat_result = self.storage.stat("test_file.txt")

        time.sleep(0.01)  # Let time elapse before/after the test calls
        test_finish = dt.datetime.now(dt.timezone.utc)

        self.assertTrue(stat_result.mtime > test_start)
        self.assertTrue(stat_result.mtime < test_finish)

    @unittest.skipIf(_util.is_windows(), "ATime testing disabled for Windows / NTFS")
    def test_stat_file_atime(self):
    
        # For cloud storage implementations, file atime may not be available
        # So, allow implementations to return a null atime
        # If an atime is returned for files, then it must be valid

        # NTFS does not handle atime reliably for this test. From the docs:

        #      NTFS delays updates to the last access time for a file by up to one hour after the last access.
        #      NTFS also permits last access time updates to be disabled.
        #      Last access time is not updated on NTFS volumes by default.

        # https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getfiletime?redirectedfrom=MSDN

        # On FAT32, atime is limited to an access date, i.e. one-day resolution

        self.make_small_file("test_file.txt")

        test_start = dt.datetime.now(dt.timezone.utc)
        time.sleep(0.01)  # Let time elapse before/after the test calls

        self.storage.read_bytes("test_file.txt")
        
        stat_result = self.storage.stat("test_file.txt")

        time.sleep(0.01)  # Let time elapse before/after the test calls
        test_finish = dt.datetime.now(dt.timezone.utc)

        self.assertTrue(stat_result.atime is None or stat_result.atime > test_start)
        self.assertTrue(stat_result.atime is None or stat_result.atime < test_finish)
    
    def test_stat_dir_ok(self):
    
        self.storage.mkdir("some_dir/test_dir", True)

        stat_result = self.storage.stat("some_dir/test_dir")

        self.assertEqual("some_dir/test_dir", stat_result.storagePath)
        self.assertEqual("test_dir", stat_result.fileName)
        self.assertEqual(_storage.FileType.DIRECTORY, stat_result.file_type)

        # Size field for directories should always be set to 0
        self.assertEqual(0, stat_result.size)
    
    def test_stat_dir_ctime(self):
    
        # ctime, mtime and atime for dirs is unlikely to be supported in cloud storage buckets
        # So, all of these fields are optional in stat responses for directories

        test_start = dt.datetime.now(dt.timezone.utc)

        # On macOS (APFS), the stat ctime is rounded down to 1 second resolution,
        # even though the filesystem supports nanosecond precision (which is used for mtime and atime)
        # I am not sure if this is a bug in the JDK or a limitation in the underlying system calls
        # Either way, allowing a whole second of sleep should always mean the ctime is after test_start

        time.sleep(1.0)  # Let time elapse before/after the test calls

        self.storage.mkdir("some_dir/test_dir", True)

        stat_result = self.storage.stat("some_dir/test_dir")

        time.sleep(0.01)  # Let time elapse before/after the test calls
        test_finish = dt.datetime.now(dt.timezone.utc)
        
        self.assertTrue(stat_result.ctime is None or stat_result.ctime > test_start)
        self.assertTrue(stat_result.ctime is None or stat_result.ctime < test_finish)
    
    def test_stat_dir_mtime(self):
    
        # ctime, mtime and atime for dirs is unlikely to be supported in cloud storage buckets
        # So, all of these fields are optional in stat responses for directories

        self.storage.mkdir("some_dir/test_dir", True)

        # "Modify" the directory by adding a file to it

        test_start = dt.datetime.now(dt.timezone.utc)
        time.sleep(0.01)  # Let time elapse before/after the test calls

        self.make_small_file("some_dir/test_dir/a_file.txt")

        stat_result = self.storage.stat("some_dir/test_dir")

        time.sleep(0.01)  # Let time elapse before/after the test calls
        test_finish = dt.datetime.now(dt.timezone.utc)
        
        self.assertTrue(stat_result.mtime is None or stat_result.mtime > test_start)
        self.assertTrue(stat_result.mtime is None or stat_result.mtime < test_finish)
    
    def test_stat_dir_atime(self):
    
        # ctime, mtime and atime for dirs is unlikely to be supported in cloud storage buckets
        # So, all of these fields are optional in stat responses for directories

        self.storage.mkdir("some_dir/test_dir", True)
        self.make_small_file("some_dir/test_dir/a_file.txt")

        # Access the directory by running "ls" on it

        test_start = dt.datetime.now(dt.timezone.utc)
        time.sleep(0.01)  # Let time elapse before/after the test calls

        self.storage.ls("some_dir/test_dir")

        stat_result = self.storage.stat("some_dir/test_dir")

        time.sleep(0.01)  # Let time elapse before/after the test calls
        test_finish = dt.datetime.now(dt.timezone.utc)
        
        self.assertTrue(stat_result.atime is None or stat_result.atime > test_start)
        self.assertTrue(stat_result.atime is None or stat_result.atime < test_finish)
    
    def test_stat_storage_root(self):
    
        root_stat = self.storage.stat(".")

        self.assertEqual(".", root_stat.storagePath)
        self.assertEqual(".", root_stat.fileName)
        self.assertEqual(_storage.FileType.DIRECTORY, root_stat.file_type)

        # Size field for directories should always be set to 0
        self.assertEqual(0, root_stat.size)
    
    def test_stat_missing(self):

        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.stat("does_not_exist.dat"))
    
    def test_stat_bad_paths(self):
    
        self.bad_paths(self.storage.stat)
    
    # ------------------------------------------------------------------------------------------------------------------
    # LS
    # ------------------------------------------------------------------------------------------------------------------
    
    def test_ls_ok(self):

        # Simple listing, dir containing one file and one sub dir
    
        self.storage.mkdir("test_dir", False)
        self.storage.mkdir("test_dir/child_1", False)
        self.make_small_file("test_dir/child_2.txt")

        ls = self.storage.ls("test_dir")

        self.assertEqual(2, len(ls))

        child1 = next(filter(lambda x: x == "child_1", ls), None)
        child2 = next(filter(lambda x: x == "child_2.txt", ls), None)

        self.assertIsNotNone(child1)
        self.assertEqual("test_dir/child_1", child1.storagePath)
        self.assertEqual(_storage.FileType.DIRECTORY, child1.fileType)

        self.assertIsNotNone(child2)
        self.assertEqual("test_dir/child_2.txt", child2.storagePath)
        self.assertEqual(_storage.FileType.FILE, child2.fileType)
    
    def test_ls_extensions(self):

        # Corner case - dir with an extension, file without extension

        self.storage.mkdir("ls_extensions", False)
        self.storage.mkdir("ls_extensions/child_1.dat", False)
        self.make_small_file("ls_extensions/child_2_file")

        ls = self.storage.ls("ls_extensions")

        self.assertEqual(2, len(ls))

        child1 = next(filter(lambda x: x == "child_1.dat", ls), None)
        child2 = next(filter(lambda x: x == "child_2_file", ls), None)

        self.assertIsNotNone(child1)
        self.assertEqual("ls_extensions/child_1.dat", child1.storagePath)
        self.assertEqual(_storage.FileType.DIRECTORY, child1.fileType)

        self.assertIsNotNone(child2)
        self.assertEqual("ls_extensions/child_2_file", child2.storagePath)
        self.assertEqual(_storage.FileType.FILE, child2.fileType)
    
    def test_ls_trailing_slash(self):
    
        # Storage path should be accepted with or without trailing slash

        self.storage.mkdir("ls_trailing_slash", False)
        self.make_small_file("ls_trailing_slash/some_file.txt")

        ls1 = self.storage.ls("ls_trailing_slash")
        ls2 = self.storage.ls("ls_trailing_slash/")

        self.assertEqual(1, len(ls1))
        self.assertEqual(1, len(ls2))
    
    def test_ls_storage_root_allowed(self):
    
        # Ls is one operation that is allowed on the storage root!
    
        self.storage.mkdir("test_dir", False)
        self.make_small_file("test_file.txt")

        ls = self.storage.ls(".")

        self.assertTrue(len(ls) >= 2)

        child1 = next(filter(lambda x: x == "test_dir", ls), None)
        child2 = next(filter(lambda x: x == "test_file.txt", ls), None)

        self.assertIsNotNone(child1)
        self.assertEqual("test_dir", child1.storagePath)
        self.assertEqual(_storage.FileType.DIRECTORY, child1.fileType)

        self.assertIsNotNone(child2)
        self.assertEqual("test_file.txt", child2.storagePath)
        self.assertEqual(_storage.FileType.FILE, child2.fileType)
    
    def test_ls_file(self):
    
        # Attempt to call ls on a file is an error

        self.make_small_file("test_file")

        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.ls("test_file"))
    
    def test_ls_missing(self):
    
        # Ls on a missing path is an error condition

        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.ls("dir_does_not_exist/"))
    
    def test_ls_bad_paths(self):
    
        self.bad_paths(self.storage.ls)

    # ------------------------------------------------------------------------------------------------------------------
    # MKDIR
    # ------------------------------------------------------------------------------------------------------------------
    
    def test_mkdir_ok(self):

        # Simplest case - create a single directory

        self.storage.mkdir("test_dir", False)

        # Creating a single child dir when the parent already exists

        self.storage.mkdir("test_dir/child", False)

        dir_exists = self.storage.exists("test_dir")
        child_exists = self.storage.exists("test_dir/child")

        self.assertTrue(dir_exists)
        self.assertTrue(child_exists)
    
    def test_mkdir_dir_exists(self):

        # mkdir with recursive = false should throw EStorageRequest if dir already exists
    
        self.storage.mkdir("test_dir", False)

        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.mkdir("test_dir", False))
    
    def test_mkdir_file_exists(self):
    
        # mkdir should always fail if requested dir already exists and is a file

        self.make_small_file("test_dir")

        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.mkdir("test_dir", False))
    
    def test_mkdir_missing_parent(self):
    
        # With recursive = false, mkdir with a missing parent should fail
        # Neither parent nor child dir should be created

        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.mkdir("test_dir/child", False))

        dir_exists = self.storage.exists("test_dir")
        child_exists = self.storage.exists("test_dir/child")

        self.assertFalse(dir_exists)
        self.assertFalse(child_exists)
    
    def test_mkdir_recursive_ok(self):
    
        # mkdir, recursive = true, create parent and child dir in a single call

        self.storage.mkdir("test_dir/child", True)

        dir_exists = self.storage.exists("test_dir")
        child_exists = self.storage.exists("test_dir/child")
    
        self.assertTrue(dir_exists)
        self.assertTrue(child_exists)
    
    def test_mkdir_recursive_dir_eists(self):
    
        # mkdir, when recursive = true it is not an error if the target dir already exists

        self.storage.mkdir("test_dir/child", True)

        self.storage.mkdir("test_dir/child", True)

        dir_exists = self.storage.exists("test_dir")
        child_exists = self.storage.exists("test_dir/child")

        self.assertTrue(dir_exists)
        self.assertTrue(child_exists)
    
    def test_mkdir_recursive_file_exists(self):
    
        # mkdir should always fail if requested dir already exists and is a file

        self.storage.mkdir("test_dir", False)
        self.make_small_file("test_dir/child")

        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.mkdir("test_dir/child", True))
    
    def test_mkdir_bad_paths(self):
    
        self.bad_paths(lambda path_: self.storage.mkdir(path_, False))
        self.bad_paths(lambda path_: self.storage.mkdir(path_, True))
    
    def test_mkdir_storage_root(self):
    
        self.fail_for_storage_root(lambda path_: self.storage.mkdir(path_, False))
        self.fail_for_storage_root(lambda path_: self.storage.mkdir(path_, True))

    # -----------------------------------------------------------------------------------------------------------------
    # RM
    # -----------------------------------------------------------------------------------------------------------------
    
    def test_rm_ok(self):
    
        # Simplest case - create one file and delete it

        self.make_small_file("test_file.txt")

        self.storage.rm("test_file.txt", False)

        # File should be gone

        exists = self.storage.exists("test_file.txt")
        self.assertFalse(exists)
    
    def test_rm_dir(self):
    
        # Calling rm on a directory with recursive = false is a bad request, even if the dir is empty

        self.storage.mkdir("test_dir", False)
        
        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.rm("test_dir", False))

        # Dir should still exist because rm has failed

        exists = self.storage.exists("test_dir")
        self.assertTrue(exists)
    
    def test_rm_missing(self):
    
        # Try to delete a path that does not exist
        
        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.rm("missing_path", False))
    
    def test_rm_recursive(self):
    
        # Delete one whole dir tree
        # Sibling dir tree should be unaffected

        self.storage.mkdir("test_dir/child_1", True)
        self.make_small_file("test_dir/child_1/file_a.txt")
        self.make_small_file("test_dir/child_1/file_b.txt")
        self.storage.mkdir("test_dir/child_2", True)
        self.make_small_file("test_dir/child_2/file_a.txt")
        
        self.storage.rm("test_dir/child_1", True)

        exists1 = self.storage.exists("test_dir/child_1")
        exists2 = self.storage.exists("test_dir/child_2")
        size2a = self.storage.size("test_dir/child_2/file_a.txt")

        self.assertFalse(exists1)
        self.assertTrue(exists2)
        self.assertTrue(size2a > 0)
    
    def test_rm_recursive_file(self):
    
        # Calling rm for a single file with recursive = true is not an error
        # The recursive delete should just remove that individual file

        self.storage.mkdir("test_dir", False)
        self.make_small_file("test_dir/file_a.txt")
        self.make_small_file("test_dir/file_b.txt")
        
        self.storage.rm("test_dir/file_a.txt", True)

        exists_a = self.storage.exists("test_dir/file_a.txt")
        exists_b = self.storage.exists("test_dir/file_b.txt")

        self.assertFalse(exists_a)
        self.assertTrue(exists_b)
    
    def test_rm_recursive_missing(self):

        # Try to delete a path that does not exist, should fail regardless of recursive = true

        self.storage.mkdir("test_dir", False)

        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.rm("test_dir/child", True))

    def test_rm_bad_paths(self):

        self.bad_paths(lambda path_: self.storage.rm(path_, False))
        self.bad_paths(lambda path_: self.storage.rm(path_, True))

    def test_rm_storage_root(self):

        self.fail_for_storage_root(lambda path_: self.storage.rm(path_, False))
        self.fail_for_storage_root(lambda path_: self.storage.rm(path_, True))

    # -----------------------------------------------------------------------------------------------------------------
    # COMMON TESTS (tests applied to several storage calls)
    # -----------------------------------------------------------------------------------------------------------------
    
    def fail_for_storage_root(self, test_method):
    
        # TRAC should not allow write-operations with path "." to reach the storage layer
        # Storage implementations should report this as a validation gap

        self.assertRaises(_ex.EStorageRequest, lambda: test_method.__call__("."))
    
    def bad_paths(self, test_method):

        # \0 and / are the two characters that are always illegal in posix filenames
        # But / will be interpreted as a separator
        # There are several illegal characters for filenames on Windows!

        escaping_path = "../"
        absolute_path = "C:\\Windows" if _util.is_windows() else "/bin"
        invalid_path = "@$ N'`$>.)_\"+\n%" if _util.is_windows() else "nul\0char"

        self.assertRaises(_ex.EStorageRequest, lambda: test_method.__call__(escaping_path))
        self.assertRaises(_ex.EStorageRequest, lambda: test_method.__call__(absolute_path))
        self.assertRaises(_ex.EStorageRequest, lambda: test_method.__call__(invalid_path))

    def make_small_file(self, storage_path: str):

        content_size = random.randint(1024, 4096)
        content = random.randbytes(content_size)

        self.storage.write_bytes(storage_path, content)

    def make_file(self, storage_path: str, content: bytes):

        self.storage.write_bytes(storage_path, content)


class DataStorageTestSuite:

    storage: _storage.IDataStorage
    storage_format: str

    assertEqual = unittest.TestCase.assertEqual
    assertTrue = unittest.TestCase.assertTrue
    assertIsNotNone = unittest.TestCase.assertIsNotNone
    assertRaises = unittest.TestCase.assertRaises

    @staticmethod
    def sample_schema():

        trac_schema = _meta.SchemaDefinition(
            _meta.SchemaType.TABLE,
            _meta.PartType.PART_ROOT,
            _meta.TableSchema(fields=[
                _meta.FieldSchema("boolean_field", fieldType=_meta.BasicType.BOOLEAN),
                _meta.FieldSchema("integer_field", fieldType=_meta.BasicType.INTEGER),
                _meta.FieldSchema("float_field", fieldType=_meta.BasicType.FLOAT),
                _meta.FieldSchema("decimal_field", fieldType=_meta.BasicType.DECIMAL),
                _meta.FieldSchema("string_field", fieldType=_meta.BasicType.STRING),
                _meta.FieldSchema("date_field", fieldType=_meta.BasicType.DATE),
                _meta.FieldSchema("datetime_field", fieldType=_meta.BasicType.DATETIME),
            ]))

        return _data.DataMapping.trac_to_arrow_schema(trac_schema)

    @staticmethod
    def sample_data():

        return {
            "boolean_field": [True, False, True, False],
            "integer_field": [1, 2, 3, 4],
            "float_field": [1.0, 2.0, 3.0, 4.0],
            "decimal_field": [decimal.Decimal(1.0), decimal.Decimal(2.0), decimal.Decimal(3.0), decimal.Decimal(4.0)],
            "string_field": ["hello", "world", "what's", "up"],
            "date_field": [dt.date(2000, 1, 1), dt.date(2000, 1, 2), dt.date(2000, 1, 3), dt.date(2000, 1, 4)],
            "datetime_field": [
                dt.datetime(2000, 1, 1, 0, 0, 0), dt.datetime(2000, 1, 2, 1, 1, 1),
                dt.datetime(2000, 1, 3, 2, 2, 2), dt.datetime(2000, 1, 4, 3, 3, 3)]
        }

    @staticmethod
    def one_field_schema(field_type: _meta.BasicType):

        field_name = f"{field_type.name.lower()}_field"

        trac_schema = _meta.SchemaDefinition(
            _meta.SchemaType.TABLE,
            _meta.PartType.PART_ROOT,
            _meta.TableSchema(fields=[
                _meta.FieldSchema(field_name, fieldType=field_type)]))

        return _data.DataMapping.trac_to_arrow_schema(trac_schema)

    @staticmethod
    def random_bytes(n_bytes: int) -> bytes:

        bs = bytearray(n_bytes)

        for i in range(n_bytes):
            b = random.randint(0, 255)
            bs[i] = b

        return bytes(bs)

    def test_round_trip_basic(self):

        table = pa.Table.from_pydict(self.sample_data(), self.sample_schema())  # noqa

        self.storage.write_table("round_trip_basic", self.storage_format, table)
        rt_table = self.storage.read_table("round_trip_basic", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_round_trip_nulls(self):

        sample_data = self.sample_data()

        for col, values in sample_data.items():
            values[0] = None

        table = pa.Table.from_pydict(sample_data, self.sample_schema())  # noqa

        self.storage.write_table("round_trip_nulls", self.storage_format, table)
        rt_table = self.storage.read_table("round_trip_nulls", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_edge_cases_integer(self):

        schema = self.one_field_schema(_meta.BasicType.INTEGER)
        table = pa.Table.from_pydict({"integer_field": [  # noqa
            0,
            sys.maxsize,
            -sys.maxsize - 1
        ]}, schema)

        self.storage.write_table("edge_cases_integer", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_integer", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_edge_cases_float(self):

        # It may be helpful to check for / prohibit inf and -inf in some places, e.g. model outputs
        # But still the storage layer should handle these values correctly if they are present

        schema = self.one_field_schema(_meta.BasicType.FLOAT)
        table = pa.Table.from_pydict({"float_field": [  # noqa
            0.0,
            sys.float_info.min,
            sys.float_info.max,
            sys.float_info.epsilon,
            -sys.float_info.epsilon,
            math.inf,
            -math.inf
        ]}, schema)

        self.storage.write_table("edge_cases_float", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_float", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_edge_cases_float_nan(self):

        # For NaN, a special test that checks math.isnan on the round-trip result
        # Because math.nan != math.nan
        # Also, make sure to keep the distinction between NaN and None

        schema = self.one_field_schema(_meta.BasicType.FLOAT)
        table = pa.Table.from_pydict({"float_field": [math.nan]}, schema)  # noqa

        self.storage.write_table("edge_cases_float_nan", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_float_nan", self.storage_format, table.schema)

        nan_value = rt_table.column(0)[0].as_py()

        self.assertIsNotNone(nan_value)
        self.assertTrue(math.isnan(nan_value))

    def test_edge_cases_decimal(self):

        # TRAC basic decimal has precision 38, scale 12
        # Should allow for 26 places before the decimal place and 12 after

        schema = self.one_field_schema(_meta.BasicType.DECIMAL)
        table = pa.Table.from_pydict({"decimal_field": [  # noqa
            decimal.Decimal(0.0),
            decimal.Decimal(1.0) * decimal.Decimal(1.0).shift(25),
            decimal.Decimal(1.0) / decimal.Decimal(1.0).shift(12),
            decimal.Decimal(-1.0) * decimal.Decimal(1.0).shift(25),
            decimal.Decimal(-1.0) / decimal.Decimal(1.0).shift(12)
        ]}, schema)

        self.storage.write_table("edge_cases_decimal", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_decimal", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_edge_cases_string(self):

        schema = self.one_field_schema(_meta.BasicType.STRING)
        table = pa.Table.from_pydict({"string_field": [  # noqa
            "", " ", "  ", "\t", "\r\n", "  \r\n   ",
            "a, b\",", "'@@'", "[\"\"%^&", "£££", "#@",
            "Olá Mundo", "你好，世界", "Привет, мир", "नमस्ते दुनिया",
            "𝜌 = ∑ 𝑃𝜓 | 𝜓 ⟩ ⟨ 𝜓 |"
        ]}, schema)

        self.storage.write_table("edge_cases_string", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_string", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_edge_cases_date(self):

        schema = self.one_field_schema(_meta.BasicType.DATE)
        table = pa.Table.from_pydict({"date_field": [  # noqa
            dt.date(1970, 1, 1),
            dt.date(2000, 1, 1),
            dt.date(2038, 1, 20),
            dt.date.max,
            dt.date.min
        ]}, schema)

        self.storage.write_table("edge_cases_date", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_date", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)

    def test_edge_cases_datetime(self):

        schema = self.one_field_schema(_meta.BasicType.DATETIME)
        table = pa.Table.from_pydict({"datetime_field": [  # noqa
            dt.datetime(1970, 1, 1, 0, 0, 0),
            dt.datetime(2000, 1, 1, 0, 0, 0),
            dt.datetime(2038, 1, 19, 3, 14, 8),
            # Fractional seconds before and after the epoch
            # Test fractions for both positive and negative encoded values
            dt.datetime(1972, 1, 1, 0, 0, 0, 500000),
            dt.datetime(1968, 1, 1, 23, 59, 59, 500000),
            dt.datetime.max,
            dt.datetime.min
        ]}, schema)

        self.storage.write_table("edge_cases_datetime", self.storage_format, table)
        rt_table = self.storage.read_table("edge_cases_datetime", self.storage_format, table.schema)

        self.assertEqual(table, rt_table)


# ----------------------------------------------------------------------------------------------------------------------
# UNIT TESTS
# ----------------------------------------------------------------------------------------------------------------------

# Unit tests call the test suite using the local storage implementation


class LocalFileStorageTest(unittest.TestCase, FileStorageTestSuite):

    storage_root: tempfile.TemporaryDirectory
    test_number: int

    @classmethod
    def setUpClass(cls) -> None:

        cls.storage_root = tempfile.TemporaryDirectory()
        cls.test_number = 0

    def setUp(self):

        test_dir = pathlib.Path(self.storage_root.name).joinpath(f"test_{self.test_number}")
        test_dir.mkdir()

        LocalFileStorageTest.test_number += 1

        test_storage_config = _cfg.PluginConfig(
            protocol="LOCAL",
            properties={"rootPath": str(test_dir)})

        sys_config = _cfg.RuntimeConfig()
        sys_config.storage = _cfg.StorageConfig()
        sys_config.storage.buckets["test_bucket"] = test_storage_config

        manager = _storage.StorageManager(sys_config)
        self.storage = manager.get_file_storage("test_bucket")

    @classmethod
    def tearDownClass(cls) -> None:

        cls.storage_root.cleanup()


class LocalArrowNativeStorageTest(unittest.TestCase, FileStorageTestSuite):

    storage_root: tempfile.TemporaryDirectory
    test_number: int

    @classmethod
    def setUpClass(cls) -> None:

        cls.storage_root = tempfile.TemporaryDirectory()
        cls.test_number = 0

    def setUp(self):

        test_dir = pathlib.Path(self.storage_root.name).joinpath(f"test_{self.test_number}")
        test_dir.mkdir()

        LocalArrowNativeStorageTest.test_number += 1

        test_storage_config = _cfg.PluginConfig(
            protocol="LOCAL",
            properties={
                "rootPath": str(test_dir),
                "arrowNativeFs": "true"})

        sys_config = _cfg.RuntimeConfig()
        sys_config.storage = _cfg.StorageConfig()
        sys_config.storage.buckets["test_bucket"] = test_storage_config

        manager = _storage.StorageManager(sys_config)
        self.storage = manager.get_file_storage("test_bucket")

    @classmethod
    def tearDownClass(cls) -> None:

        cls.storage_root.cleanup()


class LocalStorageTest(DataStorageTestSuite):

    storage_root: tempfile.TemporaryDirectory
    file_storage: _storage.IFileStorage

    @classmethod
    def make_storage(cls):

        cls.storage_root = tempfile.TemporaryDirectory()

        bucket_config = _cfg.PluginConfig(
            protocol="LOCAL",
            properties={"rootPath": cls.storage_root.name})

        sys_config = _cfg.RuntimeConfig()
        sys_config.storage = _cfg.StorageConfig()
        sys_config.storage.buckets["test_bucket"] = bucket_config

        manager = _storage.StorageManager(sys_config)
        file_storage = manager.get_file_storage("test_bucket")
        data_storage = manager.get_data_storage("test_bucket")

        cls.file_storage = file_storage

        return data_storage

    # For file-based storage, test reading garbled data

    def test_read_garbled_data(self):

        garbage = self.random_bytes(256)
        schema = self.sample_schema()

        self.file_storage.write_bytes(f"garbled_data.{self.storage_format}", garbage)

        self.assertRaises(
            _ex.EDataCorruption,
            lambda: self.storage.read_table(
                f"garbled_data.{self.storage_format}",
                self.storage_format, schema))


class LocalCsvStorageTest(unittest.TestCase, LocalStorageTest):

    @classmethod
    def setUpClass(cls) -> None:
        cls.storage = LocalStorageTest.make_storage()
        cls.storage_format = "CSV"

        test_lib_storage_config = _cfg.PluginConfig(
            protocol="LOCAL",
            properties={"rootPath": str(_TEST_DATA_DIR)})

        sys_config = _cfg.RuntimeConfig()
        sys_config.storage = _cfg.StorageConfig()
        sys_config.storage.buckets["test_csv_bucket"] = test_lib_storage_config

        manager = _storage.StorageManager(sys_config)
        test_lib_data_storage = manager.get_data_storage("test_csv_bucket")

        cls.test_lib_storage_instance_cfg = test_lib_storage_config
        cls.test_lib_storage = test_lib_data_storage

    @classmethod
    def tearDownClass(cls):
        cls.storage_root.cleanup()

    @unittest.skip("CSV read hangs with the strict (Arrow) CSV implementation for garbled data")
    def test_read_garbled_data(self):
        super().test_read_garbled_data()

    def test_csv_basic(self):

        storage_options = {"lenient_csv_parser": True}

        schema = self.sample_schema()
        table = self.test_lib_storage.read_table("csv_basic.csv", "CSV", schema, storage_options)

        self.assertEqual(7, table.num_columns)
        self.assertEqual(10, table.num_rows)

    def test_lenient_edge_cases(self):

        storage_options = {"lenient_csv_parser": True}

        schema = self.sample_schema()
        table = self.test_lib_storage.read_table("csv_edge_cases.csv", "CSV", schema, storage_options)

        self.assertEqual(7, table.num_columns)
        self.assertEqual(10, table.num_rows)

    def test_lenient_nulls(self):

        storage_options = {"lenient_csv_parser": True}

        schema = self.sample_schema()
        table = self.test_lib_storage.read_table("csv_nulls.csv", "CSV", schema, storage_options)

        self.assertEqual(7, table.num_columns)
        self.assertEqual(7, table.num_rows)

        # Nulls test dataset has nulls in the diagonals, i.e. row 0 col 0, row 1 col 1 etc.

        for i in range(7):
            column: pa.Array = table.column(i)
            column_name = table.column_names[i]
            value = column[i].as_py()

            # The lenient CSV parser does not know the difference between empty string and null

            if column_name == "string_field":
                self.assertEqual(value, "")

            else:
                self.assertIsNone(value, msg=f"Found non-null value in row [{i}], column [{column_name}]")

    def test_lenient_read_garbled_data(self):

        # Try reading garbage data with the lenient CSV parser

        storage_options = {"lenient_csv_parser": True}

        garbage = self.random_bytes(256)
        schema = self.sample_schema()

        self.file_storage.write_bytes(f"csv_garbled_data.{self.storage_format}", garbage)

        self.assertRaises(
            _ex.EDataCorruption,
            lambda: self.storage.read_table(
                f"csv_garbled_data.{self.storage_format}",
                self.storage_format, schema, storage_options))

    def test_lenient_read_garbled_text(self):

        # Try reading garbage textual data with the lenient CSV parser
        # Because CSV is such a loose format, the parser will assemble rows and columns
        # However, some form of EData exception should still be raised
        # Since reading CSV requires a schema and the schema will not match, normally this will be EDataConformance

        storage_options = {"lenient_csv_parser": True}

        garbage = "£$%£$%£$%£$%'#[]09h8\t{}}},,,,AS£F".encode("utf-8")
        schema = self.sample_schema()

        self.file_storage.write_bytes(f"csv_garbled_data_2.{self.storage_format}", garbage)

        self.assertRaises(
            _ex.EData,
            lambda: self.storage.read_table(
                f"csv_garbled_data_2.{self.storage_format}",
                self.storage_format, schema, storage_options))

    def test_csv_nan(self):

        # Test reading in CSV NaN with the strict (Apache Arrow) CSV parser

        schema = pa.schema([("float_field", pa.float64())])
        table = self.test_lib_storage.read_table("csv_nan.csv", "CSV", schema)

        self.assertEqual(1, table.num_columns)
        self.assertEqual(2, table.num_rows)

        for row, value in enumerate(table.column(0)):
            self.assertIsNotNone(value.as_py())
            self.assertTrue(math.isnan(value.as_py()))

    def test_date_format_props(self):

        test_lib_storage_instance = copy.deepcopy(self.test_lib_storage_instance_cfg)
        test_lib_storage_instance.properties["csv.lenient_csv_parser"] = "true"
        test_lib_storage_instance.properties["csv.date_format"] = "%d/%m/%Y"
        test_lib_storage_instance.properties["csv.datetime_format"] = "%d/%m/%Y %H:%M:%S"

        sys_config = _cfg.RuntimeConfig()
        sys_config.storage = _cfg.StorageConfig()
        sys_config.storage.buckets["test_csv_bucket"] = test_lib_storage_instance

        manager = _storage.StorageManager(sys_config)
        test_lib_data_storage = manager.get_data_storage("test_csv_bucket")

        schema = self.sample_schema()
        table = test_lib_data_storage.read_table("csv_basic_uk_dates.csv", "CSV", schema)

        self.assertEqual(7, table.num_columns)
        self.assertEqual(10, table.num_rows)


class LocalArrowStorageTest(unittest.TestCase, LocalStorageTest):

    @classmethod
    def setUpClass(cls) -> None:
        cls.storage = LocalStorageTest.make_storage()
        cls.storage_format = "ARROW_FILE"

    @classmethod
    def tearDownClass(cls):
        cls.storage_root.cleanup()


class LocalParquetStorageTest(unittest.TestCase, LocalStorageTest):

    @classmethod
    def setUpClass(cls) -> None:
        cls.storage = LocalStorageTest.make_storage()
        cls.storage_format = "PARQUET"

    @classmethod
    def tearDownClass(cls):
        cls.storage_root.cleanup()
