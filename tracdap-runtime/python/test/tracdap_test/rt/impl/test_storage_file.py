#  Copyright 2023 Accenture Global Solutions Limited
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

import datetime as dt
import pathlib
import tempfile
import time
import unittest
import random

import tracdap.rt.config as _cfg
import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.data as _data  # noqa
import tracdap.rt._impl.storage as _storage  # noqa
import tracdap.rt._impl.util as _util  # noqa

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

        self.assertEqual("some_dir/test_file.txt", stat_result.storage_path)
        self.assertEqual("test_file.txt", stat_result.file_name)
        self.assertEqual(_storage.FileType.FILE, stat_result.file_type)
        self.assertEqual(expected_size, stat_result.size)

    def test_stat_file_mtime(self):

        # All storage implementations must implement mtime for files
        # Do not allow null mtime

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

        self.assertEqual("some_dir/test_dir", stat_result.storage_path)
        self.assertEqual("test_dir", stat_result.file_name)
        self.assertEqual(_storage.FileType.DIRECTORY, stat_result.file_type)

        # Size field for directories should always be set to 0
        self.assertEqual(0, stat_result.size)

    def test_stat_dir_mtime(self):

        # mtime and atime for dirs is unlikely to be supported in cloud storage buckets
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

        # mtime and atime for dirs is unlikely to be supported in cloud storage buckets
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

        self.assertEqual(".", root_stat.storage_path)
        self.assertEqual(".", root_stat.file_name)
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
    # COMMON HELPERS (used for several storage calls)
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