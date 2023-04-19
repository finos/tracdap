#  Copyright 2023 Accenture Global Solutions Limited
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
import functools
import time
import typing as tp
import unittest
import random

import tracdap.rt.exceptions as _ex
import tracdap.rt._impl.data as _data  # noqa
import tracdap.rt._impl.storage as _storage  # noqa
import tracdap.rt._impl.util as _util  # noqa

_util.configure_logging()

# randbytes was only added to the random module in 3.9
# For testing, alias to secrets.token_bytes if it is not available (available since 3.6)
if "randbytes" not in random.__dict__:
    import secrets
    random.__dict__["randbytes"] = secrets.token_bytes


class FileOperationsTestSuite:

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

    def test_exists_dir(self):

        self.storage.mkdir("test_dir", False)

        dir_present = self.storage.exists("test_dir")
        dir_not_present = self.storage.exists("other_dir")

        self.assertTrue(dir_present)
        self.assertFalse(dir_not_present)

    def test_exists_parent_dir(self):

        self.storage.mkdir("parent_dir/child_dir", True)

        dir_present = self.storage.exists("parent_dir")
        dir_not_present = self.storage.exists("other_dir")

        self.assertTrue(dir_present)
        self.assertFalse(dir_not_present)

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

        # All storage implementations must implement mtime for files, do not allow null mtime
        # Using 1 second as the required resolution (at least one FS, AWS S3, has 1 second resolution)

        test_start = dt.datetime.now(dt.timezone.utc)
        time.sleep(1.0)  # Let time elapse before/after the test calls

        self.make_small_file("test_file.txt")

        stat_result = self.storage.stat("test_file.txt")

        time.sleep(1.0)  # Let time elapse before/after the test calls
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

    @unittest.skipIf(_util.is_windows(), "ATime testing disabled for Windows / NTFS")
    def test_stat_dir_atime(self):

        # This test fails intermittently for local storage on Windows, for the same reason as test_stat_file_atime
        # https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getfiletime?redirectedfrom=MSDN

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

        child1 = next(filter(lambda x: x.file_name == "child_1", ls), None)
        child2 = next(filter(lambda x: x.file_name == "child_2.txt", ls), None)

        self.assertIsNotNone(child1)
        self.assertEqual("test_dir/child_1", child1.storage_path)
        self.assertEqual(_storage.FileType.DIRECTORY, child1.file_type)

        self.assertIsNotNone(child2)
        self.assertEqual("test_dir/child_2.txt", child2.storage_path)
        self.assertEqual(_storage.FileType.FILE, child2.file_type)

    def test_ls_extensions(self):

        # Corner case - dir with an extension, file without extension

        self.storage.mkdir("ls_extensions", False)
        self.storage.mkdir("ls_extensions/child_1.dat", False)
        self.make_small_file("ls_extensions/child_2_file")

        ls = self.storage.ls("ls_extensions")

        self.assertEqual(2, len(ls))

        child1 = next(filter(lambda x: x.file_name == "child_1.dat", ls), None)
        child2 = next(filter(lambda x: x.file_name == "child_2_file", ls), None)

        self.assertIsNotNone(child1)
        self.assertEqual("ls_extensions/child_1.dat", child1.storage_path)
        self.assertEqual(_storage.FileType.DIRECTORY, child1.file_type)

        self.assertIsNotNone(child2)
        self.assertEqual("ls_extensions/child_2_file", child2.storage_path)
        self.assertEqual(_storage.FileType.FILE, child2.file_type)

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

        child1 = next(filter(lambda x: x.file_name == "test_dir", ls), None)
        child2 = next(filter(lambda x: x.file_name == "test_file.txt", ls), None)

        self.assertIsNotNone(child1)
        self.assertEqual("test_dir", child1.storage_path)
        self.assertEqual(_storage.FileType.DIRECTORY, child1.file_type)

        self.assertIsNotNone(child2)
        self.assertEqual("test_file.txt", child2.storage_path)
        self.assertEqual(_storage.FileType.FILE, child2.file_type)

    def test_ls_file(self):

        # Calling LS on a file returns a list of just that one file

        self.make_small_file("test_file")

        ls = self.storage.ls("test_file")

        self.assertEqual(1, len(ls))

        stat = ls[0]

        self.assertEqual(_storage.FileType.FILE, stat.file_type)
        self.assertEqual("test_file", stat.file_name)
        self.assertEqual("test_file", stat.storage_path)

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

        # It is not an error to call mkdir on an existing directory

        self.storage.mkdir("test_dir", False)

        dir_exists_1 = self.storage.exists("test_dir")
        self.assertTrue(dir_exists_1)

        self.storage.mkdir("test_dir", False)

        dir_exists_2 = self.storage.exists("test_dir")
        self.assertTrue(dir_exists_2)

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

    def test_mkdir_recursive_dir_exists(self):

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

    # ------------------------------------------------------------------------------------------------------------------
    # RM
    # ------------------------------------------------------------------------------------------------------------------

    def test_rm_ok(self):

        # Simplest case - create one file and delete it

        self.make_small_file("test_file.txt")

        self.storage.rm("test_file.txt")

        # File should be gone

        exists = self.storage.exists("test_file.txt")
        self.assertFalse(exists)

    def test_rm_in_subdir_ok(self):

        # Simplest case - create one file and delete it

        self.make_small_file("sub_dir/test_file.txt")

        self.storage.rm("sub_dir/test_file.txt")

        # File should be gone

        exists = self.storage.exists("sub_dir/test_file.txt")
        self.assertFalse(exists)

    def test_rm_on_dir(self):

        # Calling rm on a directory is a bad request, even if the dir is empty

        self.storage.mkdir("test_dir")

        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.rm("test_dir"))

        # Dir should still exist because rm has failed

        exists = self.storage.exists("test_dir")
        self.assertTrue(exists)

    def test_rm_missing(self):

        # Try to delete a path that does not exist

        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.rm("missing_path.dat"))

    def test_rm_bad_paths(self):

        self.bad_paths(self.storage.rm)

    def test_rm_storage_root(self):

        self.fail_for_storage_root(self.storage.rm)

    # ------------------------------------------------------------------------------------------------------------------
    # RMDIR
    # ------------------------------------------------------------------------------------------------------------------

    def test_rmdir_ok(self):

        self.storage.mkdir("test_dir", False)

        exists = self.storage.exists("test_dir")
        self.assertTrue(exists)

        self.storage.rmdir("test_dir")

        exists = self.storage.exists("test_dir")
        self.assertFalse(exists)

    def test_rmdir_by_prefix(self):

        self.storage.mkdir("test_dir/sub_dir", False)

        exists = self.storage.exists("test_dir")
        self.assertTrue(exists)

        self.storage.rmdir("test_dir")

        exists = self.storage.exists("test_dir")
        self.assertFalse(exists)

    def test_rmdir_with_content(self):

        # Delete one whole dir tree
        # Sibling dir tree should be unaffected

        self.storage.mkdir("test_dir/child_1", True)
        self.storage.mkdir("test_dir/child_1/sub")
        self.make_small_file("test_dir/child_1/file_a.txt")
        self.make_small_file("test_dir/child_1/file_b.txt")
        self.storage.mkdir("test_dir/child_2", True)
        self.make_small_file("test_dir/child_2/file_a.txt")

        self.storage.rmdir("test_dir/child_1")

        exists1 = self.storage.exists("test_dir/child_1")
        exists2 = self.storage.exists("test_dir/child_2")
        size2a = self.storage.size("test_dir/child_2/file_a.txt")

        self.assertFalse(exists1)
        self.assertTrue(exists2)
        self.assertTrue(size2a > 0)

    def test_rmdir_on_file(self):

        # Calling rmdir on a file is a bad request

        self.make_small_file("test_file.txt")

        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.rmdir("test_file.txt"))

        # File should still exist because rm has failed

        exists = self.storage.exists("test_file.txt")
        self.assertTrue(exists)

    def test_rmdir_missing(self):

        # Try to delete a path that does not exist

        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.rmdir("missing_path"))

    def test_rmdir_bad_paths(self):

        self.bad_paths(self.storage.rmdir)

    def test_rmdir_storage_root(self):

        self.fail_for_storage_root(self.storage.rmdir)

    # ------------------------------------------------------------------------------------------------------------------
    # COMMON HELPERS (used for several storage calls)
    # ------------------------------------------------------------------------------------------------------------------

    def fail_for_storage_root(self, test_method):

        # TRAC should not allow write-operations with path "." to reach the storage layer
        # Storage implementations should report this as a validation gap

        self.assertRaises(_ex.EStorageValidation, lambda: test_method.__call__("."))

    def bad_paths(self, test_method):

        # \0 and / are the two characters that are always illegal in posix filenames
        # But / will be interpreted as a separator
        # There are several illegal characters for filenames on Windows!

        escaping_path = "../"
        absolute_path = "C:\\Windows" if _util.is_windows() else "/bin"
        invalid_path = "@$ N'`$>.)_\"+\n%" if _util.is_windows() else "nul\0char"

        self.assertRaises(_ex.EStorageValidation, lambda: test_method.__call__(escaping_path))
        self.assertRaises(_ex.EStorageValidation, lambda: test_method.__call__(absolute_path))
        self.assertRaises(_ex.EStorageValidation, lambda: test_method.__call__(invalid_path))

    def make_small_file(self, storage_path: str):

        content_size = random.randint(1024, 4096)
        content = random.randbytes(content_size)

        self.storage.write_bytes(storage_path, content)

    def make_file(self, storage_path: str, content: bytes):

        self.storage.write_bytes(storage_path, content)


class FileReadWriteTestSuite:

    # >>> Test suite for IFileStorage - read/write operations, functional and stability tests
    #
    # These tests are implemented purely in terms of the IFileStorage interface. The test suite can be run for
    #     any storage implementation and a valid storage implementations must pass this test suite.
    #
    # NOTE: To test a new storage implementation, setupStorage() must be replaced
    # with a method to provide a storage implementation based on a supplied test config.
    #
    # Storage implementations may also wish to supply their own tests that use native APIs to set up and control
    # tests. This can allow for finer grained control, particularly when testing corner cases and error conditions.

    # Unit test implementation for local storage is in LocalStorageReadWriteTest

    assertEqual = unittest.TestCase.assertEqual
    assertTrue = unittest.TestCase.assertTrue
    assertFalse = unittest.TestCase.assertFalse
    assertIsNotNone = unittest.TestCase.assertIsNotNone
    assertRaises = unittest.TestCase.assertRaises
    fail = unittest.TestCase.fail

    def __init__(self):
        self.storage: _storage.IFileStorage = None  # noqa
    
    # ------------------------------------------------------------------------------------------------------------------
    # Basic round trip
    # ------------------------------------------------------------------------------------------------------------------

    def test_round_trip_basic(self):

        storage_path = "haiku.txt"

        haiku = \
            "The data goes in;\n" + \
            "For a short while it persists,\n" + \
            "then returns unscathed!"

        haiku_bytes = haiku.encode('utf-8')

        self.do_round_trip(storage_path, [haiku_bytes], self.storage)

    def test_round_trip_large(self):

        storage_path = "test_file.dat"

        # One 10 M chunk
        bytes_ = random.randbytes(10 * 1024 * 1024)

        self.do_round_trip(storage_path, [bytes_], self.storage)

    def test_round_trip_heterogeneous(self):

        storage_path = "test_file.dat"

        # Selection of different size chunks
        bytes_ = [
            random.randbytes(3),
            random.randbytes(10000),
            random.randbytes(42),
            random.randbytes(4097),
            random.randbytes(1),
            random.randbytes(2000),
        ]

        self.do_round_trip(storage_path, bytes_, self.storage)

    def test_round_trip_empty(self):

        storage_path = "test_file.dat"
        empty_bytes = bytes()

        self.do_round_trip(storage_path, [empty_bytes], self.storage)

    def do_round_trip(self, storage_path: str, original_bytes: tp.List[bytes], storage: _storage.IFileStorage):

        with storage.write_byte_stream(storage_path) as write_stream:
            for chunk in original_bytes:
                write_stream.write(chunk)

        with storage.read_byte_stream(storage_path) as read_stream:
            read_content = read_stream.read()

        original_content = functools.reduce(lambda bs, b: bs + b, original_bytes[1:], original_bytes[0])

        self.assertEqual(original_content, read_content)

    # ------------------------------------------------------------------------------------------------------------------
    # Functional error and corner cases
    # ------------------------------------------------------------------------------------------------------------------

    # Functional error tests can be set up and verified entirely using the storage API
    # All back ends should behave consistently for these tests

    def test_write_missing_dir(self):

        # Writing a file will always create the parent dir if it doesn't already exist
        # This is in line with cloud bucket semantics

        storage_path = "missing_dir/some_file.txt"
        content = "Some content".encode('utf-8')

        self.storage.write_bytes(storage_path, content)

        dir_exists = self.storage.exists("missing_dir")
        file_exists = self.storage.exists(storage_path)

        self.assertTrue(dir_exists)
        self.assertTrue(file_exists)

        dir_stat = self.storage.stat("missing_dir")
        file_stat = self.storage.stat(storage_path)

        self.assertEqual(_storage.FileType.DIRECTORY, dir_stat.file_type)
        self.assertEqual(len(content), file_stat.size)

    def test_write_file_already_exists(self):

        # Writing a file always overwrites any existing content
        # This is in line with cloud bucket semantics

        storage_path = "some_file.txt"
        content = "Some content".encode('utf-8')

        def write_to_path(path_, content_):
            with self.storage.write_byte_stream(path_) as stream:
                stream.write(content_)

        write_to_path(storage_path, content)

        exists = self.storage.exists(storage_path)
        self.assertTrue(exists)

        new_content = "Some different content".encode('utf-8')

        write_to_path(storage_path, new_content)

        read_content = self.storage.read_bytes(storage_path)
        self.assertEqual(new_content, read_content)

    def test_write_dir_already_exists(self):

        # File storage should not allow a file to be written if a dir exists with the same name
        # TRAC prohibits this even though it is allowed in pure bucket semantics

        storage_path = "some_file.txt"

        self.storage.mkdir(storage_path)

        exists = self.storage.exists(storage_path)
        self.assertTrue(exists)

        new_content = "Some different content".encode('utf-8')

        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.write_bytes(storage_path, new_content))

    def test_write_bad_paths(self):

        # \0 and / are the two characters that are always illegal in posix filenames
        # But / will be interpreted as a separator
        # There are several illegal characters for filenames on Windows!

        absolute_path = "C:\\Temp\\blah.txt" if _util.is_windows() else "/tmp/blah.txt"
        invalid_path = "£$ N'`¬$£>.)_£\"+\n%" if _util.is_windows() else "nul\0char"

        self.assertRaises(_ex.EStorageValidation, lambda: self.storage.write_byte_stream(absolute_path))
        self.assertRaises(_ex.EStorageValidation, lambda: self.storage.write_byte_stream(invalid_path))

    def test_write_storage_root(self):

        storage_path = "."

        self.assertRaises(_ex.EStorageValidation, lambda: self.storage.write_byte_stream(storage_path))

    def test_write_outside_root(self):

        storage_path = "../any_file.txt"
        storage_path_2 = "dir/../../any_file.txt"

        self.assertRaises(_ex.EStorageValidation, lambda: self.storage.write_byte_stream(storage_path))
        self.assertRaises(_ex.EStorageValidation, lambda: self.storage.write_byte_stream(storage_path_2))

    def test_read_missing(self):

        storage_path = "missing_file.txt"

        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.read_byte_stream(storage_path))

    def test_read_dir(self):

        storage_path = "not_a_file/"

        self.storage.mkdir(storage_path)

        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.read_byte_stream(storage_path))

    def test_read_bad_paths(self):

        # \0 and / are the two characters that are always illegal in posix filenames
        # But / will be interpreted as a separator
        # There are several illegal characters for filenames on Windows!

        absolute_path = "C:\\Temp\\blah.txt" if _util.is_windows() else "/tmp/blah.txt"
        invalid_path = "£$ N'`¬$£>.)_£\"+\n%" if _util.is_windows() else "nul\0char"

        self.assertRaises(_ex.EStorageValidation, lambda: self.storage.read_byte_stream(absolute_path))
        self.assertRaises(_ex.EStorageValidation, lambda: self.storage.read_byte_stream(invalid_path))

    def test_read_storage_root(self):

        storage_path = "."

        self.assertRaises(_ex.EStorageValidation, lambda: self.storage.read_byte_stream(storage_path))

    def test_read_outside_root(self):

        storage_path = "../some_file.txt"

        self.assertRaises(_ex.EStorageValidation, lambda: self.storage.read_byte_stream(storage_path))

    # ------------------------------------------------------------------------------------------------------------------
    # Interrupted operations
    # ------------------------------------------------------------------------------------------------------------------

    # Simulate error conditions in client code that interrupt read/write operations
    # Try to check that errors are reported correctly and resources are cleaned up
    # That means all handles/streams/locks are closed and for write operation partially written files are removed
    # It is difficult to verify this using the abstracted storage API!
    # These tests look for common symptoms of resource leaks that may catch some common errors
    #
    # Error states in the streams are checked directly by mocking/spying on the Java Flow API
    # E.g. Illegal state and cancellation exceptions are checked explicitly
    # This is different from the functional tests, which unwrap stream state errors to look for EStorage errors
    #
    # Using the storage API it is not possible to simulate errors that occur in the storage back end
    # E.g. loss of connection to a storage service or disk full during a write operation
    #
    # Individual storage backends should implement tests using native calls to set up and verify tests
    # This will allow testing error conditions in the storage back end,
    # and more thorough validation of error handling behavior for error conditions in client code

    class TestException(Exception):
        pass

    def test_write_error_immediately(self):

        storage_path = "test_file.dat"

        def attempt_write():
            with self.storage.write_byte_stream(storage_path):
                raise self.TestException("Error before content")

        self.assertRaises(self.TestException, attempt_write)

        # If there is a partially written file,
        # the writer should remove it as part of the error cleanup

        exists = self.storage.exists(storage_path)
        self.assertFalse(exists)

    def test_write_error_after_chunk(self):

        storage_path = "test_file.dat"
        first_chunk = "Some content\n".encode('utf-8')

        def attempt_write():
            with self.storage.write_byte_stream(storage_path) as stream:
                stream.write(first_chunk)
                raise self.TestException("Error after first chunk")

        self.assertRaises(self.TestException, attempt_write)

        # If there is a partially written file,
        # the writer should remove it as part of the error cleanup

        exists = self.storage.exists(storage_path)
        self.assertFalse(exists)

    def test_write_error_then_retry(self):

        storage_path = "test_file.dat"
        chunk_size = 10000
        chunk = random.randbytes(chunk_size)

        # Attempt a write operation, fail after sending content
        # Exception should propagate back to client code

        def failed_write():
            with self.storage.write_byte_stream(storage_path) as stream1:
                stream1.write(chunk)
                raise self.TestException("Error after first chunk")

        self.assertRaises(self.TestException, failed_write)

        # File should not exist in storage after an aborted write
        exists1 = self.storage.exists(storage_path)
        self.assertFalse(exists1)

        # Set up a second write to retry the same operation, this time successfully

        with self.storage.write_byte_stream(storage_path) as stream2:
            stream2.write(chunk)

        # File should now be visible in storage
        file_size = self.storage.size(storage_path)
        self.assertEqual(chunk_size, file_size)

    def test_read_concurrent_streams(self):

        # Allow two read streams to be opened concurrently to the same file

        storage_path = "some_file.txt"
        content = "Some content".encode('utf-8')

        self.storage.write_bytes(storage_path, content)

        with self.storage.read_byte_stream(storage_path) as stream_1:
            with self.storage.read_byte_stream(storage_path) as stream_2:
                read_result_1 = stream_1.read(1024)
                read_result_2 = stream_2.read(1024)

        self.assertEqual(content, read_result_1)
        self.assertEqual(content, read_result_2)

    def test_read_delete_while_open(self):

        storage_path = "some_file.txt"
        content = random.randbytes(64 * 1024)

        self.storage.write_bytes(storage_path, content)

        # Platforms vary in how this is handled
        # There is no easy way to force the behavior, but we can check for consistency

        with self.storage.read_byte_stream(storage_path) as stream:

            try:
                self.storage.rm(storage_path)

            # If rm fails, stream should still be open and readable
            except _ex.EStorage:
                read_content = stream.read(64 * 1024)
                self.assertEqual(content, read_content)

            # If rm succeeds, file should no longer exist in storage
            else:
                exists = self.storage.exists(storage_path)
                self.assertFalse(exists)

                # If the stream is still readable, data should not be corrupted
                # If the stream throws an error on read, that is fine
                try:
                    read_content = stream.read(64 * 1024)
                    self.assertEqual(content, read_content)
                except OSError:
                    pass

    def test_read_cancel_immediately(self):

        storage_path = "some_file.txt"
        content = "Some content".encode('utf-8')

        self.storage.write_bytes(storage_path, content)

        # Close read stream without reading - should not cause an error

        with self.storage.read_byte_stream(storage_path):
            pass

        exists = self.storage.exists(storage_path)
        self.assertTrue(exists)

    def test_read_cancel_and_retry(self):

        # Create a file big enough that it can't be read in a single chunk

        storage_path = "some_file.txt"
        content = random.randbytes(10000)

        self.storage.write_bytes(storage_path, content)

        # Close read stream without reading - should not cause an error

        with self.storage.read_byte_stream(storage_path):
            pass

        exists = self.storage.exists(storage_path)
        self.assertTrue(exists)

        # Now re-open the read stream and read in the file

        with self.storage.read_byte_stream(storage_path) as stream:
            chunks = []
            while stream.readable():
                chunk = stream.read(64 * 1024)
                if chunk is None or len(chunk) == 0:
                    break
                chunks.append(chunk)
            read_result = functools.reduce(lambda bs, b: bs + b, chunks, b"")

        self.assertEqual(content, read_result)

    def test_read_cancel_and_delete(self):

        storage_path = "some_file.txt"
        content = "Some content".encode('utf-8')

        self.storage.write_bytes(storage_path, content)

        # Close read stream without reading - should not cause an error

        with self.storage.read_byte_stream(storage_path):
            pass

        exists = self.storage.exists(storage_path)
        self.assertTrue(exists)

        # Now delete the file and try to re-open the read stream, this should fail

        self.storage.rm(storage_path)
        self.assertRaises(_ex.EStorageRequest, lambda: self.storage.read_byte_stream(storage_path))

        # Check the file is really gone

        exists2 = self.storage.exists(storage_path)
        self.assertFalse(exists2)
