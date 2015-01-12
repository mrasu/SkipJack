# encoding: utf-8
from subprocess import PIPE
from unittest import TestCase
from unittest.mock import MagicMock, call
from hadoop.hadoop_exception import HadoopException
from hadoop.hdfs import HadoopDistributedFileSystem


class DummyHadoopDistributedFileSystem(HadoopDistributedFileSystem):
    HADOOP_HOME = "hadoop"
    HADOOP_STREAMING_PATH = "streaming"


class TestHDFS(TestCase):
    def setUp(self):
        self.hdfs = DummyHadoopDistributedFileSystem()
    
    def communicate_mock_call_with(self, mock, args, stdout=PIPE, stderr=PIPE):
        mock.assert_called_with(args, stdout=stdout, stderr=stderr)

    def test_cat(self):
        self.hdfs._HadoopBase__communicate_if_not_mock = MagicMock(return_value=(b"a", None))
        mock = self.hdfs._HadoopBase__communicate_if_not_mock
        self.hdfs.cat("file_name")

        expected_args = [self.hdfs.get_hadoop_path(), "fs", "-cat", "file_name"]
        self.communicate_mock_call_with(mock, expected_args)

    def test_cat_error(self):
        self.hdfs._HadoopBase__communicate_if_not_mock = MagicMock(return_value=(b"a", b"WARN errorMessage"))
        mock = self.hdfs._HadoopBase__communicate_if_not_mock
        self.assertRaises(HadoopException, self.hdfs.cat, "file_name")

        expected_args = [self.hdfs.get_hadoop_path(), "fs", "-cat", "file_name"]
        self.communicate_mock_call_with(mock, expected_args)

    def test_remove_directory(self):
        self.hdfs._HadoopBase__communicate_if_not_mock = MagicMock(return_value=(b"a", None))
        mock = self.hdfs._HadoopBase__communicate_if_not_mock
        self.hdfs.remove_directory("file_name")

        expected_args = [self.hdfs.get_hadoop_path(), "fs", "-rm", "-r", "file_name"]
        self.communicate_mock_call_with(mock, expected_args)

    def test_remove_directory_error(self):
        self.hdfs._HadoopBase__communicate_if_not_mock = MagicMock(return_value=(b"a", b"errorMessage"))
        mock = self.hdfs._HadoopBase__communicate_if_not_mock
        self.assertRaises(HadoopException, self.hdfs.remove_directory, "file_name")

        expected_args = [self.hdfs.get_hadoop_path(), "fs", "-rm", "-r", "file_name"]
        self.communicate_mock_call_with(mock, expected_args)

    def test_remove_directories(self):
        self.hdfs._HadoopBase__communicate_if_not_mock = MagicMock(return_value=(b"a", None))
        mock = self.hdfs._HadoopBase__communicate_if_not_mock
        
        target_directories = ["dir1", "dir2"]
        self.hdfs.remove_directories(target_directories)

        self.assertEqual(mock.call_count, len(target_directories))
        
        common_args = [self.hdfs.get_hadoop_path(), "fs", "-rm", "-r"]
        expected_calls = [call(common_args + [dir_name], stdout=PIPE, stderr=PIPE) for dir_name in target_directories]
        mock.assert_has_calls(expected_calls)

    def test_remove_directories_error(self):
        self.hdfs._HadoopBase__communicate_if_not_mock = MagicMock(return_value=(b"a", b"errorMessage"))
        mock = self.hdfs._HadoopBase__communicate_if_not_mock

        target_directories = ["dir1", "dir2"]
        self.assertRaises(HadoopException, self.hdfs.remove_directories, target_directories)

        common_args = [self.hdfs.get_hadoop_path(), "fs", "-rm", "-r"]
        mock.assert_called_once_with(common_args + ["dir1"], stdout=PIPE, stderr=PIPE)

    def test_remove_directories_only_one_directory(self):
        self.hdfs._HadoopBase__communicate_if_not_mock = MagicMock(return_value=(b"a", None))
        mock = self.hdfs._HadoopBase__communicate_if_not_mock

        target_directories = ["dir1"]
        self.hdfs.remove_directories(target_directories)

        self.assertEqual(mock.call_count, len(target_directories))

        common_args = [self.hdfs.get_hadoop_path(), "fs", "-rm", "-r"]
        expected_calls = [call(common_args + [dir_name], stdout=PIPE, stderr=PIPE) for dir_name in target_directories]
        mock.assert_has_calls(expected_calls)
