# encoding: utf-8
import os
from subprocess import PIPE
from unittest import TestCase
from unittest.mock import MagicMock
from hadoop.streaming import HadoopStreaming


class DummyHadoopStreaming(HadoopStreaming):
    HADOOP_HOME = "hadoop"
    HADOOP_STREAMING_PATH = "streaming"
    

class TestStreaming(TestCase):
    def setUp(self):
        self.streaming = DummyHadoopStreaming()
    
    def test_get_streaming_path(self):
        expected = os.path.join(self.streaming.HADOOP_HOME, self.streaming.HADOOP_STREAMING_PATH)
        actual = self.streaming.get_streaming_path()
        
        self.assertEqual(expected, actual)
    
    def test_run_hadoop_no_cache(self):
        mapper = "mapper"
        reducer = "reducer"
        input_file = "input_file"
        output_file = "output_file"

        self.streaming._HadoopBase__communicate_if_not_mock = MagicMock(return_value=("a", None))
        mock = self.streaming._HadoopBase__communicate_if_not_mock

        self.streaming._run_hadoop(mapper, reducer, input_file, output_file)

        expected_commands = [
            self.streaming.get_hadoop_path(), "jar", self.streaming.get_streaming_path(),
            "-mapper", mapper,
            "-reducer", reducer,
            "-input", input_file,
            "-output", output_file
        ]
        mock.assert_called_once_with(expected_commands, stdout=PIPE, stderr=PIPE)

    def test_run_hadoop_one_cache(self):
        mapper = "mapper"
        reducer = "reducer"
        input_file = "input_file"
        output_file = "output_file"
        cache_file = "cache_file"

        self.streaming._HadoopBase__communicate_if_not_mock = MagicMock(return_value=("a", None))
        mock = self.streaming._HadoopBase__communicate_if_not_mock
    
        self.streaming._run_hadoop(mapper, reducer, input_file, output_file, cache_file=cache_file)

        expected_commands = [
            self.streaming.get_hadoop_path(), "jar", self.streaming.get_streaming_path(),
            "-files", cache_file,
            "-mapper", mapper,
            "-reducer", reducer,
            "-input", input_file,
            "-output", output_file
        ]
        mock.assert_called_once_with(expected_commands, stdout=PIPE, stderr=PIPE)

    def test_run_hadoop_multiple_cache(self):
        mapper = "mapper"
        reducer = "reducer"
        input_file = "input_file"
        output_file = "output_file"
        cache_file = ["cache_file", "cache2"]

        self.streaming._HadoopBase__communicate_if_not_mock = MagicMock(return_value=("a", None))
        mock = self.streaming._HadoopBase__communicate_if_not_mock

        self.streaming._run_hadoop(mapper, reducer, input_file, output_file, cache_file=cache_file)

        expected_commands = [
            self.streaming.get_hadoop_path(), "jar", self.streaming.get_streaming_path(),
            "-files", ",".join(cache_file),
            "-mapper", mapper,
            "-reducer", reducer,
            "-input", input_file,
            "-output", output_file
        ]
        mock.assert_called_once_with(expected_commands, stdout=PIPE, stderr=PIPE)
