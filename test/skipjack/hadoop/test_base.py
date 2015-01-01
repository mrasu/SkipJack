import os
from unittest import TestCase
from hadoop.base import HadoopBase


class TestHadoopBase(TestCase, HadoopBase):
    HADOOP_HOME = "/test/hadoop"
    HADOOP_STREAMING_PATH = "hadoop_streaming"
    
    def test_get_hadoop_path(self):
        expected = os.path.join(self.HADOOP_HOME, "bin/hadoop")
        self.assertEqual(expected, self.get_hadoop_path())


class TestHadoopBaseNoHadoopStreaming(TestCase, HadoopBase):
    HADOOP_HOME = "/test/hadoop"
    # HADOOP_STREAMING_PATH = "hadoop_streaming"

    def test_check(self):
        self.assertRaises(Exception, self._check)


class TestHadoopBaseNoHadoopHome(TestCase, HadoopBase):
    # HADOOP_HOME = "/test/hadoop"
    HADOOP_STREAMING_PATH = "hadoop_streaming"

    def test_check(self):
        self.assertRaises(Exception, self._check)
