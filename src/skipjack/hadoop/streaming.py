# encoding: utf-8
from abc import ABCMeta
import os
from subprocess import Popen
from hadoop.base import HadoopBase
from hadoop.hadoop_exception import HadoopException


class HadoopStreaming(HadoopBase):
    __metaclass__ = ABCMeta

    HADOOP_STREAMING_PATH = None

    def _run_hadoop(self, mapper, reducer, input_file, output, cache_file):
        hadoop_path = self.get_hadoop_path()
        hadoop_streaming_path = self.get_streaming_path()

        general_option = self.__generage_general_commands(cache_file)
        streaming_option = ["-mapper", mapper, "-reducer", reducer,
                            "-input", input_file, "-output", output]
        commands = [hadoop_path, "jar", hadoop_streaming_path]
        commands.extend(general_option)
        commands.extend(streaming_option)

        print("execute", commands)
        _, err = Popen(commands).communicate()
        if err:
            raise HadoopException(str(err))

    def get_streaming_path(self):
        if self.HADOOP_STREAMING_PATH:
            hadoop_streaming_path = self.HADOOP_STREAMING_PATH
        else:
            hadoop_streaming_path = os.path.join(self.HADOOP_HOME, "share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar")

        return hadoop_streaming_path

    def __generage_general_commands(self, cache_file):
        general_option = []
        if cache_file:
            if isinstance(cache_file, basestring):
                general_option.extend(["-files", cache_file])
            else:
                general_option.extend(["-files", ",".join(cache_file)])
        return general_option