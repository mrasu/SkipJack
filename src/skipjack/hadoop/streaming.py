# encoding: utf-8
from abc import ABCMeta
import os
from subprocess import Popen, PIPE
from hadoop.base import HadoopBase
from hadoop.hadoop_exception import HadoopException


class HadoopStreaming(HadoopBase):
    __metaclass__ = ABCMeta

    def _run_hadoop(self, mapper, reducer, input_file, output, cache_file=None):
        hadoop_path = self.get_hadoop_path()
        hadoop_streaming_path = self.get_streaming_path()

        general_option = self.__generate_general_commands(cache_file)
        streaming_option = ["-mapper", mapper, "-reducer", reducer,
                            "-input", input_file, "-output", output]
        commands = [hadoop_path, "jar", hadoop_streaming_path]
        commands.extend(general_option)
        commands.extend(streaming_option)

        _, err = self._communicate(commands, stdout=PIPE, stderr=PIPE)
        if err:
            print(err)

    def get_streaming_path(self):
        return os.path.join(self.HADOOP_HOME, self.HADOOP_STREAMING_PATH)

    def __generate_general_commands(self, cache_file):
        general_option = []
        if cache_file:
            if isinstance(cache_file, str):
                general_option.extend(["-files", cache_file])
            else:
                general_option.extend(["-files", ",".join(cache_file)])
        return general_option