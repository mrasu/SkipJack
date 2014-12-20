# encoding: utf-8
from abc import ABCMeta, abstractmethod
from subprocess import Popen, PIPE
from hadoop.base import HadoopBase
from hadoop.hadoop_exception import HadoopException


class HadoopDiskFileSystem(HadoopBase):
    __metaclass__ = ABCMeta

    def cat(self, file_name):
        content, err = Popen([self.get_hadoop_path(), "fs", "-cat", file_name], stdout=PIPE).communicate()

        if err:
            raise HadoopException(str(err))
        return content

    def remove_directory(self, directory):
        hadoop_path = self.get_hadoop_path()

        _, err = Popen([hadoop_path, "fs", "-rm", "-r", directory]).communicate()
        if err:
            raise HadoopException(str(err))

    def remove_directories(self, directories):
        for directory in directories:
            self.remove_directory(directory)