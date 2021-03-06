# encoding: utf-8
from abc import ABCMeta
from subprocess import PIPE
from hadoop.base import HadoopBase


class HadoopDistributedFileSystem(HadoopBase):
    __metaclass__ = ABCMeta

    def cat(self, file_name):
        content, err = self._communicate([self.get_hadoop_path(), "fs", "-cat", file_name], stdout=PIPE, stderr=PIPE)

        if err:
            print(err)
        return content

    def remove_directory(self, directory):
        hadoop_path = self.get_hadoop_path()

        _, err = self._communicate([hadoop_path, "fs", "-rm", "-r", directory], stdout=PIPE, stderr=PIPE)
        if err:
            print(err)

    def remove_directories(self, directories):
        for directory in directories:
            self.remove_directory(directory)
    
    def put(self, file_name, put_directory):
        content, err = self._communicate(
            [self.get_hadoop_path(), "fs", "-put", file_name, put_directory], stdout=PIPE, stderr=PIPE
        )

        if err:
            print(err)
        return content