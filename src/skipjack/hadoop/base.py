# encoding: utf-8
from abc import ABCMeta, abstractmethod
import os


class HadoopBase:
    __metaclass__ = ABCMeta

    HADOOP_HOME = None

    def get_hadoop_path(self):
        hadoop_path = os.path.join(self.HADOOP_HOME, "bin/hadoop")
        return hadoop_path

    def _check(self):
        if not self.HADOOP_HOME:
            raise Exception("not set HADOOP_HOME")