# encoding: utf-8
from abc import ABCMeta
import os
from subprocess import Popen
from hadoop.hadoop_exception import HadoopException


class HadoopBase:
    __metaclass__ = ABCMeta

    HADOOP_HOME = None
    HADOOP_STREAMING_PATH = None

    def get_hadoop_path(self):
        hadoop_path = os.path.join(self.HADOOP_HOME, "bin/hadoop")
        return hadoop_path

    def _check(self):
        if not self.HADOOP_HOME:
            raise Exception("not set HADOOP_HOME")

        if not self.HADOOP_STREAMING_PATH:
            raise Exception("not set HADOOP_STREAMING_PATH")

    def _communicate(self, *args, **kwargs):

        print("execute", args)
        content, err = self.__communicate_if_not_mock(*args, **kwargs)
        if err and " INFO " not in err:
            raise HadoopException(str(err))
        return content, err
    
    def __communicate_if_not_mock(self, args, bufsize=0, executable=None, stdin=None, stdout=None, stderr=None,
                                  preexec_fn=None, close_fds=False, shell=False, cwd=None, env=None,
                                  universal_newlines=False, startupinfo=None, creationflags=0):
        """ This method is for mock.
        Don't use any other purpose.
        """
        return Popen(args, bufsize=bufsize, executable=executable, stdin=stdin, stdout=stdout, stderr=stderr,
                                    preexec_fn=preexec_fn, close_fds=close_fds, shell=shell, cwd=cwd, env=env,
                                    universal_newlines=universal_newlines, startupinfo=startupinfo, creationflags=creationflags) \
            .communicate()