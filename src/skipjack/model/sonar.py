# encoding: utf-8
from abc import ABCMeta, abstractmethod
from hadoop.hadoop_exception import HadoopException
from hadoop.hdfs import HadoopDistributedFileSystem
from hadoop.streaming import HadoopStreaming
from model.sonar_status import SonarStatus


class Sonar(HadoopDistributedFileSystem, HadoopStreaming):
    __metaclass__ = ABCMeta
    
    def __init__(self, status_class=None):
        if status_class:
            self.status_class = status_class
        else:
            self.status_class = SonarStatus

    def run(self, clean_output_directory=True):
        status = self.status_class()

        self._check()
        self._init_hadoop(status)
        while self._do_next(status):
            self._set_status_when_start(status)
            mapper, reducer = self._get_map_reduce_file(status)

            input_file, output = self._get_in_out_file(status)
            cache_files = self._get_cache_files(status)

            if clean_output_directory:
                self.__clean_output(output)
            self._run_hadoop(mapper, reducer, input_file, output, cache_files)

            self.__set_output(status, output)
            self._set_status_when_end(status)

        self._finalize_hadoop()

    def _init_hadoop(self, status):
        pass

    def _finalize_hadoop(self):
        pass
    
    def _set_status_when_start(self, start):
        pass
    
    def _set_status_when_end(self, status):
        status._SonarStatus__increment_count()
    
    def __set_output(self, status, output_directory):
        output = self.cat(output_directory + "/*")
        
        # (CRLF, CR, LF) -> (CR, LF) -> (LF)
        output = output.replace("\r\n", "\r").replace("\r", "\n").split("\n")
        status.output = output

    def __clean_output(self, output):
        if isinstance(output, str):
            try:
                self.remove_directory(output)
            except HadoopException as e:
                print(e.message)
        else:
            try:
                self.remove_directories(output)
            except HadoopException as e:
                print(e.message)

    @abstractmethod
    def _do_next(self, status):
        raise Exception("Not implemented")

    @abstractmethod
    def _get_map_reduce_file(self, status):
        raise Exception("Not implemented")

    @abstractmethod
    def _get_in_out_file(self, status):
        raise Exception("Not implemented")

    def _get_cache_files(self, status):
        """ If needed, return cache file name or list
        :param status: hadoop running status(first time is zero)
        :return:
        """
        return None