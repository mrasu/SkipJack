# encoding: utf-8
from abc import ABCMeta, abstractmethod
from hadoop.hadoop_exception import HadoopException
from hadoop.hdfs import HadoopDistributedFileSystem
from hadoop.streaming import HadoopStreaming
from model.sonar_status import SonarStatus


class Sonar(HadoopDistributedFileSystem, HadoopStreaming):
    __metaclass__ = ABCMeta

    def run(self, clean_output_directory=True):
        status = SonarStatus()

        self._check()
        self._init_hadoop()
        while self._do_next(status):
            mapper, reducer = self._get_map_reduce_file(status)

            input_file, output = self._get_in_out_file(status)
            cache_files = self._get_cache_files(status)

            if clean_output_directory:
                self.__clean_output(output)
            self._run_hadoop(mapper, reducer, input_file, output, cache_files)
            self._validate(status, output)

            status._SonarStatus__increment_count()

        self._finalize_hadoop()

    def _init_hadoop(self):
        pass

    def _finalize_hadoop(self):
        pass

    def _validate(self, status, output):
        pass

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