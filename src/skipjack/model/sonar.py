# encoding: utf-8
from abc import ABCMeta, abstractmethod
from hadoop.hdfs import HadoopDiskFileSystem
from hadoop.streaming import HadoopStreaming


class Sonar(HadoopDiskFileSystem, HadoopStreaming):
    __metaclass__ = ABCMeta

    def run(self, clean_output_directory=True):
        count = 0

        self._check()
        self._init_hadoop()
        while self._do_next(count):
            mapper, reducer = self._get_map_reduce_file(count)

            input_file, output = self._get_in_out_file(count)
            cache_files = self._get_cache_files(count)

            if clean_output_directory:
                self.__clean_output(output)
            self._run_hadoop(mapper, reducer, input_file, output, cache_files)
            self._validate(count, output)

            count += 1

        self._finalize_hadoop()

    def _init_hadoop(self):
        pass

    def _finalize_hadoop(self):
        pass

    def _validate(self, count, output):
        pass

    def __clean_output(self, output):
        if isinstance(output, str):
            self.remove_directory(output)
        else:
            self.remove_directories(output)

    @abstractmethod
    def _do_next(self, count):
        raise Exception("Not implemented")

    @abstractmethod
    def _get_map_reduce_file(self, count):
        raise Exception("Not implemented")

    @abstractmethod
    def _get_in_out_file(self, count):
        raise Exception("Not implemented")

    def _get_cache_files(self, count):
        """ If needed, return cache file name or list
        :param count: hadoop running count(first time is zero)
        :return:
        """
        return None