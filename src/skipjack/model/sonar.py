# encoding: utf-8
from abc import ABCMeta, abstractmethod
from hadoop.hdfs import HadopDiskFileSystem
from hadoop.streaming import HadoopStreaming


class Sonar(HadopDiskFileSystem, HadoopStreaming):
    __metaclass__ = ABCMeta

    def run(self, clean_output_directory=True):
        count = 0

        self._check()
        self._init_hadoop()
        while self._do_next(count):
            mapper = self._get_mapper(count)
            reducer = self._get_reducer(count)
            input_file = self._get_input_file(count)
            output = self._get_output_file(count)

            cache_files = self.get_cache_files(count)

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
    def _get_mapper(self, count):
        raise Exception("Not implemented")

    @abstractmethod
    def _get_reducer(self, count):
        raise Exception("Not implemented")

    @abstractmethod
    def _get_input_file(self, count):
        raise Exception("Not implemented")

    @abstractmethod
    def _get_output_file(self, count):
        raise Exception("Not implemented")

    def get_cache_files(self, count):
        """ If needed, return cache file name or list
        :param count: hadoop running count(first time is zero)
        :return:
        """
        pass