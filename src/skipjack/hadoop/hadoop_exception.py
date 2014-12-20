# encoding: utf-8


class HadoopException(Exception):
    def __index__(self, message):
        super(HadoopException, self).__init__(message)