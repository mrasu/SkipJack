# encoding: utf-8
from abc import ABCMeta, abstractmethod
import json
from model.grid_status import GridStatus
from model.sonar import Sonar


class GridSonar(Sonar):
    __metaclass__ = ABCMeta

    CACHE_FILE_NAME = "cache/condition.txt"
    
    def __init__(self):
        Sonar.__init__(self, GridStatus)

    def _set_status_when_start(self, status):
        Sonar._set_status_when_start(self, status)
        self.__write_condition(status.condition)
    
    def _set_status_when_end(self, status):
        Sonar._set_status_when_end(self, status)
        status.condition = self._get_next_condition(status)

    @abstractmethod
    def _get_next_condition(self, status):
        raise Exception("Not implemented")

    def _do_next(self, status):
        if status.get_count() == 0 or status.condition:
            return True
        else:
            return False

    def _get_cache_files(self, count):
        return self.CACHE_FILE_NAME
    
    # At mapper-file, want enable to get condition by get_condition() or mapper(condition) or any
    #   to hide all not related to learning process
    def __write_condition(self, condition):
        f = open(self.CACHE_FILE_NAME, "w")
        f.write(json.dumps(condition))

