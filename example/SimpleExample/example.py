# encoding: utf-8
import os
from model.sonar import Sonar


class ExampleSonar(Sonar):
    HADOOP_HOME = os.environ['HADOOP_HOME']
    HADOOP_STREAMING_PATH = "share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar"

    def __init__(self):
        Sonar.__init__(self)
        self.results = []

    def _do_next(self, status):
        if status.get_count() < 4:
            return True
        else:
            return False

    def _get_map_reduce_file(self, status):
        if status.get_count() == 0:
            return ["wordcount/wc_mapper.py", "wordcount/wc_reducer.py"]
        else:
            return ["average/ave_mapper.py", "average/ave_reducer.py"]

    def _get_in_out_file(self, status):
        if status.get_count() == 0:
            return ["input/LICENSE.txt", "outputs/wordcount"]
        else:
            return ["outputs/wordcount", "outputs/average"]

    def _get_cache_files(self, status):
        if not status.get_count() == 0:
            return "cache/separate_interval.txt"

    def _set_status_when_end(self, status):
        Sonar._set_status_when_end(self, status)
        print(status.get_count())

        separate_interval = str((status.get_count() + 1) * 5)
        f = open("cache/separate_interval.txt", "w")
        f.write(separate_interval)


if __name__ == "__main__":
    sonar = ExampleSonar()
    sonar.run()