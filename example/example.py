# encoding: utf-8
from model.sonar import Sonar


class ExampleSonar(Sonar):
    HADOOP_HOME = "/usr/src/hadoop-2.6.0"
    HADOOP_STREAMING_PATH = "share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar"

    def __init__(self):
        self.results = []

    def _do_next(self, count):
        if count < 4:
            return True
        else:
            return False

    def _get_map_reduce_file(self, count):
        if count == 0:
            return ["wordcount/wc_mapper.py", "wordcount/wc_reducer.py"]
        else:
            return ["average/ave_mapper.py", "average/ave_reducer.py"]

    def _get_in_out_file(self, count):
        if count == 0:
            return ["input/LICENSE.txt", "outputs/wordcount"]
        else:
            return ["outputs/wordcount", "outputs/average"]

    def _get_cache_files(self, count):
        if not count == 0:
            return "cache/separate_interval.txt"

    def _validate(self, count, output):
        output = self.cat(output + "/*")
        print(output)

        separate_interval = str((count + 1) * 5)
        f = open("cache/separate_interval.txt", "w")
        f.write(separate_interval)


if __name__ == "__main__":
    sonar = ExampleSonar()
    sonar.run()