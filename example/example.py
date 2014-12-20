# encoding: utf-8
from model.sonar import Sonar


class ExampleSonar(Sonar):
    HADOOP_HOME = "/usr/src/hadoop-2.6.0"

    def __init__(self):
        self.results = []

    def _do_next(self, count):
        if count < 4:
            return True
        else:
            return False

    def _get_mapper(self, count):
        if count == 0:
            mapper_file = "wordcount/wc_mapper.py"
        else:
            mapper_file = "average/ave_mapper.py"
        return mapper_file

    def _get_reducer(self, count):
        if count == 0:
            reducer_file = "wordcount/wc_reducer.py"
        else:
            reducer_file = "average/ave_reducer.py"
        return reducer_file
    
    def _get_input_file(self, count):
        if count == 0:
            input_file = "input/LICENSE.txt"
        else:
            input_file = "outputs/wordcount"

        return input_file

    def _get_output_file(self, count):
        if count == 0:
            outputs = "outputs/wordcount"
        else:
            outputs = "outputs/average"

        return outputs

    def get_cache_files(self, count):
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