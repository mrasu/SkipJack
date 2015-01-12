# encoding: utf-8
import json
import os
from model.grid_sonar import GridSonar


class GridSearch(GridSonar):
    HADOOP_HOME = os.environ['HADOOP_HOME']
    HADOOP_STREAMING_PATH = "share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar"

    MAX_GAMMA = 1
    MIN_GAMMA = 0.001
    MAX_C = 1000
    MIN_C = 200
    
    CONDITION_DIVIDE_COUNT = 5
    
    GRID_SEARCH_COUNT = 5

    def __init__(self):
        GridSonar.__init__(self)
        self.previous_tops = []
        
    def _init_hadoop(self, status):
        # GridSearch.data_creator.create_input_data()
        # self.put("data.txt", "input/data.txt")
        status.condition = self.__create_condition(self.MIN_GAMMA, self.MAX_GAMMA, self.MIN_C, self.MAX_C)
        
    def _get_map_reduce_file(self, count):
        return ["mapper.py", "reducer.py"]

    def _get_in_out_file(self, count):
        return ["input/data.txt", "outputs/grid_search"]

    def _get_next_condition(self, status):
        search_result = self.previous_tops + [json.loads(row) for row in status.output if not len(row) == 0]
        tops = sorted(search_result, key=lambda x: x["mean"])[:4]
        print(tops)

        if status.get_count() > self.GRID_SEARCH_COUNT:
            self.best_match = tops[0]
            return None

        div_count = self.CONDITION_DIVIDE_COUNT
        gamma_diff = ((self.MAX_GAMMA - self.MIN_GAMMA) / div_count / status.get_count() / 2) + self.MIN_GAMMA
        c_diff = ((self.MAX_C - self.MIN_C) / div_count / status.get_count() / 2) + self.MIN_C
        
        next_conditions = [
            {"gamma": condition["gamma"], "C": condition["C"]}
            for top in tops 
            for condition in self.__create_condition(
                top["gamma"] - gamma_diff, top["gamma"] + gamma_diff,
                top["C"] - c_diff, top["C"] + c_diff
            )
        ]
        self.previous_tops = tops
        return next_conditions
    
    def __create_condition(self, min_gamma, max_gamma, min_c, max_c):
        count = self.CONDITION_DIVIDE_COUNT
        gamma_list = [min_gamma + ((max_gamma - min_gamma) / count * a) for a in range(count)]
        c_list = [(max_c - min_c) / count * (a + 1) for a in range(count)]
        
        return [{"gamma": gamma, "C": c} for gamma in gamma_list for c in c_list]

if __name__ == "__main__":
    sonar = GridSearch()
    sonar.run()
    print("BEST MATCH: " + str(sonar.best_match))