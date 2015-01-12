from unittest import TestCase
from mock import MagicMock
from model.grid_sonar import GridSonar


class DummyGridSonar(GridSonar):
    HADOOP_HOME = "hadoop"
    HADOOP_STREAMING_PATH = "streaming"
    
    CACHE_FILE_NAME = "cache_condition.txt"
    
    def _get_next_condition(self, status):
        raise "Implement Now!"

    def _get_map_reduce_file(self, status):
        return "mapper", "reducer"

    def _get_in_out_file(self, status):
        return "in", "out"


class TestGridSonar(TestCase):
    
    def setUp(self):
        self.sonar = DummyGridSonar()
        self.sonar._check = MagicMock()
        self.sonar._HadoopBase__communicate_if_not_mock = MagicMock(return_value=(b"a", None))
        self.communicate_mock = self.sonar._HadoopBase__communicate_if_not_mock
        
    def test_check_called(self):
        self.sonar._get_next_condition = lambda status: None if status.get_count() >= 1 else [{"dummy": 1}]
        self.sonar.run()
        
        self.assertTrue(self.sonar._check.called)
    
    def test_run_count(self):
        run_count = 3

        def _get_next_condition(status):
            if status.get_count() >= run_count:
                return None
            else:
                return [{"dummy": 1}]
        self.sonar._get_next_condition = _get_next_condition
        
        self.sonar.run()

        self.assertTrue(self.sonar._check.called)
        # run, clean, cat result commands are called
        self.assertEqual(self.communicate_mock.call_count, run_count * 3)