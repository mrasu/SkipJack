# encoding: utf-8
from unittest import TestCase
from unittest.mock import MagicMock
from model.sonar import Sonar


class DummySonar(Sonar):
    HADOOP_HOME = "hadoop"
    HADOOP_STREAMING_PATH = "streaming"
    
    def _get_in_out_file(self, status):
        return "in", "out"

    def _get_map_reduce_file(self, status):
        return "mapper", "reducer"

    def _do_next(self, status):
        if status.get_count() < self.run_count:
            return True


class TestSonar(TestCase):
    def setUp(self):
        self.sonar = DummySonar()

    def test_run(self):
        run_count = 3
        self.sonar.run_count = run_count

        self.sonar._check = MagicMock()
        self.sonar._HadoopBase__communicate_if_not_mock = MagicMock(return_value=("a", None))
        mock = self.sonar._HadoopBase__communicate_if_not_mock

        self.sonar.run()

        self.assertTrue(self.sonar._check.called)
        # hadoop実行とアウトプットディレクトリ削除で各job2回実行される
        self.assertEqual(mock.call_count, run_count * 2)

    def test_run_no_clean(self):
        run_count = 3
        self.sonar.run_count = run_count

        self.sonar._HadoopBase__communicate_if_not_mock = MagicMock(return_value=("a", None))
        mock = self.sonar._HadoopBase__communicate_if_not_mock

        self.sonar._check = MagicMock()

        self.sonar.run(False)

        self.assertTrue(self.sonar._check.called)
        self.assertEqual(mock.call_count, run_count)
