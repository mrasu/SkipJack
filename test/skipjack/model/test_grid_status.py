from unittest import TestCase
from model.grid_status import GridStatus


class TestGridStatus(TestCase):
    def test_init(self):
        status = GridStatus()
        self.assertIsNone(status.condition)
    
    def test_set_condition(self):
        status = GridStatus()
        self.assertIsNone(status.condition)
