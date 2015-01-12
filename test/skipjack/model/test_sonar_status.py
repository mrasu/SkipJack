from unittest import TestCase
from model.sonar_status import SonarStatus


class TestSonarStatus(TestCase):
    def test_init(self):
        status = SonarStatus()
        self.assertEqual(0, status.get_count())
    
    def test_increment(self):
        status = SonarStatus()
        status._SonarStatus__increment_count()
        self.assertEqual(1, status.get_count())
