import json
from unittest import TestCase
import sys
from util.stream_input import input_tab_key_value, input_tab_key_json


class TestStreamInput(TestCase):
    def test_input(self):
        self.assertEqual(False, False)
    
    def test_input_tab_key_value(self):
        input_value = ["key\tvalue"]
        sys.stdin = input_value
        actual = [res for res in input_tab_key_value()]

        expected = [("key", "value")]
        self.assertListEqual(expected, actual)

    def test_input_tab_key_value_multiple_line(self):
        input_value = ["key\tvalue", "key2\tvalue2"]
        sys.stdin = input_value
        actual = [res for res in input_tab_key_value()]

        expected = [("key", "value"), ("key2", "value2")]
        self.assertListEqual(expected, actual)

    def test_input_tab_key_value_no_input(self):
        input_value = []
        sys.stdin = input_value
        actual = [res for res in input_tab_key_value()]

        expected = []
        self.assertListEqual(expected, actual)
        
    def test_input_tab_key_value_multiple_tab(self):
        input_value = ["key\tvalue\tvalue2"]
        sys.stdin = input_value
        actual = [res for res in input_tab_key_value()]

        expected = [("key", "value\tvalue2")]
        self.assertListEqual(expected, actual)
    
    def test_input_tab_key_json_list(self):
        input_value_object = [1, 2]
        input_value = ["key\t" + json.dumps(input_value_object)]
        sys.stdin = input_value
        actual = [res for res in input_tab_key_json()]

        expected = [("key", input_value_object)]
        self.assertListEqual(expected, actual)

    def test_input_tab_key_json_dict(self):
        input_value_object = {"dic_key": "dic_value"}
        input_value = ["key\t" + json.dumps(input_value_object)]
        sys.stdin = input_value
        actual = [res for res in input_tab_key_json()]

        expected = [("key", input_value_object)]
        self.assertListEqual(expected, actual)
