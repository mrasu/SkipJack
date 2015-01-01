from io import StringIO
import json
from unittest import TestCase
from util.stream_output import output, output_json


class TestStreamOutput(TestCase):
    def setUp(self):
        self.out = StringIO()
        
    def get_value(self):
        return self.out.getvalue().strip("\r\n")
    
    def test_output(self):
        output("a", "b", output_io=self.out)
        
        actual_output = self.get_value()
        self.assertEqual("a\tb", actual_output)

    def test_output_multiple(self):
        outputs = [["key1", "value1"], ["key2", "value"], ["key3", "value3"]]
        
        for o in outputs:
            output(o[0], o[1], output_io=self.out)

        actual_output = self.get_value().split("\n")
        self.assertListEqual(["\t".join(expect) for expect in outputs], actual_output)
    
    def test_output_tab_include_value(self):
        output("a", "b\tc", output_io=self.out)

        actual_output = self.get_value()
        self.assertEqual("a\tb\tc", actual_output)

    def test_output_tab_no_output(self):
        output("", "", output_io=self.out)

        actual_output = self.get_value()
        self.assertEqual("\t", actual_output)
    
    def test_output_json_list(self):
        json_value = [1, 2]
        output_json("key", json_value, output_io=self.out)

        actual_output = self.get_value()
        self.assertEqual("key\t" + json.dumps(json_value), actual_output)

    def test_output_json_dictionary(self):
        json_value = {"dic_key": "dic_value"}
        output_json("key", json_value, output_io=self.out)

        actual_output = self.get_value()
        self.assertEqual("key\t" + json.dumps(json_value), actual_output)

    def test_output_json_no_input(self):
        json_value = None
        output_json("key", json_value, output_io=self.out)

        actual_output = self.get_value()
        self.assertEqual("key\t" + json.dumps(json_value), actual_output)

