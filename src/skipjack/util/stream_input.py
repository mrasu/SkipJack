# encoding: utf-8
import json
import sys


def input_tab_key_value():
    for line in sys.stdin:
        yield line.split("\t", 1)


def input_tab_key_json():
    for key, json_string in input_tab_key_value():
        value = json.loads(json_string)
        yield key, value