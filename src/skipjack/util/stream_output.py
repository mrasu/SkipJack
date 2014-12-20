# encoding: utf-8
import json


def output(key, value):
    print("{0}\t{1}".format(key, value))


def output_json(key, value):
    json_string = json.dumps(value)
    output(key, json_string)