# encoding: utf-8
import json


def output(key, value, output_io=None):
    output_value = "{0}\t{1}".format(key, value)
    if output_io:
        output_io.write(output_value + "\n")
    else:
        print(output_value)


def output_json(key, value, output_io=None):
    json_string = json.dumps(value)
    output(key, json_string, output_io=output_io)