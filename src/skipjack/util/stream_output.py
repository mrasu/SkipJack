# encoding: utf-8
import json


def output_raw(text, output_io=None):
    if output_io:
        output_io.write(text + "\n")
    else:
        print(text)


def output_raw_json(value, output_io=None):
    json_string = json.dumps(value)
    output_raw(json_string, output_io=output_io)
    

def output(key, value, output_io=None):
    output_value = "{0}\t{1}".format(key, value)
    output_raw(output_value, output_io=output_io)


def output_json(key, value, output_io=None):
    json_string = json.dumps(value)
    output(key, json_string, output_io=output_io)
