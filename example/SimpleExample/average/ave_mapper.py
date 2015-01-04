#!/usr/bin/env python3
# encoding: utf-8
from util.stream_input import input_tab_key_value
from util.stream_output import output_json

f = open("cache/separate_interval.txt")
interval = int(list(f)[0])
if not interval:
    interval = 1

summation = 0
summation_count = 0
count = 0

for key, value in input_tab_key_value():
    if not count % interval == 0:
        summation += int(value)
        summation_count += 1
    count += 1

result = {"count": summation_count, "summation": summation}

output_json("1", result)