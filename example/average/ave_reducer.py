#!/usr/bin/env python3
# encoding: utf-8
from __future__ import division
from util.stream_input import input_tab_key_json
from util.stream_output import output

summation = 0
count = 0
for key, count_dict in input_tab_key_json():
    summation += count_dict["summation"]
    count += count_dict["count"]

average = summation / count if not count == 0 else 0

output("result", str(average))
