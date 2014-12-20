#!/usr/bin/env python3
# encoding: utf-8

import sys

for l in sys.stdin:
    for word in l.strip().split(): print('{0}\t1'.format(word))
