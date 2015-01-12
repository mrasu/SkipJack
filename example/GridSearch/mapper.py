#!/usr/bin/env python3
import json
import sys
from util.stream_output import output

# This file comes from GridSearch#CACHE_FILE_NAME
# It will no need to identify filename for future
f = open("cache/condition.txt")
condition = json.loads((list(f))[0])

for value in sys.stdin:
    for i in range(len(condition)):
        # distribute all data to all reducer
        output(str(i), value.replace("\r", "").replace("\n", ""))
