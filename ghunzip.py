import ujson
import gzip
import matplotlib
import pandas
import os
import re
import time
import json

files = os.listdir()
types = []
json_file = []
# for name in files:
#     if "json.gz" in name:
with gzip.open("2016-01-01-0.json.gz") as f:
    for i, line in enumerate(f):
        json_file.append(ujson.loads(line))
    f.close()
# new_name = name.replace("json.gz", "json")
with open("2016-01-01-0.json", 'w') as fp:
    json.dump(json_file, fp, indent=2)
# print(new_name)

# print(data)
