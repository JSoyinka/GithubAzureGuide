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
for name in files:
    if "json.gz" in name:
        with gzip.open() as f:
            for i, line in enumerate(f):
                json_data = ujson.loads(line)
                json_file.append(json_data)
            f.close()
        new_name = name.replace("json.gz", "json")
        with open(new_name, 'w') as fp:
            json.dump(json_file, fp)
        print(new_name)

print(data)


