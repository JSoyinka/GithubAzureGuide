# Do it alkl
import argparse
import datetime
import gzip
import hashlib
import json
import logging
import os
import queue
import re
import requests
import sys
import threading
import urllib3

import mysql.connector
from mysql.connector import errorcode

from grimoirelab_toolkit.datetime import datetime_utcnow


def convert_json(archive_path):
    with gzip.uncompress(io.BytesIO(archive_path), 'r') as content:
        for i, line in enumerate(f):
            decoded = line.decode("utf-8")
            delimited = re.sub(r'}{"(?!\W)', '}JSONDELIMITER{"', decoded)
            for chunk in delimited.split('JSONDELIMITER'):
                json_file.append(ujson.loads(line))
            f.close()
    new_name = name.replace("json.gz", "json")
    with open(new_name, 'w') as fp:
        json.dump(json_file, fp, indent=2)
        fp.close()
    if os.path.exists(archive_path):
        os.remove(archive_path)
        logging.debug("Thread %s: archive %s deleted", self.getName(), archive_path)
