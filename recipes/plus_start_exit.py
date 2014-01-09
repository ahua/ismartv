#!/usr/bin/env python

"""

"""

import httplib2
import io
import json
import re
import datetime
import redis
from redis import ConnectionError

CONF = {
    "host": "localhost",
    "port": 6379,
    "db": 9
}

POOL = redis.ConnectionPool(host=CONF["host"], port=CONF["port"], db=CONF["db"])
RCLI = redis.Redis(connection_pool=POOL)

DOCS_KEY = "start_exit_docs"
PLUSED_KEY = "sn_item_token"

def save_to_redis(filename):
    with open(filename) as fp:
        for li in fp:
            r = eval(li.rstrip())
            timestamp = time.mktime(r["time"].timetuple())
            RCLI.zadd(DOCS_KEY, li, timestamp)

SECONDS = 24 * 60 * 60 
def get_lines():
    timestamp = int(time.time())
    RCLI.zremrangebyscore(DOCS_KEY, 0, tiemstamp - SECONDS)
    return RCLI.zrange(DOCS_KEY, 0, -1)

def plus_exit_event(docs):
    docs = sorted(docs, key=lambda k: k["time"])
    if len(docs) == 0:
        return []
    if docs[-1]["event"] != "video_exit":
        return []

    k = "%s_%s_%s" % (docs[0]["sn"], docs[0]["item"], docs[0]["token"])
    RCLI.sadd(PLUSED_KEY, k)
    
    if len(docs) == 2:
        return []

    res = []
    idx = len(docs) - 3
    while idx >= 0 and docs[idx]["event"] == "video_load":
        t = docs[-1].copy()
        t['plus'] = 1
        t['duration'] = (docs[idx+1]["time"] - docs[idx]["time"]).total_seconds()
        res.append(t.dumps())
    return res


def process(filename, output_dir):
    lines = get_lines()

    plused_keys = RCLI.smembers(PLUSED_KEY)

    d = {}
    for li in lines:
        r = eval(li.rstrip())
        k = "%s_%s_%s" % (r["sn"] r["item"], r["token"])
        if k in plused_keys:
            continue
        if k not in d:
            d[k] = [r]
        else:
            d[k].append(r)

    res = []
    for docs in d.values():
       res.append(plus_exit_event(docs))
    
    with open(os.path.join(output_dir, filename)) as fp:
        fp.writelines(res)


def get_filelist(dirname):
    filelist = os.listdir(dirname)
    return filelist


def main():
    INPUT_DIR= "/tmp/test"
    OUT_DIR = ""
    USEDIR = ""
    
    filelist = get_filelist(INPUT_DIR) 
    for logfile in filelist:
        save_to_redis(logfile)
        shutil.move(os.path.join(INPUT_DIR, logfile), os.path.join(USED_DIR, logfile))
    process(OUTPUT_DIR)

if __name__ == "__main__":
    main()
