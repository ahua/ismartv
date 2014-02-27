#!/usr/bin/env python
#coding:utf-8

"""
由于设备固件的缺陷
对于连续剧的连播，跳级时没有对应每集的video_exit和video_start事件上报
只有连续的video_play_load（开始播放缓冲结束）事件
因此需要对该类事件特征进行video_exit&video_start的补充,
由于最后一个
"""

import datetime
import os
import time
import shutil
import redis
import cPickle
import hashlib

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
    """
    保存filename中每行到redis的sorted set中
    """
    with open(filename) as fp:
        for li in fp:
            r = eval(li.rstrip())
            timestamp = int(time.mktime(r["time"].timetuple()))
            RCLI.zadd(DOCS_KEY, li, timestamp)

SECONDS = 24 * 60 * 60 
def get_lines():
    """
    返回最近24小时的所有log
    """
    timestamp = int(time.time())
    RCLI.zremrangebyscore(DOCS_KEY, 0, timestamp - SECONDS)
    return RCLI.zrange(DOCS_KEY, 0, -1)

def plus_exit_event(docs):
    """
    添加start和exit事件
    """
    docs = sorted(docs, key=lambda k: k["time"])
    if len(docs) <= 2:
        if len(docs) >= 1 and docs[-1]["event"] == "video_exit":
            return [str(docs[-1]) + "\n"]
        return []
    if docs[-1]["event"] != "video_exit" or docs[0]["event"] != "video_start":
        if docs[-1]["event"] == "video_exit":
            return [str(docs[-1]) + "\n"]
        return []

    k = "%s_%s_%s" % (docs[0]["sn"], docs[0]["item"], docs[0]["token"])
    RCLI.sadd(PLUSED_KEY, k)

    res = []

    # plus exit event
    idx = len(docs) - 3
    while idx >= 0 and docs[idx]["event"] == "video_play_load":
        t = docs[-1].copy()
        t['_plus'] = 1
        t['time'] = docs[idx+1]["time"]
        t['duration'] = (docs[idx+1]["time"] - docs[idx]["time"]).\
            total_seconds()
        t['_unique_key'] = hashlib.md5(cPickle.dumps(t)).hexdigest()
        res.append(str(t) + "\n")
        idx = idx - 1
    
    # plus start event
    last_start = docs[0]
    last_play_load = docs[-2]
    last_exit = docs[-1]

    idx = 2
    while idx <= len(docs) - 1 and docs[idx]["event"] == "video_play_load":
        t = docs[0].copy()
        t["_plus"] = 1
        t["time"] = docs[idx]["time"]
        t['_unique_key'] = hashlib.md5(cPickle.dumps(t)).hexdigest()
        res.append(str(t) + "\n")
        idx = idx + 1
        last_start = t

    if last_exit["duration"] <= 1 or last_exit["duration"] >= 100000:
        duration = (last_exit["time"] - last_start["time"]).total_seconds()
        last_duration = last_play_load.get("duration", 0)
        position = duration - last_duration if duration > last_duration else 0
        last_exit["duration"] = duration
        last_exit["position"] = position
    
    res.append(str(last_exit) + "\n")

    return res

def process(output_dir):
    """
    补全output目录下日志中所缺事件
    """
    lines = get_lines()
    plused_keys = RCLI.smembers(PLUSED_KEY)

    d = {}
    for li in lines:
        r = eval(li.rstrip())
        try:
            k = "%s_%s_%s" % (r["sn"], r["item"], r["token"])
            if k in plused_keys:
                continue
            if k not in d:
                d[k] = [r]
            else:
                d[k].append(r)
        except:
            continue

    res = []
    for docs in d.values():
        res = res + plus_exit_event(docs)
    
    filename = "%s.out" % datetime.datetime.now().strftime("%Y%m%d%H%M")
    with open(os.path.join(output_dir, filename), "w") as fp:
        fp.writelines(res)


def get_filelist(dirname):
    """
    返回这个目录下所有文件名
    """
    filelist = os.listdir(dirname)
    return filelist

INPUT_DIR = "/tmp/input"
OUTPUT_DIR = "/tmp/output"
USED_DIR = "/tmp/used"
def main():
    """
    main function
    """
    filelist = get_filelist(INPUT_DIR) 
    for logfile in filelist:
        save_to_redis(os.path.join(INPUT_DIR, logfile))
        shutil.move(os.path.join(INPUT_DIR, logfile),
                    os.path.join(USED_DIR, logfile))
    process(OUTPUT_DIR)

if __name__ == "__main__":
    main()
