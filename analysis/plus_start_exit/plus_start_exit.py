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
import sys

reload(sys)
sys.setdefaultencoding('utf8')

CONF = {
    "host": "localhost",
    "port": 6379,
    "db": 9
}

POOL = redis.ConnectionPool(host=CONF["host"], port=CONF["port"], db=CONF["db"])
RCLI = redis.Redis(connection_pool=POOL)
SECONDS = 36 * 60 * 60 

def save_to_redis(k, li):
    """
    保存li到redis的sorted set中
    """
    if RCLI.exists(k):
        RCLI.rpush(k, li)
    else:
        RCLI.rpush(k, li)
        RCLI.expire(k, SECONDS)

def get_key_list():
    return RCLI.keys()

def get_docs(k):
    """
    返回最近36小时的所有log
    """
    lines = RCLI.lrange(k, 0, -1)
    docs = []
    for li in lines:
        docs.append(eval(li.rstrip()))
    return docs
        
def del_from_redis(k):
    """
    删除log
    """
    return RCLI.delete(k)

def get_timestamp(doc):
    return time.mktime(doc['time'].timetuple())

def plus_exit_event(docs):
    """
    函数作用:
    1.对play_load事件添加start和exit事件, _plus = 1
    2.修改exit的duration, 以及 _plus = 2
    3.所有原始log _plus = 5

    算法:
    两个连续到事件类型组合:
    start, start
    start, exit   # 如果是在同一秒, 则交换顺序.
    start, play_load
    play_load, start
    play_load, play_load
    play_load, exit
    exit, start
    exit, play_load
    exit, exit
    """
    docs = sorted(docs, key=lambda k: k["time"])
    up = len(docs) - 1
    idx = 1
    while idx <= up:
        if docs[idx-1]["event"] == "video_start" \
                and docs[idx]["event"] == "video_exit" \
                and get_timestamp(docs[idx-1]) + 1 >= get_timestamp(docs[idx]) \
                and docs[idx-1]["item"] != docs[idx]["item"]:
            docs[idx-1], docs[idx] = docs[idx], docs[idx-1]
            print docs[idx-2] if idx >= 2 else "none"
            print docs[idx-1]
            print docs[idx]
            print docs[idx+1] if idx + 1 <= up else "none"
            print "\n" * 3
        idx += 1

    res = []
    idx = 0
    last_start = None
    last_exit = None
    while idx <= up:
        if docs[idx]["event"] == "video_start":
            last_start = docs[idx]
        elif docs[idx]["event"] == "video_exit":
            last_exit = docs[idx]
            # change duration and add _plus=2
            t = docs[idx].copy()
            t["_plus"] = 2
            if t["duration"] <= 1 or t["duration"] >= 100000:
                if idx >= 1:
                    duration = (docs[idx]["time"] - docs[idx-1]["time"]).total_seconds()
                    position = duration
                    t["duration"] = duration if duration >= 0 and duration < 100000 else 0
                    t["position"] = t["duration"]
                else:
                    t["duration"] = 0
                    t["position"] = 0
            res.append(str(t) + "\n")
        elif docs[idx]["event"] == "video_play_load":
            if idx <= 0 or docs[idx-1]["event"] != "video_start":
                # plus start event
                t = None
                if last_start:
                    t = last_start.copy()
                else:
                    t = docs[idx].copy()
                    t["event"] = "video_start"
                t["_plus"] = 1
                t["time"] = docs[idx]["time"]
                t["clip"] = docs[idx].get("clip", -2)
                t['_unique_key'] = hashlib.md5(cPickle.dumps(t)).hexdigest()
                res.append(str(t) + "\n")
            if idx <= up - 1 and docs[idx+1]["event"] != "video_exit":
                # plus exit event
                t = None
                if last_exit:
                    t = last_exit.copy()
                else:
                    t = docs[idx].copy()
                t['_plus'] = 1
                t['time'] = docs[idx+1]["time"]
                t['duration'] = (docs[idx+1]["time"] - docs[idx]["time"]).\
                    total_seconds()
                t['_unique_key'] = hashlib.md5(cPickle.dumps(t)).hexdigest()
                res.append(str(t) + "\n")
        idx += 1

    if docs[up]["event"] == "video_start" or docs[up]["event"] == "video_play_load":
        return res, docs[up]
    else:
        return res, None


def process(logfile, outfile):
    """
    补全output目录下日志中所缺事件
    """
    print datetime.datetime.now()
    with open(logfile) as fin:
        with open(outfile, "a") as fout:
            for li in fin:
                try:
                    r = eval(li.rstrip())                    
                    # item 一组连续剧标志
                    # clip 视频唯一id
                    k = "%s_%s_%s" % (r.get("sn", "0"), r.get("item", "0"), r.get("token", "0"))
                    save_to_redis(k, li)
                except Exception as e:
                    continue

    print datetime.datetime.now()
    print "save to redis %s.." % logfile


def process_all_in_redis(outfile):
    key_list = get_key_list()
    with open(outfile, "a") as fout:
        for key in key_list:
            docs = get_docs(key)
            res, last_event = plus_exit_event(docs)
            fout.writelines(res)
            del_from_redis(key)
            if last_event:
                save_to_redis(key, str(last_event) + "\n")

def get_filelist(input_dir):
    """
    返回这个目录下所有文件名
    """
    filelist = os.listdir(input_dir)
    return [os.path.join(input_dir, logfile) for logfile in filelist]

def main():
    """
    main function
    """
    OUTPUT_DIR = "/var/tmp/output"
    USED_DIR = "/var/tmp/used"
    INPUT_DIR = "/var/tmp/input"

    filelist = get_filelist(INPUT_DIR) 
    starttime = datetime.datetime.now()
    ymdhm = starttime.strftime("%Y_%m_%d_%H_%M")

    #OUTPUT_DIR = os.path.join(OUTPUT_DIR, ymdhm)
    USED_DIR = os.path.join(USED_DIR, ymdhm)
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)
    if not os.path.exists(USED_DIR):
        os.mkdir(USED_DIR)

    outfile = os.path.join(OUTPUT_DIR, ymdhm + ".txt")
    for logfile in filelist:
        process(logfile, outfile)
        shutil.move(logfile, USED_DIR)

    process_all_in_redis(outfile)


if __name__ == "__main__":
    main()

