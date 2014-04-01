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
    return time.mktime(datetime.datetime.strptime(doc['time'], "%Y-%m-%dT%H:%M:%S.%fZ").timetuple())

def plus_exit_event(docs):
    """
    添加start和exit事件,
    docs[-1] 一定是exit事件, 且docs中只能有一个video_exit
    如果倒数第二个是start事件则交换
    """
    docs[-1]["_plus"] = 2
    
    another_start = None

    if len(docs) >= 3:
        if docs[-2]["event"] == "video_start" \
                and docs[-1]["event"] == "video_exit" \
                and get_timestamp(docs[-2]) + 1 >= get_timestamp(docs[-1]) \
                and docs[-3]["item"] == docs[-1]["item"] \
                and docs[-3].get("subitem", "") == docs[-1].get("subitem", ""):
            docs[-2], docs[-1] = docs[-1], docs[-2]
            another_start = str(docs.pop(-1)) + "\n"
            print "Another_start:", another_start,


    if len(docs) == 1:
        return [str(docs[-1]) + "\n"], another_start

    docs = sorted(docs, key=lambda k: k["time"])
    # swap video_exit to docs[-1]
    num = len(docs)
    idx = num - 1
    while idx >= 0 and docs[idx]["event"] != "video_exit":
        idx = idx - 1
    if idx != num -1:
        t = docs[idx]
        docs[idx] = docs[num-1]
        docs[num-1] = t
        t = docs[idx]["time"]
        docs[idx]["time"] = docs[num-1]["time"]
        docs[num-1]["time"] = t

    res = []
    if docs[0]["event"] != "video_start":
        t = docs[0].copy()
        t["event"] = "video_start"
        t["_plus"] = 1
        t["_unique_key"] = hashlib.md5(cPickle.dumps(t)).hexdigest()
        docs.insert(0, t)
        res.append(str(t) + "\n")

    # plus exit event
    idx = len(docs) - 3
    while idx >= 0:
        if docs[idx]["event"] == "video_play_load":
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
    while idx <= len(docs) - 1:
        if docs[idx]["event"] == "video_play_load":
            t = docs[0].copy()
            t["_plus"] = 1
            t["time"] = docs[idx]["time"]
            t["clip"] = docs[idx].get("clip", -2)
            t['_unique_key'] = hashlib.md5(cPickle.dumps(t)).hexdigest()
            res.append(str(t) + "\n")
            last_start = t
        idx = idx + 1

    if last_exit["duration"] <= 1 or last_exit["duration"] >= 100000:
        duration = (last_exit["time"] - last_start["time"]).total_seconds()
        last_duration = last_play_load.get("duration", 0)
        position = duration - last_duration if duration > last_duration else 0
        last_exit["duration"] = duration
        last_exit["position"] = position
    
    res.append(str(last_exit) + "\n")

    return res, another_start

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
                    if r["event"] == "video_exit":
                        docs, another_start = get_docs(k)
                        del_from_redis(k)
                        if another_start:
                            save_to_redis(another_start)
                        res = plus_exit_event(docs)
                        fout.writelines(res)
                except Exception as e:
                    continue

    print datetime.datetime.now()
    print "All done.."


def plus_exit_event_2(docs):
    """
    添加start和exit事件,
    docs中还没有遇到video_exit事件, 返回补的事件和一条start事件
    """
    if len(docs) == 1:
        return [], None
    if len(docs) == 2 and docs[0]["event"] == "video_start":
        return [], None
    docs = sorted(docs, key=lambda k: k["time"])
    res = []
    # plus first start event.
    if docs[0]["event"] != "video_start":
        t = docs[0].copy()
        t["event"] = "video_start"
        t["_plus"] = 1
        t["_unique_key"] = hashlib.md5(cPickle.dumps(t)).hexdigest()
        docs.insert(0, t)
        res.append(str(t) + "\n")

    last_event = docs.pop(-1)
    # plus last exit event
    t = last_event.copy()
    t["event"] = "video_exit"
    t["_plus"] = 1
    t["_unique_key"] = hashlib.md5(cPickle.dumps(t)).hexdigest()
    t["duration"] = (last_event["time"] - docs[-1]["time"]).total_seconds()
    docs.append(t)
    res.append(str(t) + "\n")

    # plus exit event
    idx = len(docs) - 3
    while idx >= 0:
        if docs[idx]["event"] == "video_play_load":
            t = docs[-1].copy()
            t['_plus'] = 1
            t['time'] = docs[idx+1]["time"]
            t['duration'] = (docs[idx+1]["time"] - docs[idx]["time"]).\
                total_seconds()
            t['_unique_key'] = hashlib.md5(cPickle.dumps(t)).hexdigest()
            res.append(str(t) + "\n")
        idx = idx - 1
    
    # plus start event
    idx = 2
    while idx <= len(docs) - 1:
        if docs[idx]["event"] == "video_play_load":
            t = docs[0].copy()
            t["_plus"] = 1
            t["time"] = docs[idx]["time"]
            t["clip"] = docs[idx].get("clip", -2)
            t['_unique_key'] = hashlib.md5(cPickle.dumps(t)).hexdigest()
            res.append(str(t) + "\n")
            last_start = t
        idx = idx + 1
            
    return res, last_event

def process_all_in_redis(outfile):
    key_list = get_key_list()
    with open(outfile, "a") as fout:
        for key in key_list:
            docs = get_docs(key)
            res, last_event = plus_exit_event_2(docs)
            fout.writelines(res)
            if last_event:
                del_from_redis(key)
                save_to_redis(key, last_event)

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

    h = int(starttime.strftime("%H"))
    if h > 2 and h < 4:
        process_all_in_redis(outfile)


if __name__ == "__main__":
    main()

