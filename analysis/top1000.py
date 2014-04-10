#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import datetime

from decorators import timed
from HiveInterface import HiveInterface

HIVEHOST = "hadoopsnn411"
ONE_DAY = datetime.timedelta(days=1)

hiveinterface = HiveInterface(HIVEHOST)    
hiveinterface.execute("SET mapred.job.tracker=hadoopns410:8021")

def get_top(day_str):
    sql = """select count(*), item
                 from daily_logs where parsets >= "%s" and item >= 0
                 and event = "video_start"
                 group by item
              """ % day_str
    res = hiveinterface.execute(sql)
    if not res:
        res = []
    d = {}
    for li in res:
        count, item = li.split()
        if item == "-1":
            continue
        channel = get_channel(item)
        if channel == "-":
            continue
        
        if channel not in d:
            d[channel] = [[item,count]]
        else:
            d[channel].append([item,count])

    allcat = []
    for channel in d:
        d[channel] = sorted(d[channel], key=lambda t: int(t[1]), reverse=True)[0:1000]
        allcat += d[channel]
    allcat = sorted(allcat, key=lambda t: int(t[1]), reverse=True)[0:1000]
    
    with open("/var/tmp/%s.csv" % "allcat" , "w") as fp:
        idx = 0
        for item, count in allcat:
            idx += 1
            title = get_description_picture(item)
            fp.write("%s,%s,%s\n"%(idx, item, title))
    
    for channel in d:
        if channel == "-": continue
        
        with open("/var/tmp/%s.csv" % channel , "w") as fp:
            idx = 0
            for item, count in d[channel]:
                idx += 1
                title = get_description_picture(item)
                fp.write("%s,%s,%s\n"%(idx, item, title))
                if idx >= 100:
                    break
import urllib2
import json
def get_description_picture(item):
    url = "http://cord.tvxio.com/api/item/%s/" % item
    s = urllib2.urlopen(url).read()
    j = json.loads(s)
    return j["title"].encode("utf8")

TITLES = {}
def get_title(item):
    global TITLES
    if not TITLES:
        with open("./files/itemtitle.csv") as fp:
            for li in fp:
                try:
                    item, title = li.rstrip().split(",")
                    TITLES[item] = title
                except:
                    pass
    return TITLES.get(item, "-")

CHANNELS = {}
def get_channel(item):
    global CHANNELS
    if not CHANNELS:
        with open("./files/itemchannel.csv") as fp:
            for li in fp:
                try:
                    item, channel = li.rstrip().split(",")
                    CHANNELS[item] = channel
                except:
                    pass
    return CHANNELS.get(item, "-")

def main():
    day = datetime.datetime.now() - datetime.timedelta(days=6)
    get_top(day.strftime("%Y%m%d"))

if __name__ == "__main__":
    main()

