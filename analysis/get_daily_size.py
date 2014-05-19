#!/usr/bin/env python
#-*- coding: utf-8 -*-

import os
import sys
import datetime
import gzip
import redis
import json
import urllib2
from decorators import timed
from HbaseInterface import HbaseInterface
from HiveInterface import HiveInterface

HIVEHOST = "hadoopsnn411"
HBASEHOST = "hadoopns410"
hiveinterface = HiveInterface(HIVEHOST)

device_size_count = HbaseInterface(HBASEHOST, "9090", "device_size_count")
sn_table = HbaseInterface(HBASEHOST, "9090", "sn_table")

def save_to_sn_table(sn, size):
    key = "sn_%s" % (sn)
    key1 = "%s_%s" % (day_str, sn)
    d = {"a:size": size}
    sntable.write(key, d)
    sntable.write(key1, d)

def save_to_hbase(device, size, count):
    key = "device_%s_%s" % (device, size)
    d = {"a:size": size, "a:count": count}
    device_size_count.write(key, d)

def process(day):
    day_str = day.strftime("%Y%m%d")

    sql = """select distinct sn, mediaip from daily_logs
          where parsets = '%s' and event = 'video_start';
         """ % day_str
    res = hiveinterface.execute(sql)
    for li in res:
        sn, size = li.rstrip().split()
        save_to_sn_table(sn, size)

def calc_prectent():
    rowlist = sn_table.read_all("sn_", ["a:device", "a:size"])
    sn_list = []
    for r in rowlist:
        day_sn = r.row
        t = [day_sn[9:]]
        if "a:device" in r.columns and "a:size" in r.columns:
            t = t + [r.columns["a:device"].value, r.columns["a:size"].value]
            sn_list.append(t)

    total = len(sn_list)
    d = {}
    for _, device, size in sn_list:
        k = "%s:%s" % (device, size)
        if k in d:
            d[k] += 1
        else:
            d[k] = 1
    for k in d:
        device, size = k.split(":")
        p = str(round(float(d[k])/total*100, 2))
        save_to_hbase(device, size, p)

ONE_DAY = datetime.timedelta(days=1)
def main():
    if len(sys.argv) == 1:
        daylist = [datetime.datetime.now() - datetime.timedelta(days=1)]
    elif len(sys.argv) == 2:
        daylist = [datetime.datetime.strptime(sys.argv[1], "%Y%m%d")]
    else:
        startday = datetime.datetime.strptime(sys.argv[1], "%Y%m%d")
        endday = datetime.datetime.strptime(sys.argv[2], "%Y%m%d")
        daylist = []
        while startday <= endday:
            daylist.append(startday)
            startday = startday + ONE_DAY
    
    for day in daylist:
        process(day)
    calc_prectent()

if __name__ == "__main__":
    main()

