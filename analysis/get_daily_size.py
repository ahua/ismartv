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

HBASEHOST = "hadoopns410"
device_size_count = HbaseInterface(HBASEHOST, "9090", "device_size_count")
sn_table = HbaseInterface(HBASEHOST, "9090", "sn_table")

def save_to_hbase(device, size, count):
    key = "device_%s_%s" % (device, size)
    d = {"a:size": size, "a:count": count}
    device_size_count.write(key, d)

def process(day):
    sql = ""

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
        k = "%s_%s" % (device, size)
        if k in d:
            d[k] += 1
        else:
            d[k] = 1
    for k in d:
        device, size = k.split("_")
        p = str(round(float(d[k])/total*100, 2))
        save_to_hbase(device, size, d[k], p)

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

