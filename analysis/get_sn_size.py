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
sntable = HbaseInterface(HBASEHOST, "9090", "sn_table")

def exists_in_hbase(sn):
    key = "sn_%s" % (sn)
    return sntable.read(key, ["a:device"])

def save_to_hbase(sn, day_str, size):
    key = "sn_%s" % (sn)
    key1 = "%s_%s" % (day_str, sn)
    d = {"a:size": province}
    sntable.write(key, d)
    sntable.write(key1, d)

def get_sn_list_by_day(day_str):
    rowlist = sntable.read_all(day_str, ["a:device", "a:day"])
    sn_list = []
    for r in rowlist:
        day_sn = r.row 
        sn_list.append(day_sn[9:])
    return sn_list

SN_DEV_SIZE = {
    "TD01": ["S51", "42"],
    "TD02": ["S51", "47"],
    "TD03": ["S51", "55"],
    "RD01": ["S61", "42"],
    "RD02": ["S61", "47"],
    "RD03": ["S61", "55"],
    "UD01": ["S31", "39"],
    "UD02": ["S31", "50"],
    "CD01": ["K82", "60"],
    "CD02": ["LX750A", "46"],
    "CD03": ["LX750A", "52"],
    "CD04": ["LX750A", "60"],
    "CD05": ["LX850A", "60"],
    "CD06": ["LX850A", "70"],
    "CD07": ["LX850A", "80"],
    "CD08": ["K72", "60"],
    "CD09": ["DS70A", "46"],
    "CD10": ["DS70A", "52"],
    "CD11": ["DS70A", "60"],
    "CD12": ["LX755A", "46"],
    "CD13": ["LX755A", "52"],
    "CD14": ["LX755A", "60"],
    "CD15": ["UD10A", "60"],
    "CD16": ["UD10A", "70"],
    "CD17": ["LX960A", "52"],
    "CD18": ["LX960A", "60"],
    "CD19": ["LX960A", "70"],
    "TD04": ["A21", "32"],
    "TD05": ["A21", "42"],
    "TD06": ["A11", "32"],
    "TD07": ["A11", "42"]
    }

def get_size(sn):
    k = sn[0:4].upper()
    if k in SN_DEV_SIZE:
        return SN_DEV_SIZE[k][1]
    return None

def process(day):
    day_str = day.strftime("%Y%m%d")
    sn_list = get_sn_list_by_day(day_str)
    for sn in sn_list:
        size = get_size(sn)
        if size:
            save_to_hbase(sn, day_str, size)
        print sn, model, size        

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

if __name__ == "__main__":
    main()

