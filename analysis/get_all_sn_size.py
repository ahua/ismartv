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

def get_sn_list_by_day(day_str):
    rowlist = sntable.read_all(day_str, ["a:device", "a:size"])
    sn_list = []
    for r in rowlist:
        day_sn = r.row 
        sn_list.append(day_sn[9:])
    return sn_list

def process(day):
    day_str = day.strftime("%Y%m%d")
    sn_list = get_sn_list_by_day(day_str)


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

