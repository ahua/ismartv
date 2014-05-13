#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import datetime

from decorators import timed
from HiveInterface import HiveInterface
from HbaseInterface import HbaseInterface

HIVEHOST = "hadoopsnn411"
HBASEHOST = "hadoopns410"
ONE_DAY = datetime.timedelta(days=1)
hiveinterface = HiveInterface(HIVEHOST)    

def save(filename, lines):
    with open(os.path.join("/var/tmp/", filename), "w") as fp:
        fp.writelines(lines)

@timed
def calc_by_device(parsets, h):
    d = datetime.datetime.strptime(parsets, "%Y%m%d")
    start = d.replace(hour=h)
    end = d.replace(hour=h+1) if h < 23 else d.replace(hour=h, minute=59, second=59)
    startts = int(start.strftime("%H%M%S"))
    endts = int(end.strftime("%H%M%S"))
    sql = """select count(distinct sn), device
             from daily_logs where parsets = "%s" and ts >= %s and ts < %s
             and event in ("video_start", "video_play_load", "video_play_start", "video_exit")
             group by device
          """ % (parsets, startts, endts)
    print sql
    filename = "device_%s_%s.log" %(parsets, h)
    res = hiveinterface.execute(sql)
    if not res:
        res = []
    save(filename, res)

# VOD用户数
@timed
def calc_by_device_and_channel(parsets, h):
    d = datetime.datetime.strptime(parsets, "%Y%m%d")
    start = d.replace(hour=h)
    end = d.replace(hour=h+1) if h< 23 else d.replace(hour=h, minute=59, second=59)
    startts = int(start.strftime("%H%M%S"))
    endts = int(end.strftime("%H%M%S"))
    sql = """select count(distinct sn), device, channel
             from daily_logs where parsets = "%s" and ts >= %s and ts < %s
             and event in ("video_start", "video_play_load", "video_play_start", "video_exit")
             group by device, channel
          """ % (parsets, startts, endts)
    print sql
    filename = "channel_%s_%s.log" % (parsets, h)
    res = hiveinterface.execute(sql)
    if not res:
        res = []
    save(filename, res)


calc_by_device("20140512", 9)
calc_by_device_and_channel("20140512", 9)

startday = datetime.datetime(2014, 4, 11)
endday = datetime.datetime(2014, 5, 9)

while startday <= endday:
    parsets = startday.strftime("%Y%m%d")
    for i in range(0, 24):
        calc_by_device(parsets, i)
        calc_by_device_and_channel(parsets, i)
    startday += datetime.timedelta(days=1)
