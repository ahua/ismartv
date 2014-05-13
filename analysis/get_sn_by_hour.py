#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import datetime

from decorators import timed
from HiveInterface import HiveInterface
from HbaseInterface import HbaseInterface

#VOD用户数、播放量、VOD户均时长(分钟)、VOD户均访次、频道激活率
# 频道激活率 = 频道vod用户数 / vod总用户数

HIVEHOST = "hadoopsnn411"
HBASEHOST = "hadoopns410"
ONE_DAY = datetime.timedelta(days=1)
hiveinterface = HiveInterface(HIVEHOST)    

# VOD用户数
@timed
def calc_by_device(parsets, h):
    d = datetime.datetime.strptime(parsets, "%Y%m%d")
    start = d.replace(hour=h)
    end = d.replace(hour=h+1)
    startts = int(start.strftime("%H%M%S"))
    endts = int(end.startftime("%H%M%S"))
    sql = """select count(distinct sn), device
             from daily_logs where parsets = "%s" and ts >= %s and ts < %s
             and event in ("video_start", "video_play_load", "video_play_start", "video_exit")
             group by device
          """ % (parsets, startts, endts)
    print sql
    return 
    res = hiveinterface.execute(sql)
    if not res:
        res = []
    for li in res:
        print li,


# VOD用户数
@timed
def calc_by_device_and_channel(parsets, h):
    d = datetime.datetime.strptime(parsets, "%Y%m%d")
    start = d.replace(hour=h)
    end = d.replace(hour=h+1)
    startts = int(start.strftime("%H%M%S"))
    endts = int(end.startftime("%H%M%S"))
    sql = """select count(distinct sn), device, channel
             from daily_logs where parsets = "%s" and ts >= %s and ts < %s
             and event in ("video_start", "video_play_load", "video_play_start", "video_exit")
             group by device, channel
          """ % (parsets, startts, endts)
    print sql
    return 
    res = hiveinterface.execute(sql)
    if not res:
        res = []
    for li in res:
        print li,



startday = datetime.datetime(2014, 4, 11)
endday = datetime.datetime(2014, 5, 9)

while startday <= endday:
    parsets = startday.strftime("%Y%m%d")
    for i in range(0, 24):
        calc_by_device(parsets, i)
        calc_by_device_and_channel(parsets, i)
    startday += datetime.timedelta(days=1)
