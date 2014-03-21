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

class DailyTask:
    hiveinterface = HiveInterface(HIVEHOST)    
    hiveinterface.execute("SET mapred.job.tracker=hadoopns410:8021")
    hbaseinterface = HbaseInterface(HBASEHOST, "9090","daily_channel")

    def __init__(self, day):
        self.day = day
        self.day_str = day.strftime("%Y%m%d")
        self.last_day = day - ONE_DAY
        self.last_day_str = self.last_day.strftime("%Y%m%d")


    # VOD用户数
    @timed
    def _a(self):
        sql = """select count(distinct sn), device, channel
                 from daily_logs where parsets = "%s"
                 and event in ("video_start", "video_play_load", "video_play_start", "video_exit")
                 group by device, channel
              """ % self.day_str
        res = DailyTask.hiveinterface.execute(sql)
        if not res:
            res = []
        for li in res:
            value, device, channel = li.split()
            key = self.day_str + device + channel
            DailyTask.hbaseinterface.write(key, {"a:a": value})

    # VOD播放次数
    @timed
    def _b(self):
        sql = """select count(*), device, channel
                 from daily_logs where parsets = "%s"
                 and event = "video_start"
                 group by device, channel
              """ % self.day_str
        res = DailyTask.hiveinterface.execute(sql)
        if not res:
            res2 = []
        for li in res:
            value, device, channel = li.split()
            key = self.day_str + device + channel
            DailyTask.hbaseinterface.write(key, {"a:b": value})

    # VOD用户播放总时长
    @timed
    def _c(self):
        sql = """select sum(duration), device, channel
                 from daily_logs where parsets = "%s"
                 and event = "video_exit" and duration > 0 and isplus != 2
                 group by device, channel
              """ % self.day_str
        res = DailyTask.hiveinterface.execute(sql)
        if not res:
            res = []
        for li in res:
            value, device, channel = li.split()
            key = self.day_str + device + channel
            DailyTask.hbaseinterface.write(key, {"a:c": value})

    def execute(self):
        self._a()
        self._b()
        self._c()


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
        task = DailyTask(day)
        task.execute()

if __name__ == "__main__":
    main()

