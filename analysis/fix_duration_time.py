#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import datetime

from decorators import timed
from HiveInterface import HiveInterface
from HbaseInterface import HbaseInterface

import dailysql


HIVEHOST = "hadoopsnn411"
HBASEHOST = "hadoopns410"
ONE_DAY = datetime.timedelta(days=1)

class DailyTask:
    hiveinterface = HiveInterface(HIVEHOST)    
    hiveinterface.execute("SET mapred.job.tracker=hadoopns410:8021")
    hbaseinterface = HbaseInterface(HBASEHOST, "9090","daily_result")

    def __init__(self, day):
        self.day = day
        self.day_str = day.strftime("%Y%m%d")
        self.last_day = day - ONE_DAY
        self.last_day_str = self.last_day.strftime("%Y%m%d")
        #self.hiveinterface.execute("SET mapred.job.tracker=hadoopns410:8021")
    

    # VOD用户播放总时长
    @timed
    def _f(self):
        sql = """select sum(duration), device
                 from daily_logs where parsets = '%s'
                 and event = "video_exit" and duration <= 100000
                 group by device
              """ % self.day_str
        res = DailyTask.hiveinterface.execute(sql)
        if not res:
            res = []
        for li in res:
            value, device = li.split()
            key = self.day_str + device
            DailyTask.hbaseinterface.write(key, {"a:f": value})


    def execute(self):
        self._f()


if __name__ == "__main__":
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
                                           
