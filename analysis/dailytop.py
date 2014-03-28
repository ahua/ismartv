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

class DailyTop:
    hiveinterface = HiveInterface(HIVEHOST)    
    hiveinterface.execute("SET mapred.job.tracker=hadoopns410:8021")
    hbaseinterface = HbaseInterface(HBASEHOST, "9090","daily_top")

    def __init__(self, day):
        self.day = day
        self.day_str = day.strftime("%Y%m%d")
        self.last_day = day - ONE_DAY
        self.last_day_str = self.last_day.strftime("%Y%m%d")

    # VOD播放次数
    @timed
    def _a(self):
        sql = """select count(*), item
                 from daily_logs where parsets = "%s"
                 and event = "video_start"
                 group by item
              """ % self.day_str
        res = DailyTask.hiveinterface.execute(sql)
        if not res:
            res = []
        for li in res:
            value, item = li.split()
            key = self.day_str + "_" + item
            DailyTask.hbaseinterface.write(key, {"a:a": value})

    def execute(self):
        self._a()



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

