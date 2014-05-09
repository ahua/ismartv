#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import os
import datetime

from decorators import timed
from HiveInterface import HiveInterface
from HbaseInterface import HbaseInterface

HIVEHOST = "hadoopsnn411"
HBASEHOST = "hadoopns410"
ONE_DAY = datetime.timedelta(days=1)

class DailySearch:
    hiveinterface = HiveInterface(HIVEHOST)    
    hiveinterface.execute("SET mapred.job.tracker=hadoopns410:8021")
    hbaseinterface = HbaseInterface(HBASEHOST, "9090","daily_search")

    def __init__(self, day):
        self.day = day
        self.day_str = day.strftime("%Y%m%d")
        self.last_day = day - ONE_DAY
        self.last_day_str = self.last_day.strftime("%Y%m%d")

    def save_to_hbase(self, day_str, i, c, q):
        key = "%s_%s" % (day_str, i)
        d = {"a:c": c, "a:q": q}
        DailyTask.hbaseinterface.write(key, d)

    @timed
    def _calc(self):
        cmd = """hive -e "select mediaip from daily_logs where parsets = '%s' and event = 'video_search' and length(mediaip) > 1;" > /tmp/t 2>/dev/null
              """ % self.day_str
        os.system(cmd)
        cmd = """cat /tmp/t | sort | uniq -c  | sort -n -r | head -n 30"""
        f = os.popen(cmd)
        lines = f.readlines()
        i = 0
        for li in lines:
            items = li.strip().rstrip().split()
            c = items[0]
            q = " ".join(items[1:])
            i = i + 1
            self.save_to_hbase(self.daylist, i, c, q)

    def execute(self):
        self._calc()

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
        task = DailySearch(day)
        task.execute()

if __name__ == "__main__":
    main()

