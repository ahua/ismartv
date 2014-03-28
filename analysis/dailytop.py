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
        self.last_counts = {}

    # VOD播放次数
    @timed
    def _a(self):
        sql = """select count(*), item
                 from daily_logs where parsets = "%s"
                 and event = "video_start"
                 group by item
              """ % self.day_str
        res = DailyTop.hiveinterface.execute(sql)
        if not res:
            res = []
        for li in res:
            count, item = li.split()
            title = get_title(item)
            channel = get_channel(item)
            key = self.day_str + "_" + item
            last_count = self.get_last_count(item)
            up = "0"
            if int(count) > int(last_count):
                up = "1"
            elif int(count) < int(last_count):
                up = "-1"
            DailyTop.hbaseinterface.write(key, {"a:count": count, "a:title": title, "a:channel": channel, "a:up": up, "a:item": item})

    def get_last_count(self, item):
        if not self.last_counts:
            rowlist = DailyTop.hbaseinterface.read_all(self.last_day_str, ["a:count"])
            for r in rowlist:
                day, item = r.row.split("_")
                count = "0"
                try:
                    count = r.columns["a:count"].value
                except:
                    pass
                self.last_counts[item] = count
        return self.last_counts.get(item, "0")
        

    def execute(self):
        self._a()



TITLES = {}
def get_title(item):
    global TITLES
    if not TITLES:
        with open("./files/itemtitle.csv") as fp:
            for li in fp:
                try:
                    item, title = li.rstrip().split(",")
                    TITLES[item] = title
                except:
                    pass
    return TITLES.get(item, "-")

CHANNELS = {}
def get_channel(item):
    global CHANNELS
    if not CHANNELS:
        with open("./files/itemchannel.csv") as fp:
            for li in fp:
                try:
                    item, channel = li.rstrip().split(",")
                    CHANNELS[item] = channel
                except:
                    pass
    return CHANNELS.get(item, "-")


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
        task = DailyTop(day)
        task.execute()

if __name__ == "__main__":
    main()

