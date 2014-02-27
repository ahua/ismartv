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
    
    # 累计用户数
    @timed
    def _a(self):
        sql = """select count(distinct sn), device 
                 from daily_logs where parsets <= '%s'
                 group by device
              """ % self.day_str
        res = DailyTask.hiveinterface.execute(sql)
        if not res:
            res = []
        for li in res:
            value, device = li.split()
            key = self.day_str + device
            DailyTask.hbaseinterface.write(key, {"a:a": value})

    # 新增用户数
    @timed
    def _b(self):
        sql = """select distinct device from daily_logs 
                 where parsets = '%s'
              """ % self.day_str
        res = DailyTask.hiveinterface.execute(sql)
        if not res:
            res = []
        for device in res:
            x = DailyTask.hbaseinterface.read(self.day_str + device, ["a:a"])
            y = DailyTask.hbaseinterface.read(self.last_day_str + device, ["a:a"])
            if x and y:
                z = int(x.columns["a:a"].value) - int(y.columns["a:a"].value)
            elif x and not y:
                z = int(x.columns["a:a"].value)
            elif not x and y:
                z = 0
            else:
                z = 0
            DailyTask.hbaseinterface.write(self.day_str + device, {"a:b": str(z)})
            
    # 活跃用户数
    @timed
    def _c(self):
        sql = """select count(distinct sn), device 
                 from daily_logs where parsets = '%s'
                 group by device
              """ % self.day_str
        res = DailyTask.hiveinterface.execute(sql)
        if not res:
            res = []
        for li in res:
            value, device = li.split()
            key = self.day_str + device
            DailyTask.hbaseinterface.write(key, {"a:c": value})

    # VOD用户数
    @timed
    def _d(self):
        sql = """select count(distinct sn), device
                 from daily_logs where parsets = '%s'
                 and event in ("video_start", "video_play_load", "video_play_start", "video_exit")
                 group by device
              """ % self.day_str
        res = DailyTask.hiveinterface.execute(sql)
        if not res:
            res = []
        for li in res:
            value, device = li.split()
            key = self.day_str + device
            DailyTask.hbaseinterface.write(key, {"a:d": value})


    # VOD播放次数
    @timed
    def _e(self):
        # device in ("K", "S"), event = "video_play_load"
        sql = """select count(*), device
                 from daily_logs where parsets = '%s'
                 and event = "video_play_load"
                 group by device
              """ % self.day_str
        res1 = DailyTask.hiveinterface.execute(sql)
        if not res1:
            res1 = []
        for li in res1:
            value, device = li.split()
            key = self.day_str + device
            DailyTask.hbaseinterface.write(key, {"a:e": value})

        # device in ("A"), event = "video_start"
        sql = """select count(*), device
                 from daily_logs where parsets = '%s'
                 and event = "video_start"
                 group by device
              """ % self.day_str
        res2 = DailyTask.hiveinterface.execute(sql)
        if not res2:
            res2 = []
        for li in res2:
            value, device = li.split()
            key = self.day_str + device
            DailyTask.hbaseinterface.write(key, {"a:e": value})

    # VOD用户播放总时长
    @timed
    def _f(self):
        sql = """select sum(duration), device
                 from daily_logs where parsets = '%s'
                 and event = "video_exit"
                 group by device
              """ % self.day_str
        res = DailyTask.hiveinterface.execute(sql)
        if not res:
            res = []
        for li in res:
            value, device = li.split()
            key = self.day_str + device
            DailyTask.hbaseinterface.write(key, {"a:f": value})

    # 应用激活率
    @timed
    def _g(self):
        sql = dailysql.sql_g_format % self.day_str
        res = DailyTask.hiveinterface.execute(sql)
        if not res:
            res = []
        for li in res:
            value, device = li.split()
            key = self.day_str + device
            DailyTask.hbaseinterface.write(key, {"a:g": "%s" % value})


    # 智能激活率
    @timed
    def _h(self):
        sql = dailysql.sql_h_format % self.day_str
        res = DailyTask.hiveinterface.execute(sql)
        if not res:
            res = []
        for li in res:
            value, device = li.split()
            key = self.day_str + device
            DailyTask.hbaseinterface.write(key, {"a:h": "%s" % value})

    @timed
    def _i(self):
        sql = dailysql.sql_i_format % self.day_str
        res = DailyTask.hiveinterface.execute(sql)
        if not res:
            res = []
        for li in res:
            value, device = li.split()
            key = self.day_str + device
            DailyTask.hbaseinterface.write(key, {"a:i": "%s" % value})


    def execute(self):
        self._a()
        self._b()
        self._c()
        self._d()
        self._e()
        self._f()
        self._g()
        self._h()
        self._i()

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
                                           
