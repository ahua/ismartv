#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import datetime

from decorators import timed
from HiveInterface import HiveInterface
from HbaseInterface import HbaseInterface

import weeklysql

HIVEHOST = "hadoopsnn411"
HBASEHOST = "hadoopns410"
ONE_DAY = datetime.timedelta(days=1)

class WeeklyTask:
    hiveinterface = HiveInterface(HIVEHOST)
    hiveinterface.execute("SET mapred.job.tracker=hadoopns410:8021")
    hbaseinterface = HbaseInterface(HBASEHOST, "9090", "weekly_result")

    gameappinterface = HbaseInterface(HBASEHOST, "9090", "app_game_weekly")

    # 周的定义：上周五0点-本周四24点
    def __init__(self, day):
        self.day = day
        self.day_str = day.strftime("%Y%m%d")
        self.week = day.strftime("%w")
        
        if self.week != '5':
            raise Exception("WeeklyTask must be executed at Friday...")

        self.startday = self.day - 7 * ONE_DAY
        self.endday = self.day - ONE_DAY
        self.startday_str = self.startday.strftime("%Y%m%d")
        self.endday_str = self.endday.strftime("%Y%m%d")


    def read_hbase(self, keyprefix, colprefix="game"):
        colkeys = ["%s:value" % colprefix]
        rowlist = WeeklyTask.gameappinterface.read_all(keyprefix, colkeys)
        d = {}
        for r in rowlist:
            if colkeys[0] in r.columns:
              d[r.row[8:]] = r.columns[colkeys[0]].value
        return d

    # 周活跃用户数
    @timed
    def _a(self):
        sql = """select count(distinct sn), device 
                 from daily_logs where parsets >= "%s" and parsets <= "%s"
                 group by device
              """ % (self.startday_str, self.endday_str)
        res = WeeklyTask.hiveinterface.execute(sql)
        for li in res:
            value, device = li.split()
            key = self.day_str + device
            WeeklyTask.hbaseinterface.write(key, {"a:a": value})

    # 周VOD用户数
    @timed
    def _b(self):
        sql = """select count(distinct sn), device
                 from daily_logs where parsets >= "%s" and parsets <= "%s"
                 and event in ("video_start", "video_play_load", "video_play_start", "video_exit")
                 group by device
              """ % (self.startday_str, self.endday_str)
        res = WeeklyTask.hiveinterface.execute(sql)
        for li in res:
            value, device = li.split()
            key = self.day_str + device
            WeeklyTask.hbaseinterface.write(key, {"a:b": value})

    # 周应用用户数
    @timed
    def _c(self):
        sql = weeklysql.sql_c_format % (self.startday_str, self.endday_str)
        res = WeeklyTask.hiveinterface.execute(sql)
        for li in res:
            value, device = li.split()
            key = self.day_str + device
            WeeklyTask.hbaseinterface.write(key, {"a:c": "%s" % value})

    # 周智能用户数
    @timed
    def _d(self):
        sql = weeklysql.sql_d_format % (self.startday_str, self.endday_str)
        res = WeeklyTask.hiveinterface.execute(sql)
        for li in res:
            value, device = li.split()
            key = self.day_str + device
            WeeklyTask.hbaseinterface.write(key, {"a:d": "%s" % value})

    # 应用用户数(除去系统应用和游戏应用), group by code, title, device 
    @timed
    def _e(self):
        sql = weeklysql.sql_e_format % (self.startday_str, self.endday_str)
        res = WeeklyTask.hiveinterface.execute(sql)
        d = self.read_hbase((self.startday - ONE_DAY*7).strftime("%Y%m%d"), "app")
        lines = []
        for li in res:
            value, code, title, device = li.split()
            key = self.startday_str + code
            up = 0
            if d.has_key(code):
                try:
                    up = float(value) * 1.0 / float(d[code])
                except:
                    pass
            WeeklyTask.gameappinterface.write(key, {"app:value": value,
                                                    "app:code": code,
                                                    "app:title": title,
                                                    "app:device": device,
                                                    "app:up": str(up)})

    
    # 游戏用户数, group by (code, title, device)
    @timed
    def _f(self):
        sql = weeklysql.sql_f_format % (self.startday_str, self.endday_str)
        res = WeeklyTask.hiveinterface.execute(sql)
        d = self.read_hbase((self.startday - ONE_DAY*7).strftime("%Y%m%d"), "game")
        lines = []                 
        for li in res:
            value, code, title, device = li.split()
            key = self.startday_str + code
            up = 0
            if d.has_key(code):
                try:
                    up = float(value) * 1.0 / float(d[code])
                except:
                    pass
            WeeklyTask.gameappinterface.write(key, {"game:value": value,
                                                    "game:code": code, 
                                                    "game:title": title,
                                                    "game:device": device,
                                                    "game:up": str(up)})

    def execute(self):
        self._a()
        self._b()
        self._c()
        self._d()
        self._e()
        self._f()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        daylist = [datetime.datetime.now()]
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
        if day.strftime("%w") == '5':
            task = WeeklyTask(day)
            task.execute()
                 
