#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import datetime

from decorators import timed
from HiveInterface import HiveInterface
from HbaseInterface import HbaseInterface


HOST = "hadoopns410"
ONE_DAY = datetime.timedelta(days=1)

class WeeklyTask:
    hiveinterface = HiveInterface(HOST)    
    hbaseinterface = HbaseInterface(HOST, "9090","weekly_result")

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

    # 周活跃用户数
    @timed
    def _a(self):
        sql = """select count(distinct sn), device 
                 from daily_logs where d >= %s and d <= %s
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
                 from daily_logs where d >= %s and d <= %s
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
        sql = """select count(distinct sn), device
                 from daily_logs where d >= %s and d <= %s
                 and event = "app_start"
                 and code not in  ("-",
                                 'com.lenovo.oobe',
                                 'com.lenovo.dll.nebula.vod',
                                 'com.lenovo.nebula.packageinstaller',
                                 'com.lenovo.nebula.settings',
                                 'com.lenovo.nebula.app',
                                 'com.lenovo.nebula.recovery',
                                 'com.lenovo.dll.nebula.launcher',
                                 'com.lenovo.nebula.local.player.video',
                                 'com.lenovo.nebula.local.player.music',
                                 'com.lenovo.vod.player',
                                 'com.android.settings',
                                 'com.lenovo.leos.pushengine',
                                 'com.android.systeminfo',
                                 'com.lenovo.ChangeServerAddress',
                                 'wnc.w806.engineermode',
                                 'com.lenovo.tv.freudsettings',
                                 'com.lenovo.service',
                                 'com.lenovo.leyun',
                                 'com.lenovo.nebula.Launcher',
                                 'com.baidu.input.oem',
                                 'com.lenovo.nebula.local.player.image',
                                 'com.lenovo.dc',
                                 'com.android.quicksearchbox',
                                 'com.iflytek.speechservice',
                                 'com.lenovo.nebula.weibo',
                                 'com.chinatvpay',
                                 'com.lenovo.leos.pay')
                 group by device
              """ % (self.startday_str, self.endday_str)
        res = WeeklyTask.hiveinterface.execute(sql)
        for li in res:
            value, device = li.split()
            key = self.day_str + device
            WeeklyTask.hbaseinterface.write(key, {"a:c": "%s" % value})

    # 周智能用户数
    @timed
    def _d(self):
        sql = """select count(distinct sn), device
                 from daily_logs where d >= %s and d <= %s
                 and event in ("video_start", "video_play_load", "video_play_start", "video_exit", "app_start")
                 and code not in ("-",
                                 'com.lenovo.oobe',
                                 'com.lenovo.dll.nebula.vod',
                                 'com.lenovo.nebula.packageinstaller',
                                 'com.lenovo.nebula.settings',
                                 'com.lenovo.nebula.app',
                                 'com.lenovo.nebula.recovery',
                                 'com.lenovo.dll.nebula.launcher',
                                 'com.lenovo.nebula.local.player.video',
                                 'com.lenovo.nebula.local.player.music',
                                 'com.lenovo.vod.player',
                                 'com.android.settings',
                                 'com.lenovo.leos.pushengine',
                                 'com.android.systeminfo',
                                 'com.lenovo.ChangeServerAddress',
                                 'wnc.w806.engineermode',
                                 'com.lenovo.tv.freudsettings',
                                 'com.lenovo.service',
                                 'com.lenovo.leyun',
                                 'com.lenovo.nebula.Launcher',
                                 'com.baidu.input.oem',
                                 'com.lenovo.nebula.local.player.image',
                                 'com.lenovo.dc',
                                 'com.android.quicksearchbox',
                                 'com.iflytek.speechservice',
                                 'com.lenovo.nebula.weibo',
                                 'com.chinatvpay',
                                 'com.lenovo.leos.pay')
                 group by device
              """ % (self.startday_str, self.endday_str)
        res = WeeklyTask.hiveinterface.execute(sql)
        for li in res:
            value, device = li.split()
            key = self.day_str + device
            WeeklyTask.hbaseinterface.write(key, {"a:j": "%s" % value})

    def execute(self):
        self._a()
        self._b()
        self._c()
        self._d()


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
        if day.strftime("%w") == '5':
            task = WeeklyTask(day)
            task.execute()
                 
