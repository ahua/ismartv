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

class MonthlyTask:
    hiveinterface = HiveInterface(HIVEHOST)    
    hiveinterface.execute("SET mapred.job.tracker=hadoopns410:8021")
    hbaseinterface = HbaseInterface(HOST, "9090","monthly_result")

    # 自然月
    def __init__(self, day):
        self.day = day
        if self.day.day != 1:
            raise Exception("MonthlyTask must be executed at 1th of month...")
        self.endday = self.day - ONE_DAY
        self.startday = self.endday.replace(day=1)
        self.startday_str = self.startday.strftime("%Y%m%d")
        self.endday_str = self.endday.strftime("%Y%m%d")
        self.month_str = self.startday.strftime("%Y%m")
            
    # 活跃用户数
    @timed
    def _a(self):
        sql = """select count(distinct sn), device 
                 from daily_logs where parsets >= '%s' and parsets <= '%s'
                 group by device
              """ % (self.startday_str, self.endday_str)
        res = MonthlyTask.hiveinterface.execute(sql)
        for li in res:
            value, device = li.split()
            key = self.month_str + device
            MonthlyTask.hbaseinterface.write(key, {"a:a": value})

    # VOD用户数
    @timed
    def _b(self):
        sql = """select count(distinct sn), device
                 from daily_logs where parsets >= '%s' and parsets <= '%s'
                 and event in ("video_start", "video_play_load", "video_play_start", "video_exit")
                 group by device
              """ % (self.startday_str, self.endday_str)
        res = MonthlyTask.hiveinterface.execute(sql)
        for li in res:
            value, device = li.split()
            key = self.month_str + device
            MonthlyTask.hbaseinterface.write(key, {"a:b": value})

    # 应用激活数
    @timed
    def _c(self):
        sql = """select count(distinct sn), device
                 from daily_logs where parsets >= '%s' and parsets <= '%s'
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
        res = MonthlyTask.hiveinterface.execute(sql)
        for li in res:
            value, device = li.split()
            key = self.month_str + device
            MonthlyTask.hbaseinterface.write(key, {"a:c": "%s" % value})

    # 智能激活数
    @timed
    def _d(self):
        sql = """select count(distinct sn), device
                 from daily_logs where parsets >= '%s' and parsets <= '%s'
                 and event in ("video_start", "video_play_load", "video_play_start", "video_exit", "app_start")
                 and code not in ('com.lenovo.oobe',
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
        res = MonthlyTask.hiveinterface.execute(sql)
        for li in res:
            value, device = li.split()
            key = self.month_str + device
            MonthlyTask.hbaseinterface.write(key, {"a:d": "%s" % value})

    # 首次缓冲3秒内（含）次数
    @timed
    def _e(self):
        sql = """select count(*), device 
                 from daily_logs
                 where event = "video_play_load" and duration <= 3
                 and parsets >= '%s' and parsets <= '%s'
                 group by device
              """ % (self.startday_str, self.endday_str)
        res = MonthlyTask.hiveinterface.execute(sql)
        for li in res:
            value, device = li.split()
            key = self.month_str + device
            MonthlyTask.hbaseinterface.write(key, {"a:e": "%s" % value})

    # 每次播放卡顿2次内次数
    @timed
    def _f(self):
        sql = """select device, sn, token, clip, count(*)
                 from daily_logs 
                 where parsets >= '%s' and parsets <= '%s'
                 and event = "video_play_blockend"
                 group by device, sn, token, clip
              """ % (self.startday_str, self.endday_str)
        res = MonthlyTask.hiveinterface.execute(sql)
        dd = {}
        for li in res:
            device, _, _, _, c = li.split()
            if int(c) < 2:
                if dd.has_key(device):
                    dd[device] += 1
                else:
                    dd[device] = 1
        for device, value in dd.iteritems():
            key = self.month_str + device
            MonthlyTask.hbaseinterface.write(key, {"a:f": "%s" % value})

    def execute(self):
        self._a()
        self._b()
        self._c()
        self._d()
        self._e()
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
        task = MonthlyTask(day)
        task.execute()
                                           
