#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import datetime

from decorators import timed
from HiveInterface import HiveInterface
from HbaseInterface import HbaseInterface


HOST = "hadoopmaster"
ONE_DAY = datetime.timedelta(days=1)

class WeeklyTask:
    hiveinterface = HiveInterface(HOST)    
    hbaseinterface = HbaseInterface(HOST, "9090","daily_result")

    # 周的定义：上周五0点-本周四24点
    def __init__(self, day):
        self.day = day
        self.day_str = day.stritime("%Y%m%d")
        self.week = day.strftime("%w")
        
        if self.week != '5':
            raise Exception("WeeklyTask must be executed at Friday...")

        self.startday = self.day - 7 * ONE_DAY
        self.endday = self.day - ONE_DAY
        self.start_s = self.startday.strftime("%Y%m%d")
        self.end_s = self.endday.strftime("%Y%m%d")

    # 周活跃用户数
    @timed
    def _a(self):
        sql = """select count(distinct sn), device 
                 from daily_logs where d >= %s and d <= %s
                 group by device
              """ % (self.start_s, self.end_s)
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
              """ % (self.start_s, self.end_s)
        res = WeeklyTask.hiveinterface.execute(sql)
        for li in res:
            value, device = li.split()
            key = self.day_str + device
            WeeklyTask.hbaseinterface.write(key, {"a:b": value})

    # 周VOD激活率
    @timed
    def _c(self):
        sql = """select distinct device from daily_logs 
                 where d = %s
              """ % self.day_str
        res = WeeklyTask.hiveinterface.execute(sql)
        for device in res:
            x = WeeklyTask.hbaseinterface.read(self.day_str + device, ["a:d"])
            y = WeeklyTask.hbaseinterface.read(self.day_str + device, ["a:a"])
            z = float(x.columns["a:d"].value)/float(y.columns["a:a"].value)
            WeeklyTask.hbaseinterface.write(self.day_str + device, {"a:c": "%s" % round(z, 4)})

    # 周应用激活率
    @timed
    def _d(self):
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
              """ % (self.start_s, self.end_s)
        res = WeeklyTask.hiveinterface.execute(sql)
        for li in res:
            value, device = li.split()
            key = self.day_str + device
            x = WeeklyTask.hbaseinterface.read(self.day_str + device, ["a:c"])
            z = float(value) / float(x.columns["a:c"].value)
            WeeklyTask.hbaseinterface.write(key, {"a:h": "%s" % round(z, 4)})

    # 周智能激活率
    @timed
    def _e(self):
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
              """ % (self.start_s, self.end_s)
        res = WeeklyTask.hiveinterface.execute(sql)
        for li in res:
            value, device = li.split()
            key = self.day_str + device
            x = WeeklyTask.hbaseinterface.read(self.day_str + device, ["a:c"])
            z = float(value) / float(x.columns["a:c"].value)
            WeeklyTask.hbaseinterface.write(key, {"a:j": "%s" % round(z, 4)})

    # 日均活跃用户数
    @timed
    def _f(self):
        pass

    # 日均VOD用户数
    @timed
    def _g(self):
        pass

    # 日均VOD播放次数
    @timed
    def _h(self):
        pass

    # 日均户均时长
    @timed
    def _i(self):
        pass

    # 日均VOD激活率
    @timed
    def _j(self):
        pass

    # 日均应用激活率
    @timed
    def _k(self):
        pass

    # 日均智能激活率
    @timed
    def _l(self):
        pass


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
        self._j()
        self._k()
        self._l()


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
                 
