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
    sntable = HbaseInterface(HBASEHOST, "9090", "sn_table")

    def __init__(self, day):
        self.day = day
        self.day_str = day.strftime("%Y%m%d")
        self.last_day = day - ONE_DAY
        self.last_day_str = self.last_day.strftime("%Y%m%d")
        #self.hiveinterface.execute("SET mapred.job.tracker=hadoopns410:8021")

    def exists_in_hbase(self, sn):
        key = "sn_%s" % (sn)
        return DailyTask.sntable.read(key, ["a:device"])

    def save_to_hbase(self, sn, device, day_str):
        key = "sn_%s" % (sn)
        key1 = "%s_%s" % (day_str, sn)
        d = {"a:device": device, "a:day": day_str}
        DailyTask.sntable.write(key, d)
        DailyTask.sntable.write(key1, d)

    @timed
    def init_sn_table(self):
        sql = """select distinct sn, device
                 from daily_logs where parsets < '20131231'
              """
        res = DailyTask.hiveinterface.execute(sql)
        if not res:
            res = []
        for li in res:
            sn, device = li.rstrip().split()
            if not self.exists_in_hbase(sn):
                self.save_to_hbase(sn, device, '20131230')

        total = {}
        for li in res:
            sn, device = li.rstrip().split()
            if device not in total:
                total[device] = 1
            else:
                total[device] += 1
        for device, c in total.iteritems():
            key = "20131230" + device
            DailyTask.hbaseinterface.write(key, {"a:a": str(c)})


    # 累计用户数 & 新增用户数
    @timed
    def _a(self):
        sql = """select distinct device from daily_logs 
                 where parsets >= "%s"
              """ % self.last_day_str
        res = DailyTask.hiveinterface.execute(sql)
        devices = {}
        res = res + ["A11", "A21", "K72", "K82", "K91",
                     "S31", "S51", "S61", 
                     "E31", '960A', 'E62', 'UD10A',
                     "DS70A", "LX750A", "LX755A", "LX850A"]
        res = [i.rstrip() for i in res]
        for li in res:
            devices[li.rstrip()] = {"x": 0, # 今天累计用户数
                                    "y": 0, # 昨天累计用户数
                                    "z": 0  # 新增用户数
                                    }

        sql = """select distinct sn, device
                 from daily_logs where parsets = "%s"
              """ % self.day_str
        res = DailyTask.hiveinterface.execute(sql)
        if not res:
            res = []
        for li in res:
            sn, device = li.rstrip().split()
            if not self.exists_in_hbase(sn):
                self.save_to_hbase(sn, device, self.day_str)
                devices[device]["z"] += 1
    
        for device in devices:
            y = DailyTask.hbaseinterface.read(self.last_day_str + device, ["a:a"])
            if y:
                devices[device]["y"] = int(y.columns["a:a"].value)
            else:
                devices[device]["y"] = 0
            devices[device]["x"] = devices[device]["y"] + devices[device]["z"]

            key = self.day_str + device
            DailyTask.hbaseinterface.write(key, {"a:a": str(devices[device]["x"])})
            DailyTask.hbaseinterface.write(key, {"a:b": str(devices[device]["z"])})

    # 新增用户数
    @timed
    def _b(self):
        pass 
            
    # 活跃用户数
    @timed
    def _c(self):
        sql = """select count(distinct sn), device 
                 from daily_logs where parsets = "%s"
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
                 from daily_logs where parsets = "%s"
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
       # sql = """select count(*), device
       #          from daily_logs where parsets = '%s'
       #          and event = "video_play_load"
       #          group by device
       #       """ % self.day_str
       # res1 = DailyTask.hiveinterface.execute(sql)
       # if not res1:
       #     res1 = []
       # for li in res1:
       #     value, device = li.split()
       #     key = self.day_str + device
       #     DailyTask.hbaseinterface.write(key, {"a:e": value})

        # device in ("A"), event = "video_start"
        sql = """select count(*), device
                 from daily_logs where parsets = "%s"
                 and event = "video_start"
                 group by device
              """ % self.day_str
        res2 = DailyTask.hiveinterface.execute(sql)
        if not res2:
            res2 = []
        for li in res2:
            value, device = li.split()
            key = self.day_str + device
            #if device.upper() in ['A11', 'A21']:
            DailyTask.hbaseinterface.write(key, {"a:e": value})

    # VOD用户播放总时长
    @timed
    def _f(self):
        sql = """select sum(duration), device
                 from daily_logs where parsets = "%s"
                 and event = "video_exit" and duration > 0 and isplus != 2
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

    # 普通应用(除去游戏应用,系统应用)用户
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

    # 游戏应用用户
    @timed
    def _j(self):
        sql = dailysql.sql_j_format % self.day_str
        res = DailyTask.hiveinterface.execute(sql)
        if not res:
            res = []
        for li in res:
            value, device = li.split()
            key = self.day_str + device
            DailyTask.hbaseinterface.write(key, {"a:j": "%s" % value})

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


def test():
    task = DailyTask(datetime.datetime.now())
    #print task.exists_in_hbase("sn0")
    #task.save_to_hbase("sn0", "device", "20140314")
    #print task.exists_in_hbase("sn0")
    task.init_sn_table()


if __name__ == "__main__":
    main()
    #test()

