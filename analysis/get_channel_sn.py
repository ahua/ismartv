#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import datetime

from HbaseInterface import HbaseInterface

SHARP_DEV_LIST = ['LX755A', 'LX850A', 'LX960A', 'DS70A', 'LX750A', 'UD10A']
LENOVO_DEV_LIST = ['A21', 'K72', 'S61', 'A11', 'E31', 'E62', 'K82', 'K91', 'S31', 'S51']
CHANNEL_LIST = ['chinesemovie','comic','documentary','music','overseas','sport','teleplay','variety']

HBASEHOST = "hadoopns410"
ONE_DAY = datetime.timedelta(days=1)
hbaseinterface = HbaseInterface(HBASEHOST, "9090","daily_channel")


startday = datetime.datetime(2014, 4, 11)
endday = datetime.datetime(2014, 5, 9)


f = open("/tmp/sharp_by_day.csv", "w")
while startday <= endday:
    parsets = startday.strftime("%Y%m%d")
    t = {}
    for i in CHANNEL_LIST:
        t[i] = [0, 0]
    for dev in SHARP_DEV_LIST:
        for channel in CHANNEL_LIST:
            key = parsets+dev+channel
            r = hbaseinterface.read(key, ["a:a","a:c"])
            if r:
                a = int(r.columns["a:a"].value)
                c = int(r.columns["a:c"].value)
                t[channel][0] += a
                t[channel][1] += c
    for i in CHANNEL_LIST:
        s = "%s,%s,%s,%s,%s\n" % (parsets, "sharp", i, t[i][0], t[i][1]/t[i][0] if t[i][0] != 0 else 0)
        f.write(s)
    startday += ONE_DAY

f.close()


startday = datetime.datetime(2014, 4, 11)
endday = datetime.datetime(2014, 5, 9)
f = open("/tmp/lenovo_by_day.csv", "w")
while startday <= endday:
    parsets = startday.strftime("%Y%m%d")
    t = {}
    for i in CHANNEL_LIST:
        t[i] = [0, 0]
    for dev in LENOVO_DEV_LIST:
        for channel in CHANNEL_LIST:
            key = parsets+dev+channel
            r = hbaseinterface.read(key, ["a:a","a:c"])
            if r:
                a = int(r.columns["a:a"].value)
                c = int(r.columns["a:c"].value)
                t[channel][0] += a
                t[channel][1] += c
    for i in CHANNEL_LIST:
        s = "%s,%s,%s,%s,%s\n" % (parsets, "lenovo", i, t[i][0], t[i][1]/t[i][0] if t[i][0] != 0 else 0)
        f.write(s)
    startday += ONE_DAY

f.close()

