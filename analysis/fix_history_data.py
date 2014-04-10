#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import datetime

from decorators import timed
from HiveInterface import HiveInterface
from HbaseInterface import HbaseInterface

HBASEHOST = "hadoopns410"
ONE_DAY = datetime.timedelta(days=1)

hbaseinterface = HbaseInterface(HBASEHOST, "9090","daily_result")
def we(key, value):
    hbaseinterface.write(key, {"a:e": value})

def wf(key, value):
    hbaseinterface.write(key, {"a:f": value})

if __name__ == "__main__":
    with open("files/all_20140301.csv") as fp:
        for li in fp:
            dev, date, e, f = li.rstrip().replace(" ", "").split(",")
            key = date.replace("-", "") + dev.upper()
            print key, e, f
            we(key, e)
            wf(key, f)

