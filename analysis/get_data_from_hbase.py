#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import datetime

from decorators import timed
from HbaseInterface import HbaseInterface

HBASEHOST = "hadoopns410"
ONE_DAY = datetime.timedelta(days=1)
hbaseinterface = HbaseInterface(HBASEHOST, "9090","daily_result")

hbaseinterface.read(self.day_str + device, ["a:a"])

                                           
