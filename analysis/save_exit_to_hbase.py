#!/usr/bin/env python

import os
import sys
import time
import json
import datetime
import shutil
from HbaseInterface import *

DELIMITER = ","

EVENT_LIB = set(['video_exit'])
HBASEMASTER = "10.0.4.10"

def is_right(event):
    if event in EVENT_LIB:
        return True
    return False

def process(filename):
    fin = open(filename, "r")
    client = HbaseInterface(HBASEMASTER,"9090","video_exit")
    for li in fin:
        try:
            r = eval(li.rstrip())
            if not is_right(r["event"]):
                continue

            if r["event"] == "video_exit":
                if r["duration"] == "0" and r["to"] == "next":
                    r["duration"] = r["position"]                 

            timestamp = time.mktime(r["time"].timetuple())
            minute = r["time"].strftime("%Y%m%d%H%M")
            _device = r["_device"]
            _unique_key = r["_unique_key"]
            sn = r["sn"]
            duration = r.get("duration", -1)
            client.write(_unique_key, {"log:minute": minute,
                                       "log:device": _device,
                                       "log:duration": duration,
                                       "log:sn": sn})
        except Exception as e:
            print e
            continue
        
    print "%s done..." % filename


def get_filelist():
    return None


def main():
    filelist = get_filelist(INPUT_DIR)
    for logfile in filelist:
        process(os.path.join(INPUT_DIR, logfile))

    print "end at: %s" % datetime.datetime.now()
    print "All Done..."


if __name__ == "__main__":
    main()

