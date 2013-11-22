#!/usr/bin/env python

import os
import sys
import time
import json
import datetime

INPUT_DIR = "/home/deploy/ismartv/log/A21/used"
OUTPUT_DIR = "/home/deploy/ismartv/output/test"
DELIMITER = ","

def process_logfile(filename):
    logfile = open(filename, "r")
    basename = os.path.basename(filename)
    if os.path.exists("%s/%s" % (OUTPUT_DIR, basename)):
        print "%s exists..." % basename
        return     
    outfile = open("%s/%s" % (OUTPUT_DIR, basename), "w")
    for li in logfile:
        try:
            r = eval(li.rstrip())
            if r["event"] == "video_exit":
                if r["duration"] == "0" and r["to"] == "next":
                    r["duration"] = r["position"]

            timestamp = time.mktime(r["time"].timetuple())
            day = r["time"].strftime("%Y%m%d")
            _device = r["_device"]
            _unique_key = r["_unique_key"]
            sn = r["sn"]
            token= r["token"]
            ip = r["ip"]
            event = r["event"]
            duration = r.get("duration", -1)
            clip = r.get("clip", -1)
            code = r.get("code", "-")
            
            vals =[str(i) for i in [timestamp, day, _device, _unique_key, sn, token, ip, event, duration, clip, code]]
            outfile.write(DELIMITER.join(vals) + "\n")
        except Exception as e:
            print e
            continue
    print "%s done..." % basename
    outfile.flush()
    outfile.close()
    logfile.close()

def get_filelist(dirname):
    return os.listdir(dirname)

if __name__ == "__main__":
    dirname = INPUT_DIR
    filelist = get_filelist(dirname)
    for logfile in filelist:
        process_logfile("%s/%s" % (dirname, logfile))
