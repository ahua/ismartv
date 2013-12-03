#!/usr/bin/env python

import os
import sys
import time
import json
import datetime

INPUT_DIR = "/home/deploy/ismartv/log/A21/used"
OUTPUT_DIR = "/home/deploy/ismartv/output/test"
DELIMITER = ","

DEVICE = "K91"

def from_splunk(filename):
    logfile = open(filename, "r")
    basename = os.path.basename(filename)
    if os.path.exists("%s/%s" % (OUTPUT_DIR, basename)):
        print "%s exists..." % basename
        return     
    outfile = open("%s/%s" % (OUTPUT_DIR, basename), "w")
    for li in logfile:
        try:
            vs = li.split("\t")
            r = {}
            r["time"] = datetime.datetime.strptime(vs[0], "%Y-%m-%d %H:%M:%S")
            r["sn"] = vs[1]
            for _t in vs[2:]:
                _i = _t.find("=")
                k = _t[:_i]
                v = _t[_i+1:]
                r[k] = v
                
            if r["event"] == "video_exit":
                if r["duration"] == "0" and r.has_key("to") and r["to"] == "next":
                    r["duration"] = r["position"]
                    
            timestamp = time.mktime(r["time"].timetuple())
            day = r["time"].strftime("%Y%m%d")
            _device = DEVICE
            _unique_key = "-"
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
            print li
            continue
        
    print "%s done..." % basename
    outfile.flush()
    outfile.close()
    logfile.close()


def from_logfile(filename):
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
            _device = "K91"
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

def help():
    msg = "$prog [INPUT_DIR [OUTPUT_DIR]]"
    print msg

if __name__ == "__main__":
    if "--splunk" in sys.argv:
        process = from_splunk
    else:
        process = from_logfile

    for i in sys.argv:
        if i.startswith("-"):
            sys.argv.remove(i)

    if len(sys.argv) >= 2:
        INPUT_DIR = sys.argv[1]
    if len(sys.argv) >= 3:
        OUTPUT_DIR = sys.argv[2]
    
    dirname = INPUT_DIR
    filelist = get_filelist(dirname)
    for logfile in filelist:
        process("%s/%s" % (dirname, logfile))
