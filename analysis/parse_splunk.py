#!/usr/bin/env python

import os
import sys
import time
import json
import datetime
import shutil

INPUT_DIR = "/home/deploy/ismartv/log/"
OUTPUT_DIR = "/home/deploy/ismartv/output/"
USED_DIR = "/home/deploy/ismartv/used/"

DELIMITER = ","

SN_DEV_MAP = {
    "TD01": ["S51", "42"],"TD02": ["S51", "47"],"TD03": ["S51", "55"],
    "RD01": ["S61", "42"],"RD02": ["S61", "47"],"RD03": ["S61", "55"],
    "UD01": ["S31", "39"],"UD02": ["S31", "50"],
    "TD04": ["A21", "32"],"TD05": ["A21", "42"],
    "TD06": ["A11", "32"],"TD07": ["A11", "42"],
    "CD01": ["K82", "60"],
    "CD02": ["LX750A", "46"],"CD03": ["LX750A", "52"],"CD04": ["LX750A", "60"],
    "CD05": ["LX850A", "60"],"CD06": ["LX850A", "70"],"CD07": ["LX850A", "80"],
    "CD08": ["K72", "60"],
    "CD09": ["DS70A", "46"],"CD10": ["DS70A", "52"],"CD11": ["DS70A", "60"],
    "CD12": ["DS755A", "46"],"CD13": ["DS755A", "52"],"CD14": ["DS755A", "60"]
}

def get_device_size(sn):
    if type(sn)==str and sn.isalnum():
        return ["K91","unknown"] if sn.islower() and len(sn) in [6,7,8] else SN_DEV_MAP.get(sn[0:4],["unknown","unknown"])
    return ["unknown","unknown"]

def get_device(sn):
    return get_device_size(sn)[0]


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
            _unique_key = "-"
            sn = r["sn"]
            _device = get_device(sn)
            token= r["token"]
            event = r["event"]
            duration = r.get("duration", -1)
            clip = r.get("clip", -1)
            code = r.get("code", "-")
            
            vals =[str(i) for i in [timestamp, day, _device, _unique_key, sn, token, event, duration, clip, code]]
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
            if not is_right(r["event"]):
                continue
            if r["event"] == "video_exit":
                if r["duration"] == "0" and r["to"] == "next":
                    r["duration"] = r["position"]                 
                              
            timestamp = time.mktime(r["time"].timetuple())
            day = r["time"].strftime("%Y%m%d")
            _device = "K91"
            _unique_key = r["_unique_key"]
            sn = r["sn"]
            token= r["token"]
            event = r["event"]
            duration = r.get("duration", -1)
            clip = r.get("clip", -1)
            item = r.get("item", -1)
            subitem = r.get("subitem", -1)
            code = r.get("code", "-")
            vals =[str(i) for i in [timestamp, day, _device, _unique_key, sn, token, event, duration, clip, code, item, subitem]]
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
    filelist = get_filelist(DIRNAME)
    for logfile in filelist:
        process("%s/%s" % (dirname, logfile))
        shutil.move(OS
