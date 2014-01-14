#!/usr/bin/env python

import gzip
import bz2
import os
import sys
import time
import json
import datetime
import shutil

DELIMITER = ","

EVENT_LIB = set(('system_off', 'system_on'))

def is_right(event):
    if event in EVENT_LIB:
        return True
    return False

def process(filename, output_dir):
    OUT_DIC = {}

    if filename.endswith(".gz"):
        fin = gzip.open(filename, "rb")
    elif filename.endswith(".bz2"):
        fin = bz2.BZ2File(filename, "rb")
    else:
        print "not support file type."
        return 

    for li in fin:
        try:
            r = eval(li.rstrip())
            if not is_right(r["event"]):
                continue
            if r["event"] == "video_exit":
                if r["duration"] == "0" and r["to"] == "next":
                    r["duration"] = r["position"]                 
                              
            timestamp = time.mktime(r["time"].timetuple())
            day = r["time"].strftime("%Y%m%d")
            _device = r["_device"]
            _unique_key = r["_unique_key"]
            sn = r["sn"]
            token= r["token"]
            event = r["event"]
            duration = r.get("duration", -1)
            clip = r.get("clip", -1)
            item = r.get("item", -1)
            subitem = r.get("subitem", -1)
            code = r.get("code", "-")
            mediaip = r.get("mediaip", "-")
            cdn = r.get("_cdn", "-")
            vals =[str(i) for i in [timestamp, day, _device, _unique_key, sn, token, event, duration, clip, code, item, subitem, mediaip, cdn]]

            outline = DELIMITER.join(vals) + "\n"
            if not OUT_DIC.has_key(day):
                OUT_DIC[day] = open(os.path.join(output_dir, day + ".log"), 'a')
            OUT_DIC[day].write(outline)
        except Exception as e:
            print e
            continue
    for fp in OUT_DIC.values():
        fp.flush()
        fp.close()
    fin.close()
        
    print "%s done..." % filename


def get_filelist():
    filelist = []
    with open("/tmp/s") as fp::
        for li in fp:
            filename = li.rstrip()
            if filename.endswith(".gz") or filename.endswith(".bz2"):
                filelist.append(filename)
    return filelist


def load_to_hive(output_dir):
    filelist = os.listdir(output_dir)
    for filename in filelist:
        path = os.path.join(output_dir, filename)
        parsets = filename.split(".")[0]
        sql = """load data local inpath '%s' into table daily_logs partition(parsets='%s');""" % (path, parsets)
        print sql
        os.system('''hive -e "%s"''' % sql)

def main():
    OUTPUT_DIR = "/tmp/output/"
    print "start at: %s" % datetime.datetime.now()
    filelist = get_filelist()
    for logfile in filelist:
        process(logfile, OUTPUT_DIR)
        #shutil.move(os.path.join(INPUT_DIR, logfile), os.path.join(USED_DIR, logfile))

    #load_to_hive(OUTPUT_DIR)
    
    print "end at: %s" % datetime.datetime.now()
    print "All Done..."


def test():
    pass

if __name__ == "__main__":
    main()

