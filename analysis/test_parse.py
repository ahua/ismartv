#!/usr/bin/env python

import os
import sys
import time
import json
import datetime
import shutil

DELIMITER = ","

EVENT_LIB = set(('system_off',
 'video_play_blockend',
 'video_start',
 'video_play_load',
 'video_exit',
 'app_start',
 'system_on',
 'app_exit'))

def is_right(event):
    if event in EVENT_LIB:
        return True
    return False

CHANNELS = {}
def get_channel(item):
    global CHANNELS
    if not CHANNELS:
        with open("./files/itemchannel.csv") as fp:
            for li in fp:
                try:
                    item, channel = li.rstrip().split(",")
                    CHANNELS[item] = channel
                except:
                    pass
    return CHANNELS.get(item, "-")

def process(filename, output_dir):
    OUT_DIC = {}
    
    fin = open(filename, "r")
    for li in fin:
        try:
            r = eval(li.rstrip())
            if not is_right(r["event"]):
                continue
            if r.get("_type", "normal") != "normal":
                continue
            if r["event"] == "video_exit":
                if r["duration"] == "0" and r["to"] == "next":
                    r["duration"] = r["position"]                 
                if float(r["duration"]) >= 100000:
                    r["duration"] = r["position"]
                if float(r["duration"]) < 0:
                    r["duration"] = 0

            timestamp = time.mktime(r["time"].timetuple())
            day = r["time"].strftime("%Y%m%d")
            _device = r["_device"]
            _unique_key = r["_unique_key"]
            sn = r["sn"]
            token= r["token"]
            event = r["event"]
            duration = int(r.get("duration", -1))
            clip = r.get("clip", -1)
            item = r.get("item", -1)
            subitem = r.get("subitem", -1)
            code = r.get("code", "-")
            mediaip = r.get("mediaip", "-")
            cdn = r.get("_cdn", "-").encode("utf8")
            isplus = r.get("_plus", 5)
            channel = get_channel(item)
            quality = r.get("quality", "-")
            vals =[str(i) for i in [timestamp, day, _device, _unique_key, sn, token, event, duration, clip, code, item, \
                                        subitem, mediaip, cdn, isplus, channel, quality]]

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


def get_filelist(dirname):
    filelist = os.listdir(dirname)
    now = datetime.datetime.now()
    SECONDS = 60 * 60
    def modified_before(filepath):
        mtime = datetime.datetime.fromtimestamp(os.stat(os.path.join(dirname, filepath)).st_mtime)
        d = (now - mtime).total_seconds()
        return d >= SECONDS
    return filter(modified_before, filelist)


def load_to_hive(output_dir):
    filelist = os.listdir(output_dir)
    for filename in filelist:
        path = os.path.join(output_dir, filename)
        parsets = filename.split(".")[0]
        sql = """load data local inpath '%s' into table daily_logs partition(parsets='%s');""" % (path, parsets)
        print sql
        os.system('''hive -e "%s"''' % sql)

def main():
    INPUT_DIR =  sys.argv[1]
    OUTPUT_DIR = sys.argv[2]

    print "start at: %s" % datetime.datetime.now()
    filelist = get_filelist(INPUT_DIR)
    for logfile in filelist:
        process(os.path.join(INPUT_DIR, logfile), OUTPUT_DIR)
    
    print "end at: %s" % datetime.datetime.now()
    print "All Done..."


if __name__ == "__main__":
    main()

    

