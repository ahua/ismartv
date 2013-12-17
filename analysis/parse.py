#!/usr/bin/env python

import os
import sys
import time
import json
import datetime
import shutil

DELIMITER = ","

EVENT_LIB = set(('system_off',
 'video_relate_out',
 'video_collect',
 'video_history',
 'video_search',
 'video_exit',
 'video_collect_in',
 'video_play_seek_blockend',
 'video_play_load',
 'video_score',
 'video_play_blockend',
 'app_start',
 'video_channel_in',
 'video_search_arrive',
 'video_category_out',
 'video_history_out',
 'launcher_vod_click',
 'video_play_start',
 'video_except',
 'video_comment',
 'system_on',
 'video_channel_out',
 'video_relate',
 'video_switch_stream',
 'video_category_in',
 'video_detail_in',
 'video_history_in',
 'video_relate_in',
 'video_play_seek',
 'video_play_continue',
 'video_collect_out',
 'video_start',
 'video_detail_out',
 'video_play_pause'))

def is_right(event):
    if event in EVENT_LIB:
        return True
    return False

def process(filename, output_dir):
    fin = open(filename, "r")
    outpath = os.path.join(output_dir, os.path.basename(filename))
    if os.path.exists(outpath):
        print "%s exists..." % outpath
        return     
    fout = open(outpath, "w")
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
            vals =[str(i) for i in [timestamp, day, _device, _unique_key, sn, token, event, duration, clip, code, item, subitem]]
            fout.write(DELIMITER.join(vals) + "\n")
        except Exception as e:
            print e
            continue
    print "%s done..." % filename
    fout.flush()
    fout.close()
    fin.close()


def get_filelist(dirname):
    return os.listdir(dirname)

def load_to_hive(output_dir, date):
    sql = """load data local inpath '%s' into table daily_logs partition(parsets='%s');""" % (output_dir, date)
    os.system('''hive -e "%s"''' % sql)


if __name__ == "__main__":
    INPUT_DIR =  "/home/deploy/vod/track/log"
    OUTPUT_DIR = "/home/deploy/vod/track/output/"
    USED_DIR =   "/home/deploy/vod/track/used/"

    filelist = get_filelist(INPUT_DIR)
    date = datetime.datetime.today().strftime("%Y%m%d%H")
    OUTPUT_DIR = os.path.join(OUTPUT_DIR, date)
    USED_DIR = os.path.join(USED_DIR, date)
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)
    if not os.path.exists(USED_DIR):
        os.mkdir(USED_DIR)
    for logfile in filelist:
        process(os.path.join(INPUT_DIR, logfile), OUTPUT_DIR)
        shutil.move(os.path.join(INPUT_DIR, logfile), os.path.join(USED_DIR, logfile))

    load_to_hive(OUTPUT_DIR, date)

