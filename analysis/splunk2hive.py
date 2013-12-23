#!/usr/bin/env python

import os
import sys
import time
import json
import datetime
import shutil

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

def is_right(event):
    return True
    if event in EVENT_LIB:
        return True
    return False


def process(filename, output_dir):
    fin = open(filename, "r")
    basename = os.path.basename(filename)
    fout = open("%s/%s" % (output_dir, basename), "w")
    for li in fin:
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
            if not is_right(r["event"]):
                continue

            if r["event"] == "video_exit":
                if r["duration"] == "0" and r.has_key("to") and r["to"] == "next":
                    r["duration"] = r["position"]
                    
            timestamp = time.mktime(r["time"].timetuple())
            day = r["time"].strftime("%Y%m%d")
            sn = r["sn"]
            _device = get_device(sn)
            _unique_key = "-"
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
        
    print "%s done..." % basename
    fout.flush()
    fout.close()
    fin.close()

def get_filelist(dirname):
    return os.listdir(dirname)

if __name__ == "__main__":
    INPUT_DIR =  "/host/history_data/history_data/K"
    OUTPUT_DIR = "/host/history_data/history_data/K_OUT"

    filelist = get_filelist(INPUT_DIR)
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)
    for logfile in filelist:
        process(os.path.join(INPUT_DIR, logfile), OUTPUT_DIR)


    INPUT_DIR =  "/host/history_data/history_data/S"
    OUTPUT_DIR = "/host/history_data/history_data/S_OUT"

    filelist = get_filelist(INPUT_DIR)
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)
    for logfile in filelist:
        process(os.path.join(INPUT_DIR, logfile), OUTPUT_DIR)
