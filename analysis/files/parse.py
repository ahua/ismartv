#!/usr/bin/env python

import os
import sys
import time
import json
import datetime
import shutil

DELIMITER = ","
 
EVENT_LIB = set(('app_start',))

def is_right(event):
    if event in EVENT_LIB:
        return True
    return False

def process(filename):    
    fin = open(filename, "r")
    for li in fin:
        try:
            r = eval(li.rstrip())
            if not is_right(r["event"]):
                continue
            code = r.get("code", "-")
            title = r.get("title", "-")
            print "%s,%s" % (code, title)
        except Exception as e:
            #print e
            continue

def get_filelist(dirname):
    filelist = os.listdir(dirname)
    return filelist

def main():
    INPUT_DIR = sys.argv[1]
    
    filelist = get_filelist(INPUT_DIR)
    for logfile in filelist:
        process(os.path.join(INPUT_DIR, logfile))
        
if __name__ == "__main__":
    main()

