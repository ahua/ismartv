#!/usr/bin/env python

import os
import sys
import time
import json
import datetime

INPUT_DIR = "/home/deploy/ismartv/log/A21/used"
OUTPUT_DIR = "/home/deploy/ismartv/output/test"
DELIMITER = ","

fields = set([])

def from_splunk(filename):
    logfile = open(filename, "r")
    for li in logfile:
        try:
            vs = li.split("\t")
            r = {}
            for _t in vs[2:]:
                _i = _t.find("=")
                fields.add(_t[:_i])
        except Exception as e:
            print li
            continue        
    print "%s done..." % filename
    logfile.close()


def from_logfile(filename):
    logfile = open(filename, "r")
    for li in logfile:
        try:
            r = eval(li.rstrip())
            #print r.keys()
            fields.update(r.keys())
        except Exception as e:
            print e
            continue
    print "%s done..." % filename
    logfile.close()

def get_filelist(dirname):
    return os.listdir(dirname)


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
    for logfile in filelist[:1]:
        process("%s/%s" % (dirname, logfile))
        
    print fields
