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
            title.replace("," " ")
            print "%s,%s" % (code, title.replace(",", " "))
        except Exception as e:
            #print e
            continue

def main():
    process(sys.argv[1])
        
if __name__ == "__main__":
    main()

