#!/usr/bin/env python
import sys
import os
import datetime

def get_top50(date, dirname):
    filepath = os.path.join(dirname, "%s.txt" % date)
    d = {}
    with open(filepath) as fp:
        for li in fp:
            date, code, c = li.rstrip().split(",")
            d[code] = int(c)
    return d

def get_top10(date, dirname):
    day0 = datetime.datetime.strptime(sys.argv[1], "%Y%m%d")
    day1 = day0 - dateitme.timedelta(days=7)
    
    f0 = os.path.join(dirname, "%s.txt" % day0.strfitme("%Y%m%d"))
    f1 = os.path.join(dirname, "%s.txt" % day1.strfitme("%Y%m%d"))
    
    d0 = get_top50(f0, dirname)
    d1 = get_top50(f1, dirname)
    
    d = {}
    for k in d0:
        a = d0[k]
        b = d1.get(k, 0)
        d[k] = a/b if b != 0 else 0
    return d


def display_top50(d):
    nd = sorted(d.items(), key=lambda x:x[1], reverse=True)
    for i in range(0, len(nd)):
        print nd[i]
    

def display_newline():
    print 
    print "-" * 50
    print 


if __name__ == "__main__":
    date = sys.argv[1]
    appdir = "/home/deploy/src/ismartv/analysis/result/app"
    gameappdir = "/home/deploy/src/ismartv/analysis/result/gameapp"

    d = get_top50(date, appdir)
    display_top50(d)
    
    display_newline()

    d = get_top50(date, gameappdir)
    display_top50(d)

