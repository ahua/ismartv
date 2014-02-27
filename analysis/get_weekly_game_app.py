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
    day1 = day0 - datetime.timedelta(days=7)
    
    d0 = get_top50(date, dirname)
    d1 = get_top50(day1.strftime("%Y%m%d"), dirname)
    
    d = {}
    for k in d0:
        a = d0[k]
        b = d1.get(k, 0)
        d[k] = a*1.0/b if b != 0 else 0
    return d


def display_top(d, n=50):
    nd = sorted(d.items(), key=lambda x:x[1], reverse=True)
    for i in range(0, min(n, len(nd))):
        print "%s,%s" % (nd[i][0], nd[i][1])
    

def display_newline():
    print 
    print "-" * 50
    print 


if __name__ == "__main__":
    date = sys.argv[1]
    appdir = "/home/deploy/src/ismartv/analysis/result/app"
    gameappdir = "/home/deploy/src/ismartv/analysis/result/gameapp"

    d = get_top50(date, appdir)
    display_top(d)
    
    display_newline()

    d = get_top50(date, gameappdir)
    display_top(d)

    d = get_top10(date, appdir)
    display_top(d, 10)
    
    d = get_top10(date, gameappdir)
    display_top(d, 10)

