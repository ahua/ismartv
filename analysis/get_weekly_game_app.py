#!/usr/bin/env python
#-*- coding:utf-8 -*-
import sys
import os
import datetime

def get_top50(date, dirname):
    filepath = os.path.join(dirname, "%s.txt" % date)
    d = {}
    d_no_sharp = {}
    with open(filepath) as fp:
        for li in fp:
            date, device, code, c = li.rstrip().split(",")            
            d[code] = int(c) + d.get(code, 0)
            if device not in ["DS70A", "LX750A", "LX755A", "LX850A"]:
                d_no_sharp[code] = int(c) + d_no_sharp.get(code, 0)
    return d, d_no_sharp

def get_top10(date, dirname):
    day0 = datetime.datetime.strptime(sys.argv[1], "%Y%m%d")
    day1 = day0 - datetime.timedelta(days=7)
    
    d0, d0_no_sharp = get_top50(date, dirname)
    d1, d1_no_sharp = get_top50(day1.strftime("%Y%m%d"), dirname)
    
    d = {}
    d_no_sharp = {}

    for k in d0:
        a = d0[k]
        b = d1.get(k, 0)
        d[k] = a*1.0/b if b != 0 else 0

    for k in d0_no_sharp:
        a = d0_no_sharp[k]
        b = d1_no_sharp.get(k, 0)
        d_no_sharp[k] = a * 1.0 / b if b != 0 else 0

    return d, d_no_sharp

def get_code_name(code2):
    d = {}
    fp0 = "./files/0226_name_code.txt"
    with open(fp0) as fp:
        for li in fp:
            name, code = li.rstrip().split(",")[0:2]
            d[code] = name
    
    fp1 = "./files/code_name.txt"
    with open(fp1) as fp:
        for li in fp:
            code, name = li.rstrip().split(",")[0:2]
            d[code] = name
    return d.get(code2, "-")


def display_top(d, n=50):
    nd = sorted(d.items(), key=lambda x:x[1], reverse=True)
    for i in range(0, min(n, len(nd))):
        print "%s,%s,%s,%s" % (i, get_code_name(nd[i][0]), nd[i][0], nd[i][1])
    

def display_newline():
    print 
    print "-" * 50
    print 


if __name__ == "__main__":
    date = sys.argv[1]
    appdir = "/home/deploy/src/ismartv/analysis/result/app"
    gameappdir = "/home/deploy/src/ismartv/analysis/result/gameapp"

    d, d_no_sharp = get_top50(date, appdir)
    print "所有设备"
    print "APP应用活跃用户数top50应用排名；"
    print "id,code,tiltle,count"
    display_top(d)
    print "除去Sharp设备"
    print "APP应用活跃用户数top50应用排名；"
    print "id,code,tiltle,count"
    display_top(d_no_sharp)

    display_newline()

    d, d_no_sharp = get_top50(date, gameappdir)
    print "所有设备"
    print "Game应用活跃用户数top50应用排名；"
    print "id,code,tiltle,count"
    display_top(d)
    print "除去Sharp设备"
    print "Game应用活跃用户数top50应用排名；"
    print "id,code,tiltle,count"
    display_top(d_no_sharp)

    display_newline()

    d, d_no_sharp = get_top10(date, appdir)
    print "所有设备"
    print "本周活跃用户数增长最快的应用top10；"
    print "id,code,tiltle,count"
    display_top(d, 10)
    print "除去Sharp设备"    
    print "本周活跃用户数增长最快的应用top10；"
    print "id,code,tiltle,count"
    display_top(d_no_sharp, 10)

    display_newline()

    d, d_no_sharp = get_top10(date, gameappdir)
    print "所有设备"
    print "本周活跃用户数增长最快的游戏top10；"
    print "id,code,tiltle,count"
    display_top(d, 10)
    print "除去Sharp设备"
    print "本周活跃用户数增长最快的游戏top10；"
    print "id,code,tiltle,count"
    display_top(d_no_sharp, 10)
