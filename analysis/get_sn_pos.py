#!/usr/bin/env python
#-*- coding: utf-8 -*-

import os
import sys
import datetime
import gzip
import redis
from decorators import timed
from HbaseInterface import HbaseInterface

HBASEHOST = "hadoopns410"
sntable = HbaseInterface(HBASEHOST, "9090", "sn_table")

def exists_in_hbase(sn):
    key = "sn_%s" % (sn)
    return sntable.read(key, ["a:device"])


def save_to_hbase(sn, day_str, province, city):
    key = "sn_%s" % (sn)
    key1 = "%s_%s" % (day_str, sn)
    d = {"a:province": province, "a:city": city}
    DailyTask.sntable.write(key, d)
    DailyTask.sntable.write(key1, d)

def get_sn_list_by_day(day_str):
    rowlist = sntable.read_all(day_str, ["a:device", "a:day"])
    sn_list = []
    for r in rowlist:
        day_sn = r.row 
        sn_list.append(day_sn[9:])
    return sn_list


def get_ip(sn):
    r = redis.Redis('10.0.2.41', port = 6379, db=3)
    res = r.zrevrange(sn, 0, -1)
    if res:
        return res[0]
    return None

SN_CACHE = {}

IP_LOOKUP_URL = ['http://cdn.ismartv.com.cn/ip_query.php?ip=',
                 '&code=gb2312&format=json']

def init_sn_cache(day):
    global SN_CACHE
    
    with open("/var/tmp/sn/snlist.log") as fp:
        for li in fp:
            sn, province, city = li.rstrip().split()
            SN_CACHE[sn] = [province, city]
    
    gzfile = "/var/tmp/sn/snlist_area%s.tgz" % day.strftime("%Y_%m_%d")
    if os.path.exists(gzfile):
        f = gzip.open(gzfile)
        content = f.read()
        f.close()
        lines = content.split("\n")
        for li in lines:
            sn, province, city = li.rstrip().split()
            SN_CACHE[sn] = [province, city]


def get_pos(sn):
    if sn in SN_CACHE:
        return SN_CACHE[sn]
    ip = get_ip(sn)
    if ip:
        data = json.load(urllib2.urlopen(urllib2.Request(IP_LOOKUP_URL[0] + ip + IP_LOOKUP_URL[1])))

def process(day):
    global SN_CACHE
    if not SN_CACHE:
        init_sn_cache(day)

    day_str = day.strftime("%Y%m%d")
    sn_list = get_sn_list_by_day(day_str)
    for sn in sn_list:
        province, city = get_pos(sn)
        #save_to_hbase(sn, day_str, province, city)
        print province, city
        

def main():
    if len(sys.argv) == 1:
        daylist = [datetime.datetime.now() - datetime.timedelta(days=1)]
    elif len(sys.argv) == 2:
        daylist = [datetime.datetime.strptime(sys.argv[1], "%Y%m%d")]
    else:
        startday = datetime.datetime.strptime(sys.argv[1], "%Y%m%d")
        endday = datetime.datetime.strptime(sys.argv[2], "%Y%m%d")
        daylist = []
        while startday <= endday:
            daylist.append(startday)
            startday = startday + ONE_DAY
    
    for day in daylist:
        process(day)

if __name__ == "__main__":
    main()

