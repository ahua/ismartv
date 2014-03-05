#!/usr/bin/env python
# -*- coding: utf-8 -*- 

import sys
import math 
import datetime

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import *

HBASE_ADDR = "hadoopns410"

class NotFoundTable(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class HbaseInterface:
    def __init__(self, address, port, table):
        self.tableName = table
        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(address, port))
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(self.protocol)
        self.transport.open()
        tables = self.client.getTableNames()
        if self.tableName not in tables:
            raise NotFoundTable(self.tableName)
            
    def __del__(self):
        self.transport.close()

    def write(self, key, d):
        mutations = []
        for k,v in d.iteritems():
            mutations.append(Mutation(column=k, value=v))
        self.client.mutateRow(self.tableName, key, mutations, {})


    def read(self, key, columns):
        scannerId = self.client.scannerOpenWithPrefix(self.tableName, key, columns, None)
        rowlist = self.client.scannerGet(scannerId)
        self.client.scannerClose(scannerId)
        if len(rowlist) >= 1:
            return rowlist[0]
        return None
    
    def read_all(self, prefix, columns):
        scannerId = self.client.scannerOpenWithPrefix(self.tableName, prefix, columns, None)
        rowlist = self.client.scannerGetList(scannerId, 100)
        self.client.scannerClose(scannerId)
        return rowlist


def get_daily_data(date):
    client = HbaseInterface(HBASE_ADDR, "9090", "daily_result")    
    colkeys = ["a:c", "a:g", "a:i"]
    rowlist = client.read_all(date, colkeys)
    s = ('联网用户总数','应用活跃用户','game活跃用户')
    res = {"date": date,
           "a:c": 0,
           "a:g": 0,
           "a:i": 0}
    res_no_shart = {"date": date,
                    "a:c": 0,
                    "a:g": 0,
                    "a:i": 0}
    for r in rowlist:
        device = r.row[8:]
        for k in colkeys:
            res[k] += int(r.columns[k].value) if k in r.columns else 0
        if device not in ["DS70A", "LX750A", "LX755A", "LX850A"]:
            for k in colkeys:
                res_no_shart[k] += int(r.columns[k].value) if k in r.columns else 0
            
    return [res, res_no_shart]

ONE_DAY = datetime.timedelta(days=1)

if __name__ == "__main__":
    daylist = []
    if len(sys.argv) == 1:
        daylist = [datetime.datetime.now() - datetime.timedelta(days=1)]
    elif len(sys.argv) == 2:
        daylist = [datetime.datetime.strptime(sys.argv[1], "%Y%m%d")]
    else:
        startday = datetime.datetime.strptime(sys.argv[1], "%Y%m%d")
        endday = datetime.datetime.strptime(sys.argv[2], "%Y%m%d")
        while startday <= endday:
            daylist.append(startday)
            startday = startday + ONE_DAY

    reslist = []
    for date in daylist:
        reslist.append(get_daily_data(date.strftime("%Y%m%d")))
    
    print "所有设备"
    print '日期,联网用户总数,应用活跃用户,game活跃用户'
    for res, _ in reslist:
        print "%s,%s,%s,%s" % (res["date"], res["a:c"], res["a:g"], res["a:i"])
    print 
    print "除去Sharp设备"
    print '日期,联网用户总数,应用活跃用户,game活跃用户'
    for _, res in reslist:
        print "%s,%s,%s,%s" % (res["date"], res["a:c"], res["a:g"], res["a:i"])

