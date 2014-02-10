#!/usr/bin/python
# -*- coding: utf-8 -*- 
import sys

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import *

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



HTML = """
<html>
<head></head>
<body>
<table border="1" cellspacing="0">
 %s
</table>
</body>
</html>
"""

TR = """
<tr>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
</tr>
"""

WEEK_TR = """
<tr>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
</tr>
"""

MONTH_TR = """
<tr>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
  <td>%s</td>
</tr>
"""

def get_daily_data(key):
    client = HbaseInterface("localhost","9090","daily_result")    
    colkeys = ["a:a", "a:b", "a:c", "a:d", "a:e", "a:f", "a:g", "a:h"]
    rowlist = client.read_all(key, colkeys)
    s = TR % ('日期设备', '累计用户', '新增用户', '活跃用户', 'VOD用户',\
                  'VOD播放次数', 'VOD播放总时长', '应用激活用户', '智能激活用户')
    for r in rowlist:
        cols = r.columns
        t = [r.row]
        for k in colkeys:
            if k in cols:
                t.append(cols[k].value)
            else:
                t.append(None)
        s += TR % tuple(t)
    print HTML % s


def get_weekly_data(key):
    client = HbaseInterface("localhost","9090","daily_result")    
    colkeys = ["a:a", "a:b", "a:c", "a:d"]
    rowlist = client.read_all(key, colkeys)
    s = WEEK_TR % ('日期设备', '活跃用户', 'VOD用户', '应用激活用户', '智能激活用户')
    for r in rowlist:
        cols = r.columns
        t = [r.row]
        for k in colkeys:
            if k in cols:
                t.append(cols[k].value)
            else:
                t.append(None)
        s += WEEK_TR % tuple(t)
    print HTML % s

def get_monthly_data(key):
    client = HbaseInterface("localhost","9090","daily_result")    
    colkeys = ["a:a", "a:b", "a:c", "a:d", "a:e", "a:f"]
    rowlist = client.read_all(key, colkeys)
    s = MONTH_TR % ('日期设备', '活跃用户', 'VOD用户','应用激活用户', \
                  '智能激活用户', '首次缓冲3秒内(含)', '每次播放卡顿2次内')
    for r in rowlist:
        cols = r.columns
        t = [r.row]
        for k in colkeys:
            if k in cols:
                t.append(cols[k].value)
            else:
                t.append(None)
        s += MONTH_TR % tuple(t)
    print HTML % s


if __name__ == "__main__":
    key = "20140102"
    if len(sys.argv) == 2:
        key = sys.argv[1]
    get_daily_data(key)



