#!/usr/bin/python
# -*- coding: utf-8 -*- 
import sys
import math 

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


def get_device_list(devices):
    devices = devices.upper()
    if devices == "ALL":
        return ['S31', 'S51', 'S61', 
                'K91',
                'A11', 'A21',
                'K72', 'K82',
                'DS70A',
                'LX750A', 'LX755A', 'LX850A']
    return devices.split(",")

HBASE_ADDR = "10.0.4.10"

def get_daily_data(date, devices='ALL'):
    device_list = get_device_list(devices)

    client = HbaseInterface(HBASE_ADDR, "9090", "daily_result")    
    colkeys = ["a:a", "a:b", "a:c", "a:d", "a:e", "a:f", "a:g", "a:h"]
    rowlist = client.read_all(date, colkeys)
    s = ('日期设备', '累计用户', '新增用户', '活跃用户', 'VOD用户',\
             'VOD播放次数', 'VOD播放总时长', '应用激活用户', '智能激活用户')
    d = {}
    for r in rowlist:
        device = r.row[8:]
        d[device] = {}
        for k in colkeys:
            d[device][k] = float(r.columns[k].value) if k in r.columns else 0
            
    sn_total = math.fsum([d[dev]["a:a"] for dev in device_list])
    sn_new = math.fsum([d[dev]["a:b"] for dev in device_list])
    sn_active = math.fsum([d[dev]["a:c"] for dev in device_list])
    sn_vod_load = math.fsum([d[dev]["a:d"] for dev in device_list])
    dr_per_sn = math.fsum([d[dev]["a:f"] for dev in device_list]) / sn_vod_load\
        if sn_vod_load > 0 else 0
    load_per_active = sn_vod_load / sn_active if sn_active > 0 else 0
    active_per_total = sn_active / sn_total if sn_total > 0 else 0
    
    return {"time": date,
            "sn_total": sn_total,            # 累计用户
            "sn_new": sn_new,                # 新增用户
            "sn_active": sn_active,          # 活跃用户
            "sn_vod_load": sn_vod_load,      # VOD用户数
            "dr_per_sn": dr_per_sn,          # VOD户均时长
            "load_per_active": load_per_active,   # 激活率(VOD)
            "active_per_total": active_per_total  # 开机率(VOD)
            }

def get_weekly_data(date, device='ALL'):
    client = HbaseInterface(HBASE_ADDR, "9090", "daily_result")    
    colkeys = ["a:a", "a:b", "a:c", "a:d"]
    rowlist = client.read_all(date, colkeys)
    s = ('日期设备', '活跃用户', 'VOD用户', '应用激活用户', '智能激活用户')

def get_monthly_data(date, device='ALL'):
    client = HbaseInterface(HBASE_ADDR, "9090", "daily_result")    
    colkeys = ["a:a", "a:b", "a:c", "a:d", "a:e", "a:f"]
    rowlist = client.read_all(date, colkeys)
    s = ('日期设备', '活跃用户', 'VOD用户','应用激活用户', \
                  '智能激活用户', '首次缓冲3秒内(含)', '每次播放卡顿2次内')


if __name__ == "__main__":
    date = "20140102"
    if len(sys.argv) == 2:
        key = sys.argv[1]
    get_daily_data(date)

