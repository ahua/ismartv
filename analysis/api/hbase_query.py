#!/usr/bin/python
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
        rowlist = self.client.scannerGetList(scannerId, 100000)
        self.client.scannerClose(scannerId)
        return rowlist


def get_device_list(devices):
    #devices = devices.upper()
    if devices == "ALL":
        return ['A11', 'A21',
                'DS70A',
                'E31',
                'K72', 'K82','K91',
                'S31', 'S51', 'S61', 
                'LX750A', 'LX755A', 'LX850A']
    #return devices.split(",")
    return [i.upper() for i in devices]

def get_channel_list(channels):
    if channels == "ALL":
        return ['child', 'chnfilm', 'entertain', 'life',
                'music', 'ovsfilm', 'sports', 'teleplay']
    return [i.lower() for i in channels]

def format_res(res):
    for k in res:
        if type(res[k]) == float:
            res[k] = round(res[k], 2)

HBASE_ADDR = "10.0.4.10"

def get_daily_data(date, devices='ALL'):
    device_list = get_device_list(devices)

    client = HbaseInterface(HBASE_ADDR, "9090", "daily_result")    
    colkeys = ["a:a", "a:b", "a:c", "a:d", "a:e", "a:f", "a:g", "a:h"]
    rowlist = client.read_all(date, colkeys)
    s = ('日期设备', '累计用户', '新增用户', '活跃用户', 'VOD用户',\
             'VOD播放次数', 'VOD播放总时长', '应用激活用户', '智能激活用户')
    d = {}
    for dev in device_list:
        d[dev] = {}
        for k in colkeys:
            d[dev][k] = 0

    for r in rowlist:
        device = r.row[8:]
        d[device] = {}
        for k in colkeys:
            d[device][k] = float(r.columns[k].value) if k in r.columns else 0
            
    sn_total = math.fsum([d[dev]["a:a"] for dev in device_list])
    sn_new = math.fsum([d[dev]["a:b"] for dev in device_list])
    sn_active = math.fsum([d[dev]["a:c"] for dev in device_list])
    sn_vod_load = math.fsum([d[dev]["a:d"] for dev in device_list])
    dr_per_sn = math.fsum([d[dev]["a:f"] for dev in device_list]) / (sn_vod_load * 60)\
        if sn_vod_load > 0 else 0
    load_per_active = sn_vod_load / sn_active * 100 if sn_active > 0 else 0
    active_per_total = sn_active / sn_total  * 100 if sn_total > 0 else 0
    
    sn_play_count = math.fsum([d[dev]["a:e"] for dev in device_list])
    play_count_per_sn = sn_play_count / sn_vod_load if sn_vod_load > 0 else 0
    t =  {"time": date,
            "sn_total": sn_total,            # 累计用户
            "sn_new": sn_new,                # 新增用户
            "sn_active": sn_active,          # 活跃用户
            "sn_vod_load": sn_vod_load,      # VOD用户数
            "sn_play_count": sn_play_count,  # VOD播放次数(播放量)
            "dr_per_sn": dr_per_sn,          # VOD户均时长(分钟)
            "play_count_per_sn": play_count_per_sn,# VOD户均访次
            "load_per_active": load_per_active,    # 激活率(VOD)
            "active_per_total": active_per_total,  # 开机率(VOD)
            }
    format_res(t)
    return t

def get_weekly_data(date, devices='ALL'):
    device_list = get_device_list(devices)

    client = HbaseInterface(HBASE_ADDR, "9090", "weekly_result")    
    colkeys = ["a:a", "a:b", "a:c", "a:d"]
    rowlist = client.read_all(date, colkeys)
    s = ('日期设备', '活跃用户', 'VOD用户', '应用激活用户', '智能激活用户')

    d = {}
    for dev in device_list:
        d[dev] = {}
        for k in colkeys:
            d[dev][k] = 0

    for r in rowlist:
        device = r.row[8:]
        d[device] = {}
        for k in colkeys:
            d[device][k] = float(r.columns[k].value) if k in r.columns else 0
            
    sn_active = math.fsum([d[dev]["a:a"] for dev in device_list])
    sn_vod_load = math.fsum([d[dev]["a:b"] for dev in device_list])
    load_per_active = sn_vod_load / sn_active * 100 if sn_active > 0 else 0
    
    week_sn_total = 0
    week_sn_play_count = 0
    week_sn_active = 0
    week_sn_vod_load = 0
    week_dr = 0
    week_load_per_active = 0

    someday = datetime.datetime.strptime(date, "%Y%m%d")
    i = 1
    while i <= 7:
        someday = someday - datetime.timedelta(days=1)
        res = get_daily_data(someday.strftime("%Y%m%d"), devices)
        week_sn_active += res["sn_active"]
        week_sn_vod_load += res["sn_vod_load"]
        week_sn_play_count += res["sn_play_count"]
        week_sn_total += res["sn_total"]
        week_dr += res["dr_per_sn"] * res["sn_vod_load"]        
        i += 1
    
    t = {"time": someday.strftime("%Y%m%d"),
         "sn_active": sn_active,          # 周活跃用户
         "sn_vod_load": sn_vod_load,      # 周VOD用户数
         "week_sn_play_count": week_sn_play_count, # 周VOD播放次数(播放量)
         "load_per_active": load_per_active,   # 周激活率(VOD)
         "sn_active_per_day": week_sn_active/7,    # 日均活跃用户数
         "sn_vod_load_per_day": week_sn_vod_load/7,  # 日均vod用户数
         "play_count_per_day": week_sn_play_count/7,  # 日均播放量
         "dr_per_sn_day": week_dr/(week_sn_vod_load) \
             if week_sn_vod_load > 0 else 0,  # 日均户均时长(分钟)
         "play_count_per_sn": week_sn_play_count/week_sn_vod_load \
             if week_sn_vod_load > 0 else 0,  # 日均VOD户均访次
         "load_per_active_day": week_sn_vod_load/week_sn_active*100\
             if week_sn_active > 0 else 0,   # 日均VOD激活率
         "active_per_total_day": week_sn_active/week_sn_total * 100 \
             if week_sn_total > 0 else 0     # 日均开机率
         }

    format_res(t)
    return t

def get_monthly_data(date, devices='ALL'):
    device_list = get_device_list(devices)

    client = HbaseInterface(HBASE_ADDR, "9090", "monthly_result")    
    colkeys = ["a:a", "a:b", "a:c", "a:d", "a:e", "a:f"]
    rowlist = client.read_all(date, colkeys)
    s = ('日期设备', '活跃用户', 'VOD用户','应用激活用户', \
                  '智能激活用户', '首次缓冲3秒内(含)', '每次播放卡顿2次内')

    d = {}
    for dev in device_list:
        d[dev] = {}
        for k in colkeys:
            d[dev][k] = 0

    for r in rowlist:
        device = r.row[6:]
        d[device] = {}
        for k in colkeys:
            d[device][k] = float(r.columns[k].value) if k in r.columns else 0

    sn_active = math.fsum([d[dev]["a:a"] for dev in device_list])
    sn_vod_load = math.fsum([d[dev]["a:b"] for dev in device_list])
    buffer_3 = math.fsum([d[dev]["a:e"] for dev in device_list])
    block_2 = math.fsum([d[dev]["a:f"] for dev in device_list])
    load_per_active = sn_vod_load / sn_active * 100if sn_active > 0 else 0
    
    month_sn_active = 0
    month_sn_vod_load = 0
    month_sn_play_count = 0
    month_dr = 0
    month_load_per_active = 0
    month_sn_total = 0

    someday = datetime.datetime.strptime(date + "01", "%Y%m%d")
    while someday.strftime("%Y%m") == date:
        res = get_daily_data(someday.strftime("%Y%m%d"), devices)
        month_sn_active += res["sn_active"]
        month_sn_vod_load += res["sn_vod_load"]
        month_sn_play_count += res["sn_play_count"]
        month_dr += res["dr_per_sn"] * res["sn_vod_load"]        
        month_sn_total += res["sn_total"]
        someday = someday + datetime.timedelta(days=1)
    
    t = {"time": date,
         "sn_active": sn_active,          # 月活跃用户
         "sn_vod_load": sn_vod_load,      # 月VOD用户数
         "month_sn_play_count": month_sn_play_count, # 月播放量
         "load_per_active": load_per_active,   # 月激活率(VOD)
         "sn_active_per_day": month_sn_active/7,    # 日均活跃用户数
         "sn_vod_load_per_day": month_sn_vod_load/7,  # 日均vod用户数
         "sn_play_count_per_day": month_sn_play_count/30, # 日均播放量
         "dr_per_sn_day": month_dr/(month_sn_vod_load*7*60) \
             if month_sn_vod_load > 0 else 0,  # 日均户均时长(分钟
         "play_count_per_sn": month_sn_play_count/month_sn_vod_load \
             if month_sn_vod_load > 0 else 0 ,         # 日均VOD户均访次
         "load_per_active_day": month_sn_vod_load/(7*month_sn_active)*100\
             if month_sn_active > 0 else 0,   # 日均VOD激活率
         "sn_vod_play_count_per_day": month_sn_play_count/30, # 日均VOD访问次数
         "active_per_total_day": month_sn_active/month_sn_total* 100 \
             if month_sn_total > 0 * 100 else 0,     # 日均开机率
         "buffer_3_per_total":buffer_3 / month_sn_play_count * 100\
             if month_sn_play_count > 0 else 0, # 首次缓冲3秒内(含)占比
         "block_2_per_total":block_2 / month_sn_play_count * 100\
             if month_sn_play_count > 0 else 0  # 每次播放卡顿2次内占比 
         }
    format_res(t)
    return t

def get_daily_channel(date, channels='ALL', devices='ALL'):
    device_list = get_device_list(devices)
    channel_list = get_channel_list(channels)
    key_list = []
    for dev in device_list:
        for chnl in channel_list:
            key_list.append(dev + chnl)

    client = HbaseInterface(HBASE_ADDR, "9090", "daily_channel")    
    colkeys = ["a:a", "a:b", "a:c"]
    rowlist = client.read_all(date, colkeys)
    s = ('日期设备', 'VOD用户', 'VOD播放次数', 'VOD播放总时长')
    d = {}
    for key in key_list:
        d[key] = {}
        for colkey in colkeys:
            d[key][colkey] = 0

    for r in rowlist:
        key = r.row[8:]
        d[key] = {}
        for colkey in colkeys:
            d[key][colkey] = float(r.columns[colkey].value) if colkey in r.columns else 0
            
    sn_vod_load = math.fsum([d[key]["a:a"] for key in key_list])
    sn_play_count = math.fsum([d[key]["a:b"] for key in key_list])
    dr_per_sn = math.fsum([d[key]["a:c"] for key in key_list]) / (sn_vod_load * 60)\
        if sn_vod_load > 0 else 0
    play_count_per_sn = sn_play_count / sn_vod_load if sn_vod_load > 0 else 0


    sn_vod_load_total = get_daily_data(date, devices)["sn_vod_load"]
    load_per_channel = sn_vod_load / sn_vod_load_total * 100 if sn_vod_load_total > 0 else 0
    
    t =  {"time": date,
          "sn_vod_load": sn_vod_load,      # VOD用户数
          "sn_play_count": sn_play_count,  # VOD播放次数(播放量)
          "dr_per_sn": dr_per_sn,          # VOD户均时长(分钟)
          "play_count_per_sn": play_count_per_sn,# VOD户均访次
          "load_per_channel": load_per_channel,    # 频道激活率(VOD)
          }
    format_res(t)
    return t


def get_weekly_channel(date, channels='ALL', devices='ALL'):
    device_list = get_device_list(devices)
    channel_list = get_channel_list(channels)
    key_list = []
    for dev in device_list:
        for chnl in channel_list:
            key_list.append(dev + chnl)

    client = HbaseInterface(HBASE_ADDR, "9090", "weekly_channel")
    colkeys = ["a:a", "a:b"]
    rowlist = client.read_all(date, colkeys)
    s = ('日期设备', 'VOD用户', '播放量')

    d = {}
    for key in key_list:
        d[key] = {}
        for colkey in colkeys:
            d[key][colkey] = 0

    for r in rowlist:
        key = r.row[8:]
        d[key] = {}
        for colkey in colkeys:
            d[key][colkey] = float(r.columns[colkey].value) if colkey in r.columns else 0

    sn_vod_load = math.fsum([d[key]["a:a"] for key in key_list])
    sn_play_count = math.fsum([d[key]["a:b"] for key in key_list])
    
    week_dr_per_sn = 0
    week_play_count_per_sn = 0
    week_load_per_channel = 0
    someday = datetime.datetime.strptime(date, "%Y%m%d")
    i = 1
    while i <= 7:
        someday = someday - datetime.timedelta(days=1)
        res = get_daily_channel(someday.strftime("%Y%m%d"), channels, devices)
        week_dr_per_sn += res["dr_per_sn"]
        week_play_count_per_sn += res["play_count_per_sn"]
        week_load_per_channel += res["load_per_channel"]
        i += 1

    sn_vod_load_total = get_weekly_data(date, devices)["sn_vod_load"]
    load_per_channel = sn_vod_load / sn_vod_load_total * 100 if sn_vod_load_total > 0 else 0    

    t = {"time": someday.strftime("%Y%m%d"),
         "sn_vod_load": sn_vod_load,      # 周VOD用户数
         "sn_play_count": sn_play_count, # 周VOD播放次数(播放量)
         "load_per_channel": load_per_channel,   # 周频道激活率(VOD)
         "sn_vod_load_per_day": sn_vod_load/7,  # 日均vod用户数
         "play_count_per_day": sn_play_count/7,  # 日均播放量

         "dr_per_sn_day":  week_dr_per_sn/7,  # 日均户均时长(分钟)
         "play_count_per_sn": week_play_count_per_sn/7,  # 日均VOD户均访次
         "load_per_active_day": week_load_per_channel/7,   # 日均频道VOD激活率
         }

    format_res(t)
    return t    

def get_monthly_channel(date, channels='ALL', devices='ALL'):
    device_list = get_device_list(devices)
    channel_list = get_channel_list(channels)
    key_list = []
    for dev in device_list:
        for chnl in channel_list:
            key_list.append(dev + chnl)

    client = HbaseInterface(HBASE_ADDR, "9090", "monthly_channel")
    colkeys = ["a:a", "a:b"]
    rowlist = client.read_all(date, colkeys)
    s = ('日期设备', 'VOD用户', '播放量')

    d = {}
    for key in key_list:
        d[key] = {}
        for colkey in colkeys:
            d[key][colkey] = 0

    for r in rowlist:
        key = r.row[8:]
        d[key] = {}
        for colkey in colkeys:
            d[key][colkey] = float(r.columns[colkey].value) if colkey in r.columns else 0

    sn_vod_load = math.fsum([d[key]["a:a"] for key in key_list])
    sn_play_count = math.fsum([d[key]["a:b"] for key in key_list])
    
    month_dr_per_sn = 0
    month_play_count_per_sn = 0
    month_load_per_channel = 0

    someday = datetime.datetime.strptime(date + "01", "%Y%m%d")
    while someday.strftime("%Y%m") == date:
        res = get_daily_channel(someday.strftime("%Y%m%d"),channels,  devices)
        month_dr_per_sn += res["dr_per_sn"]
        month_play_count_per_sn += res["play_count_per_sn"]
        month_load_per_channel += res["load_per_channel"]
        someday = someday + datetime.timedelta(days=1)

    sn_vod_load_total = get_monthly_data(date, devices)["sn_vod_load"]
    load_per_channel = sn_vod_load / sn_vod_load_total * 100 if sn_vod_load_total > 0 else 0    

    t = {"time": someday.strftime("%Y%m%d"),
         "sn_vod_load": sn_vod_load,      # 周VOD用户数
         "sn_play_count": sn_play_count, # 周VOD播放次数(播放量)
         "load_per_channel": load_per_channel,   # 周频道激活率(VOD)
         "sn_vod_load_per_day": sn_vod_load/7,  # 日均vod用户数
         "play_count_per_day": sn_play_count/7,  # 日均播放量

         "dr_per_sn_day":  month_dr_per_sn/7,  # 日均户均时长(分钟)
         "play_count_per_sn": month_play_count_per_sn/7,  # 日均VOD户均访次
         "load_per_active_day": month_load_per_channel/7,   # 日均频道VOD激活率
         }

    format_res(t)
    return t    

def get_daily_top(date, channel):
    channel_list = get_channel_list(channel)
    client = HbaseInterface(HBASE_ADDR, "9090", "daily_top")
    colkeys = ["a:channel", "a:title", "a:count", "a:up", "a:item"]
    rowlist = client.read_all(date, colkeys)
    
    d = []
    for r in rowlist:
        t = []
        for colkey in colkeys:
            t.append(r.columns[colkey].value if colkey in r.columns else "0")
        d.append(t)
    
    d = filter(lambda t: t[0] in channel_list, d)
    d = sorted(d, key=lambda t: int(t[2]), reverse=True)
    d = d[0:10]
    res = []
    for i in d:
        res.append({"title": i[1],
                    "count": i[2],
                    "up": i[3]})
    return res


if __name__ == "__main__":
    date = "20140102"
    #print get_daily_data(date)
    #print get_weekly_data("20140207")
    #print get_monthly_data("201401")

    #print get_daily_channel("20140322")
    #print get_weekly_channel("20140328")
    #print get_monthly_channel("201403")
    
    channels =  ['child', 'chnfilm', 'entertain', 'life',
                 'music', 'ovsfilm', 'sports', 'teleplay', 'ALL']
    print get_daily_top("20140327", channels[0:1])
    print "*" * 20
    print get_daily_top("20140327", channels[1:2])
    print "*" * 20
    print get_daily_top("20140327", channels[2:3])
    print "*" * 20
    print get_daily_top("20140327", channels[3:4])
    print "*" * 20
    print get_daily_top("20140327", channels[4:5])
    print "*" * 20
    print get_daily_top("20140327", channels[5:6])
    print "*" * 20
    print get_daily_top("20140327", channels[6:7])
    print "*" * 20
    print get_daily_top("20140327", channels[7:8])
    print "*" * 20
    print get_daily_top("20140327", channels[8])
    print "*" * 20

