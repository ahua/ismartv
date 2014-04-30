#!/usr/bin/python
# -*- coding: utf-8 -*- 
import sys
import math 
import datetime
import urllib2
import json

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
                'E31', 'E62', 'UD10A', '960A', 'LX960A',
                'K72', 'K82','K91',
                'S31', 'S51', 'S61', 
                'LX750A', 'LX755A', 'LX850A']
    #return devices.split(",")
    return [i.upper() for i in devices]

def get_channel_list(channels):
    if channels == "ALL" or channels == 'all':
        return ['music', 'variety', 'documentary', 'overseas', 'sport', 'comic', 'teleplay', 'chinesemovie']
    return [i.lower() for i in channels]

def format_res(res):
    for k in res:
        if type(res[k]) == float:
            res[k] = round(res[k], 2)

HBASE_ADDR = "10.0.4.10"


def get_weekly_sum_data(docs, start, end):
    res = {"sn_new": 0,
           "sn_active": 0,
           "sn_vod_load": 0,
           "sn_play_count": 0,
           "dr_per_sn": 0,
           "play_count_per_sn": 0,
           "load_per_active": 0,
           "active_per_total": 0,
           }
    i = start
    while i <= end:
        for k in res:
            res[k] += docs[i][k]
        i = i + 1
    return res

def get_daily_prediction_data(docs):
    periods = []
    up = len(docs) - 1
    i = 0
    while i <= up and datetime.datetime.strptime(docs[i]["time"], "%Y%m%d").strftime("%w") == '0':
        i = i + 1
    periods.append([0, i-1])
    while i < up:
        if i + 6 <= up:
            periods.append([i, i+6])
        i += 6

    week1 = get_weekly_sum_data(docs, periods[1][0], periods[1][1])
    week2 = get_weekly_sum_data(docs, periods[2][0], periods[2][1])

    t = {}
    for k in week1:
        t[k] = week1[k]/week2[k] * docs[6][k]
    t["time"] = datetime.datetime.now().strftime("%Y%m%d")
    t["sn_total"] = docs[0]["sn_total"] + t["sn_new"]
    format_res(t)
    t["sn_total"] = int(t["sn_total"])
    t["sn_new"] = int(t["sn_new"])
    t["sn_active"] = int(t["sn_active"])
    t["sn_vod_load"] = int(t["sn_vod_load"])
    t["sn_play_count"] = int(t["sn_play_count"])
    return t

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
         "sn_active_per_day": int(week_sn_active/7),    # 日均活跃用户数
         "sn_vod_load_per_day": int(week_sn_vod_load/7),  # 日均vod用户数
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
         "sn_active_per_day": int(month_sn_active/30),    # 日均活跃用户数
         "sn_vod_load_per_day": int(month_sn_vod_load/30),  # 日均vod用户数
         "sn_play_count_per_day": month_sn_play_count/30, # 日均播放量
         "dr_per_sn_day": month_dr/(month_sn_vod_load) \
             if month_sn_vod_load > 0 else 0,  # 日均户均时长(分钟
         "play_count_per_sn": month_sn_play_count/month_sn_vod_load \
             if month_sn_vod_load > 0 else 0 ,         # 日均VOD户均访次
         "load_per_active_day": month_sn_vod_load/month_sn_active*100\
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


def sum_device_cdn_data(d, colprefix, device_list, cdn_list):
    s = 0
    for dev in device_list:
        for cdn in cdn_list:
            col = colprefix + ":" + cdn
            try:
                s += d[dev][col]
            except:
                pass
    return s


def get_month_cdn(date, cdn='all', devices='all',target='all',line_type='all'):
    device_list = []
    if devices == 'ALL' or devices == 'all':
        devices='ALL'
        device_list = get_device_list(devices)
    else:
        device_list = [i.upper() for i in devices]

    cdn_list = []
    if cdn == "ALL" or cdn == 'all':
        cdn_list = ["1", "2", "3", "5","8"]
    else:
        cdn_list = [i for i in cdn]

    target_list = []
    if target == "ALL" or target == 'all':
        target_list = ["avg_loads","vv0", "yc_lv"]
    else:
        target_list = [i for i in target]


    type_list = []
    if line_type == "ALL" or line_type == 'all':
        type_list = ["day", "high", "low"]
    else:
        type_list = [i for i in line_type]

    client = HbaseInterface(HBASE_ADDR, "9090", "month_cdn_quality")    

    query_col = ["avg_loads","count_loads","vv","vv0","yc"]

    colkeys = []
    for col in query_col:
        for cdn in  cdn_list:
            colkey = col  + ":" + cdn
            colkeys.append(colkey)
    rowlist = client.read_all(date, colkeys)

    #print date,colkeys,rowlist
    d = {}
    for r in rowlist:
        rws = r.row.split(";")
        dev = rws[1]
        type = rws[2]

        if d.has_key(type):
           if not  d[type].has_key(dev):
              d[type][dev]={}
        else:
            d[type] = {}
            d[type][dev]={}

        for colkey in colkeys:
            d[type][dev][colkey] = float(r.columns[colkey].value) if colkey in r.columns else 0

    t = {}
    t['time'] = date
    #print type_list
    #print d
    for type in type_list :
        type_tag = {} 
        for col in  query_col:   
            type_tag[col] = sum_device_cdn_data(d[type], col, device_list, cdn_list)

        t[type] = {
                   "vv0": type_tag['vv0']/type_tag['vv'] * 100 if type_tag['vv'] > 0 else 0, #零缓冲率
                   "yc_lv": type_tag['yc']/type_tag['vv']*100 if type_tag['vv'] > 0 else 0, # 异常率
                   "avg_loads": type_tag['avg_loads']/type_tag['count_loads']   #平均缓冲时长
          }
        format_res(t[type])
    #print t
    return t



def get_daily_cdn(date, cdn='all', devices='all'):
    device_list = []
    if devices == 'ALL' or devices == 'all':
        device_list = ["A21", "E", "K", "LX", "S"]
    else:
        device_list = [i.upper() for i in devices]
    cdn_list = []
    if cdn == "ALL" or cdn == 'all':
        cdn_list = ["1", "2", "3", "5"]
    else:
        cdn_list = [i for i in cdn]
    client = HbaseInterface(HBASE_ADDR, "9090", "cdn_quality")    
    colkeys = ["bl3:1",  "bl3:2",  "bl3:3",  "bl3:5",
               "lcvv:1", "lcvv:2", "lcvv:3", "lcvv:5",
               "uv:1",   "uv:2",   "uv:3",   "uv:5",
               "vv:1",   "vv:2",   "vv:3",   "vv:5",
               "vv0:1",  "vv0:2",  "vv0:3",  "vv0:5",
               "ycvv:1", "ycvv:2", "ycvv:3", "ycvv:5"]
    rowlist = client.read_all(date, colkeys)

    d = {}
    for r in rowlist:
        dev = r.row[8:]
        d[dev] = {}
        for colkey in colkeys:
            d[dev][colkey] = float(r.columns[colkey].value) if colkey in r.columns else 0

    bl3 = sum_device_cdn_data(d, "bl3", device_list, cdn_list)
    lcvv = sum_device_cdn_data(d, "lcvv", device_list, cdn_list)
    uv = sum_device_cdn_data(d, "uv", device_list, cdn_list)
    vv = sum_device_cdn_data(d, "vv", device_list, cdn_list)
    vv0 = sum_device_cdn_data(d, "vv0", device_list, cdn_list)
    ycvv = sum_device_cdn_data(d, "ycvv", device_list, cdn_list)

    t =  {"time": date,
          "uv": uv,
          "vv": vv,
          "vv0": vv0/vv * 100 if vv > 0 else 0, #零缓冲率
          "lcvv": lcvv/vv * 100 if vv > 0 else 0, #流畅率
          "ycvv": ycvv/vv * 100 if vv > 0 else 0, # 异常率
          "bl3": bl3/vv * 100 if vv > 0 else 0  # 三次以上卡顿率
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
         "sn_vod_load_per_day": int(sn_vod_load/7),  # 日均vod用户数
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
         "sn_vod_load": sn_vod_load,      # 月VOD用户数
         "sn_play_count": sn_play_count, # 月VOD播放次数(播放量)
         "load_per_channel": load_per_channel,   # 月频道激活率(VOD)
         "sn_vod_load_per_day": int(sn_vod_load/30),  # 日均vod用户数
         "play_count_per_day": sn_play_count/30,  # 日均播放量

         "dr_per_sn_day":  month_dr_per_sn/30,  # 日均户均时长(分钟)
         "play_count_per_sn": month_play_count_per_sn/30,  # 日均VOD户均访次
         "load_per_active_day": month_load_per_channel/30,   # 日均频道VOD激活率
         }

    format_res(t)
    return t    

ITEM_INFO_CACHE = {}
def get_description_picture(item):
    global ITEM_INFO_CACHE
    try:
        if item in ITEM_INFO_CACHE:
            return ITEM_INFO_CACHE[item]
        url = "http://cord.tvxio.com/api/item/%s/" % item
        s = urllib2.urlopen(url).read()
        j = json.loads(s)
        ITEM_INFO_CACHE[item] = (j["description"], j["adlet_url"], j["title"])
        return j["description"], j["adlet_url"], j["title"]
    except:
        return "", "", ""


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
        description, picture, title = get_description_picture(i[4])
        res.append({
                "title": title if title else i[1],
                "count": i[2],
                "up": i[3],
                "item": i[4],
                "description": description,
                "picture_link": picture})
    return res

def get_year_kpi():
    res = {"load_per_active": 0,
           "dr_per_sn_day": 0
           }
    client = HbaseInterface(HBASE_ADDR, "9090", "daily_result")    
    colkeys = ["a:c", "a:d", "a:f"] #'活跃用户', 'VOD用户', 'VOD播放总时长'
    startday = datetime.datetime(2014, 4, 1)
    endday = datetime.datetime.now() - datetime.timedelta(days=1)
    load_per_active_list = []
    dr_per_sn_day_list = []
    while startday <= endday:
        date = startday.strftime("%Y%m%d")
        startday += datetime.timedelta(days=1)
        rowlist = client.read_all(date, colkeys)
        active_user = 0
        vod_user = 0
        vod_play_time = 0
        for r in rowlist:
            active_user += float(r.columns["a:c"].value) if "a:c" in r.columns else 0
            vod_user += float(r.columns["a:d"].value) if "a:d" in r.columns else 0
            vod_play_time += float(r.columns["a:f"].value) if "a:f" in r.columns else 0
        t1 = vod_user/active_user * 100 if active_user != 0 else 0
        t2 = vod_play_time/(vod_user*60) if vod_user != 0 else 0
        if t1 != 0 or t2 != 0:
            load_per_active_list.append(t1)
            dr_per_sn_day_list.append(t2)
    
    res["load_per_active"] = round(math.fsum(load_per_active_list) / len(load_per_active_list), 2)
    res["dr_per_sn_day"] = round(math.fsum(dr_per_sn_day_list) / len(dr_per_sn_day_list), 2)
    return res


import MySQLdb
#LAUNCH_DB = MySQLdb.connect("10.0.1.13", "pink", "ismartv", "pink2")
#LAUNCH_DB.set_character_set('utf8')
#LAUNCH_CUR = LAUNCH_DB.cursor()

def get_launcher_click(starttime, endtime):
    global LAUNCH_DB, LAUNCH_CUR
    sql = '''select count(*) as click_count, count(distinct(a.sn)) as click_sn, 
                    c.type,
                    a.pk,
                    b.title
             from launcher_vod_click as a, 
                  launcher_vod_click_pk as b,
                  launcher_vod_click_type as c
             where a.type = c.id and a.pk = b.pk
                   and a.time >= '%s' and a.time <= '%s'
             group by a.type, a.pk;
          ''' % (starttime, endtime)
    print sql
    LAUNCH_CUR.execute(sql)
    objs = LAUNCH_CUR.fetchall()
    res = []
    for o in objs:
        picture_link = ""
        if o[2] != "section":
            _, picture_link, _ = get_description_picture(o[3])
        res.append({"click_count": o[0],
                    "click_sn": o[1],
                    "type": o[2],
                    "pk": o[3],
                    "title": o[4],
                    "picture_link": picture_link}
                   )
    return res

if __name__ == "__main__":
    date = "20140102"
    #print get_daily_data(date)
    #print get_weekly_data("20140207")
    #print get_monthly_data("201401")

    #print get_daily_channel("20140322")
    #print get_weekly_channel("20140328")
    #print get_monthly_channel("201403")

    print get_daily_cdn("20140408")
