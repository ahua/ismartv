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
        self.tsocket = TSocket.TSocket(address, port)
        self.tsocket.setTimeout(30000)
        self.transport = TTransport.TBufferedTransport(self.tsocket)
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

def get_weekly_gameapp(startdate, game_or_app):
    client = HbaseInterface(HBASE_ADDR, "9090", "app_game_weekly")
    colkeys = ["%s:%s" % (game_or_app, i) for i in ["value", "code", "title", "device", "up"]]
    KEYTABLE = {}
    for i in ["value", "code", "title", "device", "up"]:
        v = "%s:%s" % (game_or_app, i)
        KEYTABLE.update({i:v,v:i})
    rowlist = client.read_all(startdate, colkeys)
    res_top50 = {}
    res_top10 = {}
    res_top50_no_sharp = {}
    res_top10_no_sharp = {}

    doclist = []
    doclist_no_sharp = []
    for r in rowlist:
        t = {}
        isright = True
        for k in colkeys:
            if k not in r.columns:
                isright = False
            else:
                t[KEYTABLE[k]] = r.columns[k].value
        if isright:
            doclist.append(t)
        if t["device"] not in ["DS70A", "LX750A", "LX755A", "LX850A"]:
            doclist_no_sharp.append(t)
    res_top50 = sorted(doclist, key=lambda x:float(x["value"]), reverse=True)[0:50]
    res_top10 = sorted(doclist, key=lambda x:float(x["up"]), reverse=True)[0:10]
    res_top50_no_sharp = sorted(doclist_no_sharp, key=lambda x:float(x["value"]), reverse=True)[0:50]
    res_top10_no_sharp = sorted(doclist_no_sharp, key=lambda x:float(x["up"]), reverse=True)[0:10]
    return {"startday": startdate,
            "top50": res_top50,
            "top10": res_top10,
            "top50_no_sharp": res_top50_no_sharp,
            "top10_no_sharp": res_top10_no_sharp}


def get_daily_gameapp(date):
    client = HbaseInterface(HBASE_ADDR, "9090", "daily_result")    
    colkeys = ["a:c", "a:i", "a:j"]
    rowlist = client.read_all(date, colkeys)
    s = ('联网用户总数','应用活跃用户','game活跃用户')
    res = {"date": date,
           "a:c": 0,
           "a:i": 0,
           "a:j": 0}
    res_no_sharp = {"date": date,
                    "a:c": 0,
                    "a:i": 0,
                    "a:j": 0}
    for r in rowlist:
        device = r.row[8:]
        for k in colkeys:
            res[k] += int(r.columns[k].value) if k in r.columns else 0
        if device not in ["DS70A", "LX750A", "LX755A", "LX850A"]:
            for k in colkeys:
                res_no_sharp[k] += int(r.columns[k].value) if k in r.columns else 0
            
    t = {"date": date,
         "sn_online": res["a:c"],
         "app_active": res["a:i"],
         "game_active": res["a:j"],
         "sn_online_no_sharp": res_no_sharp["a:c"],
         "app_active_no_sharp": res_no_sharp["a:i"],
         "game_active_no_sharp": res_no_sharp["a:j"]
         }
    return t

def get_device_list(devices):
    #devices = devices.upper()
    if devices == "ALL":
        return ['A11', 'A21',
                'DS70A',
                'E31', 'E62', 'UD10A', 'LX960A',
                'K72', 'K82','K91',
                'S31', 'S51', 'S61', 
                'LX750A', 'LX755A', 'LX850A',
                'S9', "U1", "S52", "S1"]
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

def format_res_ary(res):
    for k in res:
        for m in k:
          if type(k[m]) == float:
             k[m] = round(k[m], 2)

def format_res_list(res):
    for k in res:
        if type(res[k]) != dict:
            continue
        for m in res[k]:
           if type(res[k][m]) == float:
               res[k][m] = round(res[k][m], 2)

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

    t = {}
    try:
        week1 = get_weekly_sum_data(docs, periods[1][0], periods[1][1])
        week2 = get_weekly_sum_data(docs, periods[2][0], periods[2][1])
        week3 = get_weekly_sum_data(docs, periods[3][0], periods[3][1])

        for k in week1:
            t[k] = (week1[k]/week2[k] + week2[k]/week3[k]) * (docs[6][k] + docs[13][k] + docs[20][k]) / 6.0
    except:
        t = {"sn_new": 0,
             "sn_active": 0,
             "sn_vod_load": 0,
             "sn_play_count": 0,
             "dr_per_sn": 0,
             "play_count_per_sn": 0,
             "load_per_active": 0,
             "active_per_total": 0,
             }
    t["time"] = datetime.datetime.now().strftime("%Y%m%d")
    t["sn_total"] = docs[0]["sn_total"] + t["sn_new"]
    format_res(t)
    t["sn_total"] = int(t["sn_total"])
    t["sn_new"] = int(t["sn_new"])
    t["sn_active"] = int(t["sn_active"])
    t["sn_vod_load"] = int(t["sn_vod_load"])
    t["sn_play_count"] = int(t["sn_play_count"])
    return t


def get_sn_info():
    client = HbaseInterface(HBASE_ADDR, "9090", "device_size_count")    
    colkeys = ["a:size", "a:count"]
    rowlist = client.read_all("device", colkeys)
    devices_list = get_device_list('ALL')
    d = []
    for r in rowlist:
        t = {}
        device = r.row[7:-3]
        if device in devices_list:
            t["device"] = device
            for k in colkeys:
                t["size" if k == "a:size" else "percent"] = r.columns[k].value
            d.append(t)
    return d
    

def get_search_word(date=None):
    if not date:
        date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    client = HbaseInterface(HBASE_ADDR, "9090", "daily_search")    
    colkeys = ["a:c", "a:q"]
    rowlist = client.read_all(date, colkeys)
    d = []
    for r in rowlist:
        t = {}
        for k in colkeys:
            t[k[-1]] = r.columns[k].value
        d.append(t)
    return d


def get_daily_data(date, devices='ALL'):
    device_list = get_device_list(devices)

    client = HbaseInterface(HBASE_ADDR, "9090", "daily_result")    
    colkeys = ["a:a", "a:b", "a:c", "a:d", "a:e", "a:f", "a:g", "a:h", "a:k"]
    rowlist = client.read_all(date, colkeys)
    s = ('日期设备', '累计用户', '新增用户', '活跃用户', 'VOD用户',\
             'VOD播放次数', 'VOD播放总时长', '应用激活用户', '智能激活用户', '户均开机时间')
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

    system_on_per_sn = math.fsum([d[dev]["a:k"] for dev in device_list])/(sn_active*60)\
        if sn_active > 0 else 0
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
          "system_on_per_sn": system_on_per_sn,  # 户均开机时间
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

def get_weekly_data_by_device(date, devices='ALL'):
    device_list = get_device_list(devices)

    client = HbaseInterface(HBASE_ADDR, "9090", "weekly_result")    
    colkeys = ["a:a", "a:b"]
    rowlist = client.read_all(date, colkeys)
    s = ('日期设备', '活跃用户', 'VOD用户')
    d = {}
    for dev in device_list:
        d[dev] = {}
        for k in colkeys:
            d[dev][k] = 0

    for r in rowlist:
        device = r.row[8:]
        if device in device_list:
            d[device] = {}
            for k in colkeys:
                d[device][k] = float(r.columns[k].value) if k in r.columns else 0
    
    t = {"time": (datetime.datetime.strptime(date, "%Y%m%d") \
                      - datetime.timedelta(days=7)).strftime("%Y%m%d")}
    t.update(d)

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
                   "avg_loads": type_tag['avg_loads']/type_tag['count_loads']  if type_tag['count_loads'] >0 else 0,  #平均缓冲时长
          }
        format_res(t[type])
    #print t
    return t


def get_cdn_name(id):
    cdn_map = {'3':'奇艺','1':'视云','2':'网宿','4':'蓝汛','5':'帝联','8':'其他'}
    if id in cdn_map:
       return cdn_map[id]
    else:
       return u'其他'

def get_prov_name(id):
    pro_map = {"HE":"河北","TJ":"天津","BJ":"北京","SX":"山西","NM":"内蒙古","LN":"辽宁","JL":"吉林","HL":"黑龙江","SH":"上海","JS":"江苏","ZJ":"浙江","AH":"安徽","FJ":"福建","JX":"江西","SD":"山东","HA":"河南","HB":"湖北","HN":"湖南","GD":"广东","GX":"广西","HI":"海南","CQ":"重庆","SC":"四川","GZ":"贵州","YN":"云南","XZ":"西藏","SN":"陕西","GS":"甘肃","QH":"青海","NX":"宁夏","XJ":"新疆","TW":"台湾","HK":"香港","MO":"澳门","OTHER":"OTHER"}
    id = id.upper()
    if id in pro_map:
       return pro_map[id]
    else:
        return "OTHER"

def get_isp_name(id):
    isp_map = {"CUC":"联通", "CTC":"电信", "CMCC":"移动", "DXT":"电信通", "FBN":"方正宽带", "GHBN":"歌华有线", "SINNET":"光环新网", "SYCATV":"广电网", "CERNET":"教育网", "CRTC":"铁通", "GDCATV":"视讯宽带", "TWSX":"天威视讯", "YTBN":"油田宽带", "OCN":"有线通", "ZXNET":"中信网络", "OTHER":"OTHER", "GWBN":"长城宽带", "KJNET":"宽捷网络", "WASU":"华数宽带", "DRPENG":"DRPENG", "BJKJ":"BJKJ", "SJHL":"SJHL", "GZEJK":"GZEJK", "PACIFIC":"PACIFIC", "BAIDU":"BAIDU", "QQ":"QQ", "WLGT":"WLGT", "ZHFD":"ZHFD", "EHOMENET":"EHOMENET", "SXKD":"SXKD"}    
    if id in isp_map:
       return isp_map[id]
    else:
       return "OTHER"



def get_prov_cdn(date, cdn='ALL', devices='ALL',prov="ALL",isp="ALL"):
    device_list = []
    if devices == 'ALL':
        device_list = get_device_list(devices)
    else:
        device_list = [i.upper() for i in devices]

    cdn_list = []
    if cdn == "ALL":
        cdn_list = ["1","2","3","5","8"]
    else:
        cdn_list = [i for i in cdn]

    prov_list = []
    if prov == "ALL":
        prov_list = ['ALL']
    else:
        prov_list = [i for i in prov]

    isp_list = []
    if isp == "ALL":
        isp_list = ['ALL']
    else:
        isp_list = [i for i in isp]

    client = HbaseInterface(HBASE_ADDR, "9090", "cdn_prov_quality")    

    colkeys = ["info:bl3",
            "info:lcvv",
            "info:vv",
            "info:vv0",
            "info:total_pltms",
            "info:total_bltms",
            "info:sumloads",
            "info:countloads",
            "info:ycvv"]

    key = date
    rowlist = client.read_all(key, colkeys)
    t = []
    temp_map = {}
    for r in rowlist:
        key_ary = r.row.split(';')

        if key_ary[1] not in device_list:
           continue

        if  key_ary[2] not in cdn_list: 
            continue

        str_prov =  key_ary[3].lower()
        if prov_list[0] == 'ALL' or  str_prov  in prov_list:
            pass
        else:
           continue

        str_sip =  key_ary[4].lower()
        if isp_list[0] == 'ALL' or  str_sip  in isp_list:
            pass
        else:
           continue

        array_d = {}
        array_d['dev'] = key_ary[1]
        array_d['cdn'] =  get_cdn_name(key_ary[2])
        array_d['prov'] = get_prov_name(key_ary[3])
        array_d['isp'] =  get_isp_name(key_ary[4])
        array_d["time"] = date

        temp_d = {}
        colkey = 'info:vv'
        if  'vv' in temp_map :
            temp_map['vv'] += float(r.columns[colkey].value) if colkey in r.columns else 0
        else:
            temp_map['vv'] = float(r.columns[colkey].value) if colkey in r.columns else 0

        colkey = 'info:vv0'
        if  'vv0' in temp_map :
            temp_map['vv0'] += float(r.columns[colkey].value) if colkey in r.columns else 0
        else:
            temp_map['vv0'] = float(r.columns[colkey].value) if colkey in r.columns else 0

        colkey = 'info:bl3'
        if  'bl3' in temp_map :
            temp_map['bl3'] += float(r.columns[colkey].value) if colkey in r.columns else 0
        else:
            temp_map['bl3'] = float(r.columns[colkey].value) if colkey in r.columns else 0

        colkey = 'info:ycvv'
        if  'ycvv' in temp_map :
            temp_map['ycvv'] += float(r.columns[colkey].value) if colkey in r.columns else 0
        else:
            temp_map['ycvv'] = float(r.columns[colkey].value) if colkey in r.columns else 0

        colkey = 'info:lcvv'
        if  'lcvv' in temp_map :
            temp_map['lcvv'] += float(r.columns[colkey].value) if colkey in r.columns else 0
        else:
            temp_map['lcvv'] = float(r.columns[colkey].value) if colkey in r.columns else 0

        colkey = 'info:total_pltms'
        if  'total_pltms' in temp_map :
            temp_map['total_pltms'] += float(r.columns[colkey].value) if colkey in r.columns else 0
        else:
            temp_map['total_pltms'] = float(r.columns[colkey].value) if colkey in r.columns else 0

        colkey = 'info:total_bltms'
        if  'total_bltms' in temp_map :
            temp_map['total_bltms'] += float(r.columns[colkey].value) if colkey in r.columns else 0
        else:
            temp_map['total_bltms'] = float(r.columns[colkey].value) if colkey in r.columns else 0

        colkey = 'info:sumloads'
        if  'sumloads' in temp_map :
            temp_map['sumloads'] += float(r.columns[colkey].value) if colkey in r.columns else 0
        else:
            temp_map['sumloads'] = float(r.columns[colkey].value) if colkey in r.columns else 0
        colkey = 'info:countloads'
        if  'countloads' in temp_map :
            temp_map['countloads'] += float(r.columns[colkey].value) if colkey in r.columns else 0
        else:
            temp_map['countloads'] = float(r.columns[colkey].value) if colkey in r.columns else 0

        colkey = 'info:total_pltms'
        temp_d['total_pltms'] = float(r.columns[colkey].value) if colkey in r.columns else 0
        colkey = 'info:total_bltms'
        temp_d['total_bltms'] = float(r.columns[colkey].value) if colkey in r.columns else 0
        colkey = 'info:sumloads'
        temp_d['sumloads'] = float(r.columns[colkey].value) if colkey in r.columns else 0
        colkey = 'info:countloads'
        temp_d['countloads'] = float(r.columns[colkey].value) if colkey in r.columns else 0
        colkey = 'info:vv'
        temp_d['vv'] = float(r.columns[colkey].value) if colkey in r.columns else 0
        colkey = 'info:vv0'
        temp_d['vv0'] = float(r.columns[colkey].value) if colkey in r.columns else 0
        colkey = 'info:lcvv'
        temp_d['lcvv'] = float(r.columns[colkey].value) if colkey in r.columns else 0
        colkey = 'info:ycvv'
        temp_d['ycvv'] = float(r.columns[colkey].value) if colkey in r.columns else 0
        colkey = 'info:bl3'
        temp_d['bl3'] = float(r.columns[colkey].value) if colkey in r.columns else 0


        array_d['vv'] = temp_d['vv']
        array_d['vv0'] = temp_d['vv0']/temp_d['vv'] if temp_d['vv'] >0 else 0
        array_d['lcvv'] = temp_d['lcvv']/temp_d['vv'] if temp_d['vv'] >0 else 0
        array_d['ycvv'] = temp_d['ycvv']/temp_d['vv'] if temp_d['vv'] >0 else 0
        array_d['bl3']  = temp_d['bl3']/temp_d['vv'] if temp_d['vv'] >0 else 0
        array_d['avgloads'] = temp_d['sumloads']/temp_d['countloads'] if temp_d['countloads'] >0 else 0
        array_d['blpl'] = temp_d['total_bltms']/temp_d['total_pltms'] if temp_d['total_pltms'] >0 else 0
        t.append(array_d)


    
    array_d = {}
    array_d["time"] = date
    array_d['dev'] = 'all'
    array_d['cdn'] = 'all'
    array_d['prov'] = 'all'
    array_d['isp'] = 'all'
    array_d['vv'] = temp_map['vv'] if 'vv' in temp_map else 0
    
    if  array_d['vv'] <=0:
        array_d['vv0'] = 0 
        array_d['lcvv'] = 0
        array_d['ycvv'] = 0
        array_d['bl3'] = 0
        array_d['avgloads'] = 0
        array_d['blpl'] = 0
    else:
        array_d['vv0'] = temp_map['vv0']/temp_map['vv'] if  'vv0' in temp_map else 0
        array_d['lcvv'] = temp_map['lcvv']/temp_map['vv'] if 'lcvv' in temp_map  else 0
        array_d['ycvv'] = temp_map['ycvv']/temp_map['vv'] if  'ycvv' in temp_map   else 0
        array_d['bl3']  = temp_map['bl3']/temp_map['vv'] if  'bl3' in temp_map  else 0

        if 'countloads' in temp_map and temp_map['countloads'] >0:
           array_d['avgloads'] = temp_map['sumloads']/temp_map['countloads'] if  'sumloads' in temp_map else 0
        else:
           array_d['avgloads'] =  0

        if 'total_pltms' in temp_map and temp_map['total_pltms'] >0:
           array_d['blpl'] = temp_map['total_bltms']/temp_map['total_pltms'] if 'total_bltms' in temp_map else 0
        else:
           array_d['blpl'] =  0

    t.append(array_d)
    format_res_ary(t)
    return t

def get_node_cdn(date, cdn='all', devices='all',limit_vv="0"):
    device_list = []
    if devices == 'ALL' or devices == 'all':
        device_list = get_device_list(devices)
    else:
        device_list = [i.upper() for i in devices]
    cdn_list = []
    if cdn == "ALL" or cdn == 'all':
        cdn_list = ["1"]
    else:
        cdn_list = [i for i in cdn]

    client = HbaseInterface(HBASE_ADDR, "9090", "cdn_mip_quality")    

    colkeys = ["bl3",
            "lcvv",
            "vv",
            "vv0",
            "total_pltms",
            "total_bltms",
            "sumloads",
            "countloads",
            "ycvv"]

    key = date + cdn_list[0]
    rowlist = client.read_all(key, colkeys)
    d = {}
    for r in rowlist:
        dev = r.row[9:]
        if dev not in device_list:
           continue
        for colkey in r.columns:
            key_tag,mip = colkey.split(':')
            if mip in d:
               if key_tag in  d[mip]:
                  d[mip][key_tag] += float(r.columns[colkey].value)
               else:
                  d[mip][key_tag] = float(r.columns[colkey].value)

            else :
                 mip_tag = {}
                 mip_tag[key_tag]=float(r.columns[colkey].value)
                 d[mip]=mip_tag
     
    t =  {}
    t["time"] = date
    intlimit_vv = int(limit_vv[0])
    for mip in  d:
       if d[mip]['vv'] <  intlimit_vv:
           continue
       tag_map = {}
       tag_map["vv"] = d[mip]["vv"]

       if  d[mip].has_key("vv0") :
           tag_map["vv0"] = d[mip]["vv0"]/d[mip]["vv"] * 100
       else :
           tag_map["vv0"] = 0
           
       if  d[mip].has_key("lcvv") :
           tag_map["lcvv"] =  d[mip]["lcvv"]/d[mip]["vv"] * 100
       else :
            tag_map["lcvv"] = 0

       if  d[mip].has_key("ycvv")  :
           tag_map["ycvv"] =  d[mip]["ycvv"]/d[mip]["vv"]* 100 
       else :
            tag_map["ycvv"] = 0


       if  d[mip].has_key("bl3") :
           tag_map["bl3"] = d[mip]["bl3"]/d[mip]["vv"] * 100
       else :
           tag_map["bl3"] = 0

       if  d[mip].has_key("total_pltms") and  d[mip]["total_pltms"]> 0:
           tag_map["blpl"] = d[mip]["total_bltms"]/d[mip]["total_pltms"] * 100
       else :
           tag_map["blpl"] = 0

       if  d[mip].has_key("countloads") and  d[mip]["countloads"] > 0:
           tag_map["avgloads"] = d[mip]["sumloads"]/d[mip]["countloads"] * 100
       else :
           tag_map["avgloads"] = 0
       t[mip] = tag_map

    format_res_list(t)
    return t



def get_daily_cdn(date, cdn='all', devices='all'):
    device_list = []
    if devices == 'ALL' or devices == 'all':
        #device_list = ["A21", "E", "K", "LX", "S"]
        device_list = get_device_list(devices)
    else:
        device_list = [i.upper() for i in devices]
    cdn_list = []
    if cdn == "ALL" or cdn == 'all':
        cdn_list = ["1", "2", "3", "5","8"]
    else:
        cdn_list = [i for i in cdn]
    client = HbaseInterface(HBASE_ADDR, "9090", "cdn_quality")    
    colkeys = ["bl3:1",  "bl3:2",  "bl3:3",  "bl3:5","bl3:8",
            "lcvv:1", "lcvv:2", "lcvv:3", "lcvv:5","lcvv:8",
            "uv:1",   "uv:2",   "uv:3",   "uv:5","uv:8",
            "vv:1",   "vv:2",   "vv:3",   "vv:5","vv:8",
            "vv0:1",  "vv0:2",  "vv0:3",  "vv0:5","vv0:8",
            "total_pltms:1",  "total_pltms:2",  "total_pltms:3",  "total_pltms:5","total_pltms:8",
            "total_bltms:1",  "total_bltms:2",  "total_bltms:3",  "total_bltms:5","total_bltms:8",
            "sumloads:1",  "sumloads:2",  "sumloads:3",  "sumloads:5","sumloads:8",
            "countloads:1",  "countloads:2",  "countloads:3",  "countloads:5","countloads:8",
            "ycvv:1", "ycvv:2", "ycvv:3", "ycvv:5","ycvv:8"]
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
    total_pltms = sum_device_cdn_data(d, "total_pltms", device_list, cdn_list)
    total_bltms = sum_device_cdn_data(d, "total_bltms", device_list, cdn_list)
    sumloads = sum_device_cdn_data(d, "sumloads", device_list, cdn_list)
    countloads = sum_device_cdn_data(d, "countloads", device_list, cdn_list)

    t =  {"time": date,
          "uv": uv,
          "vv": vv,
          "vv0": vv0/vv * 100 if vv > 0 else 0, #零缓冲率
          "lcvv": lcvv/vv * 100 if vv > 0 else 0, #流畅率
          "ycvv": ycvv/vv * 100 if vv > 0 else 0, # 异常率
          "bl3": bl3/vv * 100 if vv > 0 else 0 , # 三次以上卡顿率
          "blpl": total_bltms/total_pltms * 100 if total_pltms > 0 else 0 , # 卡播比
          "avgloads": sumloads/countloads  if countloads > 0 else 0  # 平均缓冲时长
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


    daily_data = get_daily_data(date, devices)
    sn_vod_load_total = daily_data["sn_vod_load"]
    sn_active_total = daily_data["sn_active"]

    load_per_channel = sn_vod_load / sn_vod_load_total * 100 if sn_vod_load_total > 0 else 0
    active_per_channel = sn_vod_load / sn_active_total * 100 if sn_active_total > 0 else 0
    
    t =  {"time": date,
          "sn_vod_load": sn_vod_load,      # VOD用户数
          "sn_play_count": sn_play_count,  # VOD播放次数(播放量)
          "dr_per_sn": dr_per_sn,          # VOD户均时长(分钟)
          "play_count_per_sn": play_count_per_sn,# VOD户均访次
          "load_per_channel": load_per_channel,  #频道热度 = 该频道VOD用户数 ÷ 总VOD用户数。
          "active_per_channel": active_per_channel #频道活跃度 = 该频道VOD用户数 ÷ 总活跃用户数。
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
    
    week_sn_vod_load = 0
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
        week_sn_vod_load += res["sn_vod_load"]
        i += 1

    sn_vod_load_total = get_weekly_data(date, devices)["sn_vod_load"]
    load_per_channel = sn_vod_load / sn_vod_load_total * 100 if sn_vod_load_total > 0 else 0    

    t = {"time": someday.strftime("%Y%m%d"),
         "sn_vod_load": sn_vod_load,      # 周VOD用户数
         "sn_play_count": sn_play_count, # 周VOD播放次数(播放量)
         "load_per_channel": load_per_channel,   # 周频道激活率(VOD)
         "sn_vod_load_per_day": int(week_sn_vod_load/7),  # 日均vod用户数
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
