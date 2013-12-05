#coding:utf-8

"""
日志syslog格式转换成json格式转换工具
--------------------------------

支持文件和单条日志的单向转换

_recv_time  日志接收时间，以前没有该字段，默认用事件发生时间补全
_unique_key  事件唯一标识


>>> line_handler("2013-08-09 12:33:09\tdb0cdb2c\tevent=video_exit\ttoken=7453296777391736\titem=225352\tsubitem=165828\tclip=536200\ttitle=好心作怪(3)\tquality=normal\tduration=19\tposition=2147\tspeed=1181.0KByte/s\tcity=福州\tprovince=福建\tisp=联通\tip=175.42.20.28")
{"_type": "normal", "_isp": "\u8054\u901a", "_unique_key": "cbedffdfbe0882092e54d439c14213ba", "clip": "536200", "title": "\u597d\u5fc3\u4f5c\u602a(3)", "ip": "175.42.20.28", "time": "2013-08-09T12:33:09.000Z", "speed": "1181.0KByte/s", "item": "225352", "token": "7453296777391736", "subitem": "165828", "sn": "db0cdb2c", "_province": "\u798f\u5efa", "_recv_time": "2013-08-09T12:56:07.000Z", "duration": "19", "position": "2147", "_city": "\u798f\u5dde", "quality": "normal", "event": "video_exit", "_device": "k91"}
{"ip": "175.42.20.28", "_type": "normal", "_isp": "\u8054\u901a", "_unique_key": "95180469214ad5fc61fe168a83a56140", "clip": "536200", "title": "\u597d\u5fc3\u4f5c\u602a(3)", "speed": "1181.0KByte/s", "_recv_time": "2013-08-09T12:33:09.000Z", "item": "225352", "token": "7453296777391736", "subitem": "165828", "sn": "db0cdb2c", "_province": "\u798f\u5efa", "time": "2013-08-09T12:33:09.000Z", "duration": "19", "position": "2147", "_city": "\u798f\u5dde", "quality": "normal", "event": "video_exit", "_device": "k91"}
"""

import csv
import json
import hashlib
import cPickle

SN_DEV_MAP = {
    "TD01": ["S51", "42"],"TD02": ["S51", "47"],"TD03": ["S51", "55"],
    "RD01": ["S61", "42"],"RD02": ["S61", "47"],"RD03": ["S61", "55"],
    "UD01": ["S31", "39"],"UD02": ["S31", "50"],
    "TD04": ["A21", "32"],"TD05": ["A21", "42"],
    "TD06": ["A11", "32"],"TD07": ["A11", "42"],
    "CD01": ["K82", "60"],
    "CD02": ["LX750A", "46"],"CD03": ["LX750A", "52"],"CD04": ["LX750A", "60"],
    "CD05": ["LX850A", "60"],"CD06": ["LX850A", "70"],"CD07": ["LX850A", "80"],
    "CD08": ["K72", "60"],
    "CD09": ["DS70A", "46"],"CD10": ["DS70A", "52"],"CD11": ["DS70A", "60"],
    "CD12": ["DS755A", "46"],"CD13": ["DS755A", "52"],"CD14": ["DS755A", "60"]
}

def get_device_size(sn):
    if type(sn)==str and sn.isalnum():
        return ["K91","unknown"] if sn.islower() and len(sn) in [6,7,8] else SN_DEV_MAP.get(sn[0:4],["unknown","unknown"])
    return ["unknown","unknown"]

def get_device(sn):
    return get_device_size(sn)[0]

def line_handler(csv_line):
    csv_line = csv_line.decode("utf-8")
    field = csv_line.split('\t')
    json_line_tmp = json_line = dict(time = field[0][:10] + "T" + field[0][11:] + ".000Z",
                     sn = field[1])

    for f in field[2:]:
        if f.split("=")[0] in ["city","province","isp"]:
            json_line_tmp["_"+f.split("=")[0]] = f.split("=")[1]
        else:
            json_line[f.split("=")[0]] = f.split("=")[1]

    json_line.update(_unique_key = hashlib.md5(cPickle.dumps(json_line)).hexdigest(),
                     _type = "normal",
                     _device = get_device(str(field[1])),
                     _recv_time = json_line['time'])
    json_line.update(json_line_tmp)

    return json.dumps(json_line)


def file_handler(csv_file):
    csv_file = file('120516_120522.txt', 'rb')

    for csv_line in csv.reader(csv_file):
        print line_handler(csv_line)

    csv_file.close()

if __name__ == "__main__":
    line_handler("2013-08-09 12:33:09\tdb0cdb2c\tevent=video_exit\ttoken=7453296777391736\titem=225352\tsubitem=165828\tclip=536200\ttitle=好心作怪(3)\tquality=normal\tduration=19\tposition=2147\tspeed=1181.0KByte/s\tcity=福州\tprovince=福建\tisp=联通\tip=175.42.20.28")
    import doctest
    doctest.testmod()