#!/usr/bin/env python
#coding=utf-8

"""
add to crontab:
1 1 * * * /path/to/item_info.py
"""

import httplib2
import io
import json
import re
import shutil

def fetch_channels(entrance):
    try:
        headers = {'Accept': 'application/json',
                   'User-Agent': 'ideatv_K91/1.0 333333333333'}
        http = httplib2.Http()
        _, content = http.request(entrance, headers=headers)
        channel_list = json.loads(content)
        for channel in channel_list:
            if not str(channel["channel"]).startswith("$") and  str(channel["channel"]).startswith("$") != "rankinglist":
                channel_name = channel["channel"]
                url = channel["url"]
                fetch_sections(channel_name, url)
    except Exception, ex:
        pass
    
def fetch_sections(channel, entrance):
    try:
        headers = {'Accept': 'application/json',
                   'User-Agent': 'ideatv_K91/1.0 333333333333'}
        http = httplib2.Http()
        _, content = http.request(entrance, headers=headers)
        section_list = json.loads(content)
        for section in section_list:
            if not str(section["slug"]).startswith("$"):
                section_name = section["title"]
                url = section["url"]
                fetch_items(channel, section_name, url)
    except Exception, ex:
        pass

itemlist = []

def fetch_items(channel, section, entrance, page=1):
    try:
        headers = {'Accept': 'application/json',
                   'User-Agent': 'ideatv_K91/1.0 333333333333'}
        http = httplib2.Http()
        _, content = http.request(entrance, headers=headers)
        data = json.loads(content)
        item_list = data["objects"]
        for item in item_list:
            item_pk = item["url"]
            item_score = item["bean_score"]
            item_title = item["title"]
            content = dict(item = re.findall('\d+', item_pk)[0],
                           section = section,
                           channel = channel,
                           score = item_score,
                           title = item_title
                           )
            itemlist.append(content)
            #print content
        if data["num_pages"] > 1 and page == 1:
            for page in range(2, data["num_pages"] + 1):
                fetch_items(channel, section, entrance + str(page) + "/", page)
    except Exception, ex:
        pass
    
def save_to_file():
    item_dict = {}
    title_dict = {}
    for item in itemlist:
        if item['channel'] != "rankinglist":
            item_dict[item['item']] = item['channel']
            title_dict[item['item']] = item['title']

    with io.open('./files/itemchannel1.csv', 'w', encoding="utf8") as fp:
        #fp.write(unicode("item,channel\n"))
        for item in item_dict.keys():
            fp.write(item + "," + item_dict[item] + "\n")

    with io.open('./files/itemtitle1.csv', 'w', encoding="utf8") as fp:
        for item in title_dict.keys():
            fp.write(item + "," + title_dict[item] + "\n")

    shutil.copyfile('./files/itemchannel1.csv', './files/itemchannel.csv')
    shutil.copyfile('./files/itemtitle1.csv', './files/itemtitle.csv')
    cp_to_112()
    
def get_channel(item):
    item_dict = {}
    with open("./files/channel_description.csv") as fp:
        for li in fp:
            k, v = li.rstrip().split(",")
            item_dict[k] = v
    return item_dict.get(item, None)

def cp_to_112():
    import os
    os.system("scp -P 36022 ./files/itemchannel.csv deploy@10.0.1.12:/var/tmp/channel")


if __name__ == "__main__":
    API = "http://cord.tvxio.com/api/tv/channels/"
    fetch_channels(API)
    save_to_file()
