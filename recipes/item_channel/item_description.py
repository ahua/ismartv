#!/usr/bin/env python
#coding=utf-8

__author__ = 'tdeng'
import httplib2
import io
import json
import re

ItemList=[]
number=0


def fetchChannels(entrance):
    try:
        http = httplib2.Http()
        resp, content = http.request(entrance,headers={'Accept':'application/json','User-Agent':'ideatv_K91/1.0 333333333333'})
        channel_list=json.loads(content)
        for channel in channel_list:
            if not str(channel["channel"]).startswith("$"):
                channel_name=channel["name"]
                url=channel["url"]
                fetchSections(channel_name,url)
    except Exception,ex:
        #print ex
        raise ex
def fetchSections(channel,entrance):
    try:
        http = httplib2.Http()
        resp, content = http.request(entrance,headers={'Accept':'application/json','User-Agent':'ideatv_K91/1.0 333333333333'})
        section_list=json.loads(content)
        for section in section_list:
            if not str(section["slug"]).startswith("$"):
                section_name=section["title"]
                url=section["url"]
                fetchItems(channel,section_name,url)
    except Exception,ex:
        #print ex
        raise ex

def fetchItems(channel,section,entrance,page=1):
    try:
        http = httplib2.Http()
        resp, content = http.request(entrance,headers={'Accept':'application/json','User-Agent':'ideatv_K91/1.0 333333333333'})
        data=json.loads(content)
        item_list=data["objects"]
        for item in item_list:
            item_name=item["title"]
            item_pk=item["url"]
            item_score=item["bean_score"]  
            #content= channel+"\t"+section+"\t"+item_name+"\t"+item_pk
            #print "\n" + re.findall('\d+',item_pk)[0] + "," + section + "," + channel
            content = dict(item = re.findall('\d+',item_pk)[0],
                section = section,
                channel = channel,
                score = item_score)
            ItemList.append(content)
        if data["num_pages"]>1 and page==1:
            for page in range(2,data["num_pages"]+1):
                fetchItems(channel,section,entrance+str(page)+"/",page)
    except Exception,ex:
        #print ex
        raise ex

def writeToFile():
    with io.open('/home/deploy/work/Calla/Callat/Callat/lookups/item_description.csv', 'w',encoding="utf-8") as csv_file:
        csv_file.write(unicode("item,section,channel"))
        for item in ItemList:
            csv_file.write(unicode("\n" + item['item'] + "," + item['section'] + "," + item['channel']))
            #file.write(unicode(os.linesep))

def writeAnotherFile():
    itemDict = {}
    for item in ItemList:
        if item['channel']!=u"排行榜":
            itemDict[item['item']] = item['channel']
    with io.open('/home/deploy/work/Calla/Callat/Callat/lookups/channel_description.csv', 'w',encoding="utf-8") as csv_file:
        csv_file.write(unicode("item,channel"))
        for item in itemDict.keys():
            csv_file.write(unicode("\n" + item + "," + itemDict[item]))

def writeScoreFile():
    itemDict = {}
    for item in ItemList:
        if item['score']:
            itemDict[item['item']] = item['score']
    with io.open('/home/deploy/work/Calla/Callat/Callat/lookups/score_description.csv', 'w',encoding="utf-8") as csv_file:
        csv_file.write(unicode("item,score"))
        for item in itemDict.keys():
            csv_file.write(unicode("\n" + item + "," + str(itemDict[item])))


if __name__=="__main__":
    import time
    siteAddress="http://cord.tvxio.com"
    path="/api/tv/channels/"
    import time
    start=time.time()
    #fetchItems(u"éŸ³ä¹<90>",u"æ€€æ—§è€<81>æ­Œ","http://cord.tvxio.com/api/tv/section/huaijiulaoge/")
    fetchChannels(siteAddress+path)
    writeToFile()
    writeAnotherFile()
    writeScoreFile()
    end=time.time()
    #print end-start
