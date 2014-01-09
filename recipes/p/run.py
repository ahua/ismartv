#coding:utf-8

'''
由于设备固件的缺陷
对于连续剧的连播，跳级时没有对应每集的video_exit和video_start事件上报
只有连续的video_play_load（开始播放缓冲结束）事件
因此需要对该类事件特征进行video_exit&video_start的补充
'''

import sys
from lib.sys_log import SysLog
from datetime import timedelta
import os, os.path as path
from itertools import groupby
from pprint import pprint


def run(in_fn, out_fn):
    out_fd = open(out_fn, 'w')
    
    for x in group(read(in_fn)):
        for i in do_insert(x):
            out_fd.write(i.dumps())
            out_fd.write('\n')
    out_fd.close()
    print 'DONE!'
    
    
def do_insert(es):
    if 1 == len(set(map(lambda x: '{0}_{1}'.format(x.get('item'), x.get('subitem')), es))):
        return ()
    
    if 2 == len(es):
        return ()
        
    rt = []
    
    start_evt = es[0]
    load_evts = es[1:-1]
    load_evts = rm_duplicate_load(load_evts)
    
    for i, cur in enumerate(load_evts):
        if 0 == i:
            continue
        
        start_evt = start_evt if 1 == i else rt[-1]
        
        prev = load_evts[i - 1]
        
        exit = SysLog(cur.get('ts') - timedelta(seconds = cur.get('duration')), prev.get('sn'))
        exit.append_field('event', 'video_exit')
        for k, v in prev.items():
            if k in ('ts', 'sn', 'event', 'duration', 'position'):
                continue
            exit.append_field(k, v)
            
        exit.append_field('duration', (exit.get('ts') - start_evt.get('ts')).seconds)
        exit.append_field('position', (exit.get('ts') - prev.get('ts')).seconds)
        exit.append_field('plus', '1')
        
        start = SysLog(exit.get('ts'), exit.get('sn'))
        start.append_field('event', 'video_start')
        start.append_field('plus', '1')
        for k, v in cur.items():
            if k in ('ts', 'sn', 'event', 'duration', 'position'):
                continue
            start.append_field(k, v)

        rt.append(exit)
        rt.append(start)
    
    return rt
    
    
def rm_duplicate_load(load_evts):
    _ = groupby(load_evts, lambda x: x.get('subitem'))
    
    def __():
        for k, v in _:
            yield tuple(v)[-1]
        
    return tuple(__())
            
            
def dict_syslog(d):
    _ = SysLog(d.get('ts'), d.get('sn'))
    _.copy_into(('ts', 'sn'), d)
    return _
            

def group(it):
    t = []

    for l in it:
        if l.startswith('"'):
            l = l[1:]
        if l.endswith('"'):
            l = l[:-1]
            
        discard, d = SysLog.loads(l, None)
        if discard:
            continue
        
        conv(d, 'duration')
        conv(d, 'position')
        evt = d.get('event')
        
        t.append(d)
        if 'video_exit' == evt:
            yield t
            t = []
            
            
def conv(d, f):
    if f not in d:
        return
    d.update({f: int(d.get(f))})
            
    
def read(log_fn):
    with open(log_fn, 'r') as fd:
        while 1:
            l = fd.readline()
            if not l:
                break
            l = l.strip()
            if not l:
                continue
            yield l
    
    

if __name__ == "__main__":
    if 3 != len(sys.argv):
        print 'usage\n$ python {0} <in_fn> <out_fn>'.format(sys.argv[0])
    else:
        run(sys.argv[1], sys.argv[2])
        
        
