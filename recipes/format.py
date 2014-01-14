#!/usr/bin/env python

import sys
from datetime import datetime
from itertools import imap

s = []
with open(sys.argv[1], "r") as fp:
    for li in fp:
        s.append(li)

for li in s:
    vs = li.rstrip().strip().split("\t")
    d = dict(imap(lambda x: x.split("=", 1), vs[2:]))
    d["time"] = datetime.strptime(vs[0], "%Y-%m-%d %H:%M:%S")
    d["sn"] = vs[1]
    print d
    
