#!/usr/bin/env python

import sys

with open(sys.argv[1], "r") as fp:
    for li in fp:
        print '"%s",'% li.rstrip()
