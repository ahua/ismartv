#!/usr/bin/env python

with open("app.txt") as fp:
    for li in fp:
        vs = li.rstrip().split(",")
        for i in vs:
            if i:
                print i
