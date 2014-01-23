#!/usr/bin/env bash

cd `dirname $0`
python dailytask.py 20131225 20140122 > /var/tmp/tasklog/dailytask/$(date +%Y%m%d%H).log 2>&1
