#!/usr/bin/env bash

cd `dirname $0`
python dailytask.py 20131231 20140101 > /var/tmp/tasklog/dailytask/restore.log 2>&1
