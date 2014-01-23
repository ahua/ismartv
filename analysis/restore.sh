#!/usr/bin/env bash

cd `dirname $0`
python dailytask.py 20131225 20140122 > /var/tmp/tasklog/dailytask/restore.log 2>&1
