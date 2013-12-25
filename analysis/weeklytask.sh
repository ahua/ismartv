#!/usr/bin/env bash

cd `dirname $0`
python weeklytask.py > /var/tmp/tasklog/weeklytask/$(date +%Y%m%d%H).log 2>&1

