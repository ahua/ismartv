#!/usr/bin/env bash

cd `dirname $0`
python monthlytask.py > /var/tmp/tasklog/monthlytask/$(date +%Y%m%d%H).log 2>&1

