#!/usr/bin/env bash

cd `dirname $0`
python dailytask.py > /var/tmp/tasklog/dailytask/$(date +%Y%m%d%H).log 2>&1
python get_sn_pos.py > /var/tmp/tasklog/get_sn_pos/$(date +%Y%m%d%H).log 2>&1
python dailychannel.py > /var/tmp/tasklog/dailychannel/$(date +%Y%m%d%H).log 2>&1
python dailytop.py > /var/tmp/tasklog/dailytop/$(date +%Y%m%d%H).log 2>&1

