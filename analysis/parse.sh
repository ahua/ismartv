#!/usr/bin/env bash

cd `dirname $0`
python parse.py > /tmp/parse-$(date +%Y%m%d%H).log 2>&1
