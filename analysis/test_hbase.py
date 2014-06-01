#!/usr/bin/python
# -*- coding: utf-8 -*- 
import sys

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import *

class NotFoundTable(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class HbaseTest:
    def __init__(self, address, port, table):
        self.tableName = table
        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(address, port).setTimeout(10))
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(self.protocol)
        self.transport.open()
        tables = self.client.getTableNames()
        if self.tableName not in tables:
            raise NotFoundTable(self.tableName)
            
    def __del__(self):
        self.transport.close()

test = HbaseTest("10.0.4.10", "9090", "daily_result")

