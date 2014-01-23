#!/usr/bin/python

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

class HbaseInterface:
    def __init__(self, address, port, table):
        self.tableName = table
        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(address, port))
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(self.protocol)
        self.transport.open()
        tables = self.client.getTableNames()
        if self.tableName not in tables:
            raise NotFoundTable(self.tableName)
            
    def __del__(self):
        self.transport.close()

    def write(self, key, d):
        mutations = []
        for k,v in d.iteritems():
            mutations.append(Mutation(column=k, value=v))
        self.client.mutateRow(self.tableName, key, mutations, {})


    def read(self, key, columns):
        scannerId = self.client.scannerOpenWithPrefix(self.tableName, key, columns, None)
        rowlist = self.client.scannerGet(scannerId)
        self.client.scannerClose(scannerId)
        if len(rowlist) >= 1:
            return rowlist[0]
        return None

            
if __name__ == "__main__":
    client = HbaseInterface("localhost","9090","daily_result")
    key = "20131230"
    d = {"a:a": "1",
         "a:b": "2",
         "a:c": "3",
         "a:d": "4",
         "a:e": "8",
         "a:f": "9",
         "a:g": "10",
         "a:h": "12",
         "a:i": "12",
         "a:j": "13"}
    #client.write(key, d)
    r = client.read(key, ["a:a", "a:b", "a:c", "a:d", "a:e"])
    if r:
        print r
