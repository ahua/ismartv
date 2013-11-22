#!/usr/bin/env python
import sys
sys.path.append("./hive")

from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


class HiveInterface:
    def __init__(self, host="127.0.0.1", port=10000):
        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(host, port))
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = ThriftHive.Client(self.protocol)
        self.transport.open()
       # self.client.execute('ADD jar /usr/lib/hive/lib/hive-contrib-0.10.0-cdh4.4.0.jar')
       # self.client.execute('ADD jar /usr/lib/hive/lib/hive-exec-0.10.0-cdh4.4.0.jar')
       # self.client.execute('ADD jar /usr/lib/hive/lib/hive-builtins-0.10.0-cdh4.4.0.jar')
       # self.client.execute('ADD jar /usr/lib/hive/lib/hive-shims-0.10.0-cdh4.4.0.jar')
       # self.client.execute('ADD jar /usr/lib/hive/lib/hive-common-0.10.0-cdh4.4.0.jar')
       # self.client.execute('ADD jar /usr/lib/hive/lib/hive-serde-0.10.0-cdh4.4.0.jar')
        
    def __del__(self):
        self.transport.close()

    def execute(self, sql):
        try:
            self.client.execute(sql)

            lines = self.client.fetchAll()
            return lines
        except Thrift.TException, tx:
            print '%s' % (tx.message)
            return None


if __name__ == '__main__':
    client = HiveInterface("172.16.1.204")
#    lines = client.execute("select * from daily_result")
    lines = client.execute("select count(1) from daily_logs")
    print lines
