#coding:utf-8

import socket
import traceback
import sys

def send(host, port, fn):
    
    with open(fn, 'rb') as fp:
        msglist = fp.read().split("\n")      

    if len(msglist) < 5000000:

        for msg in msglist:
        
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((host, int(port)))
                sock.send(msg)
                sock.close()
            except Exception, e:
                traceback.print_exc()

if __name__ == "__main__":

    if 4 != len(sys.argv):

        print 'usage\n$ python {0} <host> <port> <fn>'.format(sys.argv[0])

    else:

        send(sys.argv[1], sys.argv[2], sys.argv[3])

