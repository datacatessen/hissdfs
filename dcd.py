#!/usr/bin/python

# System imports
import os.path as path
import sys
from socket import gethostname

from rpyc.utils.server import ThreadedServer
# Local imports
from Utils import _mkdirp
from NameServer import NameServer


def start_name_service(port):
    NameServer._hostname = gethostname()
    NameServer._port = int(port)

    s = ThreadedServer(NameServer, port=int(port))
    s.start()


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print "usage: python dcd.py <service> <port>"
    else:
        service = sys.argv[1]
        port = int(sys.argv[2])

        if service == "nameserver":
            start_name_service(port)
        else:
            print "error: unknown service %s" % service
