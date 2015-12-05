#!/usr/bin/python

# System imports
import json
import sys
from os import path
from socket import gethostname

from rpyc.utils.server import ThreadedServer
# Local imports
from NameServer import NameServer

required_params = ['nameserver.host', 'nameserver.port']


def _validate(config):
    for key in required_params:
        if not key in config:
            print "missing %s in config" % key
            sys.exit(1)


def start_name_service(port):
    NameServer._hostname = gethostname()
    NameServer._port = int(port)

    s = ThreadedServer(NameServer, port=int(port))
    s.start()


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print "usage: python dcd.py <config> <service>"
        sys.exit(1)

    config_file = sys.argv[1]
    service = sys.argv[2]

    if not path.exists(config_file):
        print "config file %s not found" % config_file
        sys.exit(1)

    config = json.load(open(config_file, 'r'))
    _validate(config)

    if service == "nameserver":
        start_name_service(config['nameserver.port'])
    else:
        print "error: unknown service %s" % service
