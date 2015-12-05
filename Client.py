#!/usr/bin/python

import logging
import rpyc
import sys


def _connect(hostname, port=None):
    '''Connects to a rpyc slave. It can either take the form of _connect('1.2.3.4:1234') or _connect('1.2.3.4', 1234)'''

    if port is None:
        hostname, port = hostname.split(':')

    try:
        a = rpyc.connect(str(hostname), int(port))
        return a
    except Exception as e:
        logging.warning(' '.join(['There was a problem connecting to', hostname, str(port)]))

        raise e


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print "usage: python client.py <host> <cmd> [opt]"
    else:
        cmd = sys.argv[1]

        host = sys.argv[1]
        cmd = sys.argv[2]

        conn = _connect(host)

        if "exists" in cmd:
            if conn.root.exists(sys.argv[3]):
                sys.exit(0)
            else:
                sys.exit(1)
        elif "touch" in cmd:
            if conn.root.touch(sys.argv[3]):
                sys.exit(0)
            else:
                sys.exit(1)
        elif "ls" in cmd:
            for f in conn.root.ls():
                print f
        else:
             print "unknown command %s" % cmd
