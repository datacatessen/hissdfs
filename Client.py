#!/usr/bin/python

import logging
import rpyc
import sys
from Utils import _connect

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print "usage: python client.py <host> <cmd> [opt]"
    else:
        cmd = sys.argv[1]

        host = sys.argv[1]
        cmd = sys.argv[2]

        conn = _connect(host)

        if cmd == "exists":
            if conn.root.exists(sys.argv[3]):
                sys.exit(0)
            else:
                sys.exit(1)
        elif cmd == "touch":
            if conn.root.touch(sys.argv[3]):
                sys.exit(0)
            else:
                sys.exit(1)
        elif cmd == "rm":
            if conn.root.rm(sys.argv[3]):
                sys.exit(0)
            else:
                print "file '%s' does not exist" % sys.argv[3]
                sys.exit(1)
        elif cmd == "ls":
            for f in conn.root.ls():
                print f
        else:
            print "unknown command %s" % cmd
