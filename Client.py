#!/usr/bin/python

import logging
import rpyc
import sys
from Utils import _connect

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print "usage: python client.py <config> <cmd> [opts...]"
        sys.exit(1)

    exit_code = 0
    host = sys.argv[1]
    cmd = sys.argv[2]
    conn = _connect(host)

    if cmd == "exists":
        if not conn.root.exists(sys.argv[3]):
            exit_code = 1
    elif cmd == "ls":
        for f in conn.root.ls():
            print f
    elif cmd == "put":
        if len(sys.argv) == 4 or len(sys.argv) == 5:
            src = sys.argv[3]
            dst = sys.argv[4] if len(sys.argv) == 5 else sys.argv[3]

            if conn.root.exists(dst):
                print "file %s already exists" % dst
                exit_code = 1
            else:
                print "putting file %s at %s" % (src, dst)
                block_info = conn.root.create(dst)
                data_conn = _connect(block_info['host'])

                if not data_conn.root.write(block_info['id'], open(src).read()):
                    print "failed to write %s to %s, removing" % (src, dst)
                    conn.root.rm(dst)
                    exit_code = 1
        else:
            print "usage: put <src> <dst>"
    elif cmd == "rm":
        if not conn.root.rm(sys.argv[3]):
            print "file '%s' does not exist" % sys.argv[3]
            exit_code = 1
    elif cmd == "touch":
        if not conn.root.touch(sys.argv[3]):
            exit_code = 1
    else:
        print "unknown command %s" % cmd
        exit_code = 1

    sys.exit(exit_code)
