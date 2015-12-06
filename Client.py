#!/usr/bin/python

import random
import sys

from Utils import connect

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print "usage: python client.py <config> <cmd> [opts...]"
        sys.exit(1)

    exit_code = 0
    host = sys.argv[1]
    cmd = sys.argv[2]
    conn = connect(host)

    if cmd == "exists":
        if not conn.root.exists(sys.argv[3]):
            exit_code = 1
    elif cmd == "cat":
        if len(sys.argv) == 4:
            file = sys.argv[3]

            if conn.root.exists(file):
                block_info = conn.root.fetch_metadata(file)
                for blk_id in block_info:
                    assert isinstance(block_info, dict)
                    if len(block_info[blk_id]) > 0:
                        host = random.sample(block_info[blk_id], 1)[0]
                        data_conn = connect(host)
                        print data_conn.root.read(blk_id)
                    else:
                        print "no replicas for %s" % blk_id
            else:
                print "file %s does not exist" % file
                exit_code = 1
        else:
            print "usage: cat <file>"
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
                block_info = conn.root.create(dst)
                data_conn = connect(block_info["address"])

                if not data_conn.root.write(block_info["id"], open(src).read()):
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
