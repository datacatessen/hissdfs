#!/usr/bin/python

import json
import random
import sys
from Utils import connect


class Client:
    def __init__(self, config):
        self.config = config
        self.conn = connect(config["nameserver.address"])

    def exists(self, file):
        return self.conn.root.exists(file)

    def cat(self, file):
        if not self.exists(file):
            raise "file %s does not exist" % file

        block_info = self.conn.root.fetch_metadata(file)
        for blk_id in block_info:
            assert isinstance(block_info, dict)
            if len(block_info[blk_id]) > 0:
                host = random.sample(block_info[blk_id], 1)[0]
                data_conn = connect(host)
                return data_conn.root.read(blk_id)
            else:
                raise Exception("No hosts available for block %s" % file)

    def ls(self):
        return self.conn.root.ls()

    def put(self, src, dst):
        if not self.exists(dst):
            block_info = self.conn.root.create(dst)
            data_conn = connect(block_info["address"])
            if not data_conn.root.write(block_info["id"], open(src).read()):
                self.conn.root.rm(dst)
                raise Exception("Failed to write %s to %s, removed" % (src, dst))
        else:
            raise Exception("File %s already exists" % dst)

    def rm(self, file):
        if self.exists(file):
            self.conn.root.rm(file)
        else:
            raise Exception("File '%s' does not exist" % sys.argv[3])


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "usage: python client.py <config> <cmd> [opts...]"
        sys.exit(1)

    exit_code = 0
    config = json.load(open(sys.argv[1], 'r'))
    cmd = sys.argv[2]

    client = Client(config)

    if cmd == "exists":
        file = sys.argv[3]
        exit_code = 0 if client.exists(file) else 1
    elif cmd == "cat":
        if len(sys.argv) == 4:
            file = sys.argv[3]
            print client.cat(file)
        else:
            print "usage: cat <file>"
    elif cmd == "ls":
        for f in client.ls():
            print f
    elif cmd == "put":
        if len(sys.argv) == 4 or len(sys.argv) == 5:
            src = sys.argv[3]
            dst = sys.argv[4] if len(sys.argv) == 5 else sys.argv[3]
            client.put(src, dst)
        else:
            print "usage: put <src> <dst>"
    elif cmd == "rm":
        file = sys.argv[3]
        client.rm(file)
    else:
        print "unknown command %s" % cmd
        exit_code = 1

    sys.exit(exit_code)
