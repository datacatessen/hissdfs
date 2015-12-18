import random, os
from Utils import connect

"""
def connect(address):
    hostname, port = split_hostport(address)

    try:
        a = rpyc.connect(str(hostname), int(port))
        return a
    except Exception as e:
        logging.warning(' '.join(['There was a problem connecting to', hostname, str(port)]))
        raise e
"""


def run_client(config, args):
    if len(args) == 0:
        print "<cmd> [opts...]"
        return 1

    exit_code = 0
    client = Client(config)
    cmd = args[0]
    file = args[1] if len(args) >= 2 else None
    dst = args[2] if len(args) == 3 else (args[1] if len(args) == 2 else None)

    if cmd == "ping":
        print client.ping()
    elif cmd == "exists":
        print client.exists(file)
    elif cmd == "ls":
        for f in client.ls():
            print f
    elif cmd == "rm":
        if file is not None:
            client.rm(file)
        else:
            print "usage: rm <file>"
    elif cmd == "touch":
        if file is not None:
            client.touch(file)
        else:
            print "usage: touch <file>"
    elif cmd == "put":
        if file is not None and dst is not None:
            client.put(file, dst)
        else:
            print "usage: put <src> <dst>"
    elif cmd == "cat":
        if file is not None:
            client.cat(file)
        else:
            print "usage: cat <file>"
    else:
        print "unknown command %s" % cmd
        exit_code = 1

    return exit_code


class Client:
    _block_size = 1024

    def __init__(self, config):
        self.config = config
        self.conn = connect(config["nameserver.address"])

    def ping(self):
        return self.conn.root.ping()

    def exists(self, file):
        return self.conn.root.exists(file)

    def ls(self):
        return self.conn.root.ls()

    def rm(self, file):
        self.conn.root.rm(file)

    def touch(self, file):
        self.conn.root.touch(file)

    def put(self, src, dst):
        bytesWritten = 0
        numBytes = os.path.getsize(src)
        f = open(src, 'r')
        while bytesWritten < numBytes:
            block_info = self.conn.root.new_block(dst)
            data_conn = connect(block_info["address"])

            payload = f.read(self._block_size)
            if payload == "": break

            try:
                data_conn.root.write(block_info["id"], payload)
                bytesWritten += self._block_size
            except:
                self.conn.root.rm(dst)
                raise Exception("Failed to write %s to %s, removed" % (src, dst))

    def cat(self, file):
        block_info = self.conn.root.fetch_metadata(file)
        for blk_id in block_info:
            if len(block_info[blk_id]) > 0:
                host = random.sample(block_info[blk_id], 1)[0]
                data_conn = connect(host)
                print data_conn.root.read(blk_id)
            else:
                raise Exception("No hosts available for block %s" % file)
