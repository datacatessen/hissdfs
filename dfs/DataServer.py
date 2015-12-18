# DataServer.py
import logging, os, re, rpyc, threading
from os import path
from rpyc.utils.server import ThreadedServer
from socket import gethostname
from Utils import cat_host, connect, mkdirp

"""
def cat_host(hostname, port):
    return str(hostname) + ':' + str(port)

def mkdirp(path):
    path = path.strip()
    if len(path) == 0:
        raise ValueError("I can't make a directory with no name")

    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise exc
"""

'''
This is a set of block IDs that are stored on disk of this DataServer
'''
blocks = set()


def _refresh_blocks(path):
    for file in os.listdir(path):
        if re.match(r"^blk_[0-9]+$", file):
            blocks.add(file)
        else:
            logging.warn("Non-block file in storage directory, %s" % file)


def start_rpyc_dataserver(port):
    ThreadedServer(DataServer, port=port).start()


def start_data_service(config, port):
    data_dir = path.join(path.abspath(config['dataserver.root.dir']), 'storage')
    id_file = path.join(path.abspath(config['dataserver.root.dir']), 'id')
    nameserver_address = config['nameserver.address']
    dataserver_address = cat_host(gethostname(), port)
    mkdirp(data_dir)

    if os.path.exists(id_file):
        id = open(id_file, 'r').read()
        _refresh_blocks(data_dir)
    else:
        nameserver_conn = connect(nameserver_address)
        id = nameserver_conn.root.new_ds_id(dataserver_address)
        open(id_file, 'w').write(id)

    logging.info("ID is %s" % id)

    DataServer._id = id
    DataServer._data_dir = data_dir
    DataServer._config = config

    t = threading.Thread(target=start_rpyc_dataserver, args=[port])
    t.daemon = True
    t.start()

    nameserver_conn = connect(nameserver_address)
    nameserver_conn.root.register(id, dataserver_address)
    nameserver_conn.close()

    dataserver_conn = connect(dataserver_address)

    try:
        while t.isAlive():
            dataserver_conn.root.send_heartbeat()
            dataserver_conn.root.send_block_report()
            t.join(3)

    except:
        logging.info("Caught exception, unregistering")
        nameserver_conn = connect(nameserver_address)
        nameserver_conn.root.unregister(id, dataserver_address)


class DataServer(rpyc.Service):
    _config = None
    _ns_conn = None
    _data_dir = None
    _id = None

    def __init__(self, *args):
        super(DataServer, self).__init__(args)
        self._ns_conn = connect(self._config['nameserver.address'])

    def exposed_send_block_report(self):
        self._ns_conn.root.block_report(self._id, blocks)

    def exposed_write(self, blk_id, payload):
        blk_path = path.join(self._data_dir, blk_id)
        if not blk_id in blocks:
            open(blk_path, 'w').write(payload)
            blocks.add(blk_id)
            logging.info("Wrote %d bytes to block %s" % (len(payload), blk_path))
        else:
            raise Exception("Block %s has already been written" % blk_id)

    def exposed_read(self, blk_id):
        opath = path.join(self._data_dir, blk_id)
        if blk_id in blocks:
            return open(opath, 'r').read()
        else:
            raise Exception("Block %s does not exist" % blk_id)

    def exposed_replicate(self, blk_id, dst):
        try:
            opath = path.join(self._data_dir, blk_id)
            conn = connect(dst)
            conn.root.write(blk_id, open(opath).read())
            logging.info("Successfully replicated block %s to %s" % (blk_id, dst))
        except:
            logging.error("Failed to replicate block %s to %s" % (blk_id, dst))

    def exposed_send_heartbeat(self):
        self._ns_conn.root.heartbeat(self._id)
