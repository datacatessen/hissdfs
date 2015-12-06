# System imports
import logging, os, threading, time, uuid
from os import path
from socket import gethostname, error as socket_error
import rpyc
# Local imports
from Utils import cat_host, connect, mkdirp, start_rpyc_server

blocks = set()


def start_data_service(config, port):
    data_dir = path.join(path.abspath(config['dataserver.root.dir']), 'storage')
    id_file = path.join(path.abspath(config['dataserver.root.dir']), 'id')

    mkdirp(data_dir)

    nameserver_address = config['nameserver.address']
    dataserver_address = cat_host(gethostname(), port)

    id = None
    if os.path.exists(id_file):
        id = open(id_file, 'r').read()
        logging.info("ID is %s" % id)
    else:
        nameserver_conn = connect(nameserver_address)
        id = nameserver_conn.root.make_id(dataserver_address)
        f = open(id_file, 'w')
        f.write(id)
        f.close()
        logging.info("Created a new ID, %s" % id)

    DataServer._id = id
    DataServer._data_dir = data_dir
    DataServer._config = config

    t = threading.Thread(target=start_rpyc_server, args=(DataServer, port))
    t.daemon = True
    t.start()

    dataserver_conn = None
    while dataserver_conn == None:
        try:
            dataserver_conn = connect(dataserver_address)
            nameserver_conn = connect(nameserver_address)
            nameserver_conn.root.register(id, dataserver_address)
        except socket_error:
            time.sleep(3)
            pass

    try:
        while t.isAlive():
            dataserver_conn.root.send_heartbeat()
            dataserver_conn.root.send_block_report()
            t.join(3)

    except KeyboardInterrupt:
        logging.info("Caught KeyboardInterrupt, unregistering")
        nameserver_conn = connect(nameserver_address)
        nameserver_conn.root.unregister(id, dataserver_address)


def _has_blk(blk_id):
    return blk_id in blocks


def _read(blk_id, opath):
    if _has_blk(blk_id):
        return open(opath, 'r').read()
    else:
        raise Exception("Block %s does not exist" % blk_id)


def _write(blk_id, opath, payload):
    if not _has_blk(blk_id):
        f = open(opath, 'w')
        f.write(payload)
        f.close()
        blocks.add(blk_id)
        logging.info("Wrote %d bytes to block %s" % (len(payload), opath))
        return True
    else:
        raise Exception("Block %s has already been written" % blk_id)


def _rm_blk(blk_id, opath):
    if _has_blk(blk_id):
        os.remove(opath)
        blocks.remove(blk_id)
        logging.debug("Deleted block at %s" % opath)
        return True
    else:
        raise Exception("Block %s does not exist" % blk_id)


class DataServer(rpyc.Service):
    _config = None
    _conn = None
    _data_dir = None
    _id = None

    def __init__(self, *args):
        super(DataServer, self).__init__(args)
        self._conn = connect(self._config['nameserver.address'])

    def exposed_send_heartbeat(self):
        self._conn.root.heartbeat(self._id)

    def exposed_send_block_report(self):
        self._conn.root.block_report(self._id, blocks)

    def exposed_has_blk(self, blk_id):
        return _has_blk(blk_id)

    def exposed_write(self, blk_id, payload):
        opath = path.join(self._data_dir, blk_id)
        return _write(blk_id, opath, payload)

    def exposed_read(self, blk_id):
        opath = path.join(self._data_dir, blk_id)
        return _read(blk_id, opath)

    def exposed_rm_blk(self, blk_id):
        opath = path.join(self._data_dir, blk_id)
        return _rm_blk(blk_id, opath)

    def exposed_replicate(self, blk_id, dst):
        opath = path.join(self._data_dir, blk_id)
        conn = connect(dst)

        if conn.root.write(blk_id, open(opath).read()):
            logging.info("Successfully replicated block %s to %s" % (blk_id, dst))
        else:
            logging.error("Failed to replicate block %s to %s" % (blk_id, dst))

    def on_connect(self):
        pass

    def on_disconnect(self):
        pass
