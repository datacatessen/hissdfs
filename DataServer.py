# System imports
import logging, os, threading, time
from os import path
from socket import gethostname, error as socket_error

import rpyc
# Local imports
from Utils import connect, mkdirp, start_rpyc_server

blocks = set()


def start_data_service(config, port):
    data_dir = path.join(path.abspath(config['dataserver.data.dir']), 'storage')

    mkdirp(data_dir)

    DataServer._hostname = gethostname()
    DataServer._port = port
    DataServer._data_dir = data_dir
    DataServer._config = config

    t = threading.Thread(target=start_rpyc_server, args=(DataServer, port))
    t.daemon = True
    t.start()

    conn = None
    while conn == None:
        try:
            conn = connect(gethostname(), port)
            _register(config, DataServer._hostname, DataServer._port)
        except socket_error:
            time.sleep(3)
            pass

    try:
        while t.isAlive():
            conn.root.send_heartbeat()
            conn.root.send_block_report()
            t.join(3)

    except KeyboardInterrupt:
        logging.info("Caught KeyboardInterrupt, unregistering")
        _unregister(config, DataServer._hostname, DataServer._port)


def _register(config, host, port):
    nameserver_host = config['nameserver.host']
    nameserver_port = config['nameserver.port']
    conn = connect(nameserver_host, nameserver_port)
    conn.root.register(host, port)


def _unregister(config, host, port):
    nameserver_host = config['nameserver.host']
    nameserver_port = config['nameserver.port']
    conn = connect(nameserver_host, nameserver_port)
    conn.root.unregister(host, port)


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
        logging.debug("Wrote %d bytes to block %s" % (len(payload), opath))
        return True
    else:
        raise OSError.FileExistsError("Block %s has already been written" % blk_id)


def _rm_blk(blk_id, opath):
    if _has_blk(blk_id):
        os.remove(opath)
        blocks.remove(blk_id)
        logging.debug("Deleted block at %s" % opath)
        return True
    else:
        raise OSError.FileExistsError("Block %s does not exist" % blk_id)


class DataServer(rpyc.Service):
    _conn = None
    _data_dir = None
    _hostname = None
    _port = None
    _config = None

    def __init__(self, *args):
        super(DataServer, self).__init__(args)
        self._conn = connect(self._config['nameserver.host'], self._config['nameserver.port'])

    def exposed_send_heartbeat(self):
        self._conn.root.heartbeat(self._hostname, self._port)

    def exposed_send_block_report(self):
        self._conn.root.block_report(self._hostname, self._port, blocks)

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

    def on_connect(self):
        pass

    def on_disconnect(self):
        pass
