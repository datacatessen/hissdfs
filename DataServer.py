#!/usr/bin/python

# System imports
import logging, os, rpyc
from os import path
# Local imports
from Utils import _connect

blocks = set()


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
        self._conn = _connect(self._config['nameserver.host'], self._config['nameserver.port'])

    def exposed_heartbeat(self):
        self._conn.root.heartbeat(self._hostname, self._port)

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
