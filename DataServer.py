#!/usr/bin/python

# System imports
import logging, os, rpyc
from os import path

# Local imports

blocks = set()


def _has_blk(blk_id):
    return blk_id in blocks


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
    def exposed_has_blk(self, blk_id):
        return _has_blk(blk_id)

    def exposed_write(self, blk_id, payload):
        opath = path.join(self._data_dir, 'storage', blk_id)
        return _write(blk_id, opath, payload)

    def exposed_rm_blk(self, blk_id):
        opath = path.join(self._data_dir, 'storage', blk_id)
        return _rm_blk(blk_id, opath)

    def on_connect(self):
        pass

    def on_disconnect(self):
        pass
