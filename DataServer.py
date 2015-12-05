#!/usr/bin/python

# System imports
import rpyc

# Local imports

blocks = set()


def _has_blk(blk_id):
    return True


class DataServer(rpyc.Service):
    __hostname = "localhost"
    __port = 40404

    def exposed_has_blk(self, blk_id):
        return _has_blk(blk_id)

    def exposed_put(self, blk_id):
        pass

    def exposed_rm_blk(self, file_name):
        pass

    def on_connect(self):
        pass

    def on_disconnect(self):
        pass
