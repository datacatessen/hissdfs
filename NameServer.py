#!/usr/bin/python

# System imports
import errno
import os
import logging
import os.path as path
import socket, sys, rpyc
import time
from socket import gethostname
from rpyc.utils.server import ForkingServer
from rpyc.utils.server import ThreadedServer
# Local imports
import FileHandle
from Utils import _mkdirp

root = logging.getLogger()
root.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
root.addHandler(ch)

namespace = dict()

def _exists(file_name):
    logging.debug("exists %s" % file_name)
    return file_name in namespace


def _touch(file_name):
    if not _exists(file_name):
        logging.debug("touch %s" % file_name)
        namespace[file_name] = dict()
        return True
    else:
        raise OSError.FileExistsError("File %s already exists" % file_name)


def _ls():
    logging.debug("ls")
    files = []
    for key in namespace:
        logging.debug("add %s" % key)
        files.append(key)
    return files


def start_service(port, data_dir):
    data_dir = path.abspath(data_dir)
    _mkdirp(data_dir)

    SlaveServer._hostname = gethostname()
    SlaveServer._port = int(port)
    SlaveServer._data_dir = data_dir

    _mkdirp(path.join(data_dir, 'storage'))

    s = ThreadedServer(SlaveServer, port=int(port))
    s.start()


class SlaveServer(rpyc.Service):
    __hostname = "localhost"
    __port = 40404
    __data_dir = "/tmp"

    def exposed_ping(self):
        return 'pong'

    def exposed_touch(self, file_name):
        return _touch(file_name)

    def exposed_exists(self, file_name):
        return _exists(file_name)

    def exposed_ls(self):
        return _ls()

    def on_connect(self):
        pass

    def on_disconnect(self):
        pass


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print "usage: python nameserver.py <port> <data_dir>"
    else:
        port = int(sys.argv[1])
        data_dir = sys.argv[2]
        start_service(port=port, data_dir=data_dir)
