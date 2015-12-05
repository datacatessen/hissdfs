#!/usr/bin/python

# System imports
import logging
import rpyc
import sys

# Local imports

root = logging.getLogger()
root.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
root.addHandler(ch)

'''
The namespace maintains a mapping of filename to a list of (block, hostnames) pairs file-> [ (block, [ hostnames ] ) ]
'''
namespace = dict()


def _exists(file_name):
    logging.debug("exists %s" % file_name)
    return file_name in namespace


def _touch(file_name):
    if not _exists(file_name):
        logging.debug("touch %s" % file_name)
        namespace[file_name] = list()
        return True
    else:
        raise OSError.FileExistsError("File %s already exists" % file_name)


def _rm(file_name):
    if _exists(file_name):
        logging.debug("rm %s" % file_name)
        del namespace[file_name]
        return True
    else:
        return False


def _ls():
    logging.debug("ls")
    files = []
    for key in namespace:
        logging.debug("add %s" % key)
        files.append(key)
    return files


class NameServer(rpyc.Service):
    __hostname = "localhost"
    __port = 40404

    def exposed_ping(self):
        return 'pong'

    def exposed_touch(self, file_name):
        return _touch(file_name)

    def exposed_rm(self, file_name):
        return _rm(file_name)

    def exposed_exists(self, file_name):
        return _exists(file_name)

    def exposed_ls(self):
        return _ls()

    def on_connect(self):
        pass

    def on_disconnect(self):
        pass
