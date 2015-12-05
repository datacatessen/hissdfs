#!/usr/bin/python

# System imports
import logging

import rpyc

'''
The namespace maintains a mapping of filename to a list of (block, hostnames) pairs file-> [ (block, [ hostnames ] ) ]
'''
namespace = dict()

'''
This is a set of dataservers currently connected to the NameServer
'''
dataservers = set()


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


def _register(host, port):
    servername = "%s:%d" % (host, port)
    if servername in dataservers:
        logging.error("Server has already been registered with the service")
        return False
    else:
        dataservers.add(servername)
        logging.info("Registered dataserver at %s:%s" % (host, port))
        logging.info("List of dataservers is %s" % dataservers)
        return True


def _unregister(host, port):
    servername = "%s:%d" % (host, port)
    if not servername in dataservers:
        logging.warning("Call to unregister %s:%s, but no server exists" % (host, port))
        return False
    else:
        logging.info("Unregistered dataserver at %s:%s" % (host, port))
        dataservers.remove(servername)
        return True


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

    def exposed_register(self, host, port):
        return _register(host, port)

    def exposed_unregister(self, host, port):
        return _unregister(host, port)

    def on_connect(self):
        pass

    def on_disconnect(self):
        pass
