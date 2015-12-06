#!/usr/bin/python

# System imports
import logging, random, rpyc, sys, time
from Utils import _connect

'''
The namespace maintains a multi-map of filename -> block id -> { hostnames }
'''
namespace = dict()

'''
This is a set of dataservers currently connected to the NameServer
'''
dataserver_meta = {}


def _check_heartbeats():
    for (servername, heartbeat) in dataserver_meta.items():
        if heartbeat + 10 < time.time():
            logging.warning("Server %s has not check in for 10 seconds, unregistering" % servername)
            _unregister(servername.split(':')[0], int(servername.split(':')[1]))


def _random_id():
    return '_'.join(["blk", str(random.randint(0, sys.maxint))])


def _random_dataserver():
    return random.choice(dataserver_meta.keys())


def _create(file_name):
    if not _exists(file_name):
        if len(dataserver_meta) == 0:
            raise Exception("Num dataservers is zero, cannot create file")

        namespace[file_name] = dict()

        block_info = dict()
        block_info['id'] = _random_id()
        block_info['host'] = _random_dataserver()
        block_info['file'] = file_name

        ''' Update namespace block mapping to have this new block'''
        namespace[file_name] = {block_info['id']: [block_info['host']]}

        logging.debug("returning block info %s" % block_info)
        return block_info
    else:
        raise OSError.FileExistsError("File %s already exists" % file_name)


def _fetch_metadata(file_name):
    if _exists(file_name):
        logging.debug("fetch_metdata %s" % file_name)
        return namespace[file_name]
    else:
        raise Exception("File %s does not exist" % file_name)


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
        file_blocks = namespace[file_name]
        for blk_id in file_blocks:
            for host in file_blocks[blk_id]:
                ds_conn = _connect(host)
                ds_conn.root.rm_blk(blk_id)
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
    if not servername in dataserver_meta:
        dataserver_meta[servername] = time.time()
        logging.info("Registered dataserver at %s:%s" % (host, port))
        logging.info("List of dataservers is %s" % dataserver_meta.keys())
    else:
        logging.warning("Server has already been registered with the service")


def _unregister(host, port):
    servername = "%s:%d" % (host, port)
    if not servername in dataserver_meta:
        logging.warning("Call to unregister %s:%s, but no server exists" % (host, port))
    else:
        logging.info("Unregistered dataserver at %s:%s" % (host, port))
        del dataserver_meta[servername]


class NameServer(rpyc.Service):
    def exposed_ping(self):
        return 'pong'

    def exposed_heartbeat(self, host, port):
        servername = "%s:%d" % (host, port)
        logging.debug("Received heartbeat from %s" % servername)
        dataserver_meta[servername] = time.time()
        pass

    def exposed_create(self, file_name):
        return _create(file_name)

    def exposed_fetch_metadata(self, file_name):
        return _fetch_metadata(file_name)

    def exposed_touch(self, file_name):
        return _touch(file_name)

    def exposed_rm(self, file_name):
        return _rm(file_name)

    def exposed_exists(self, file_name):
        return _exists(file_name)

    def exposed_ls(self):
        return _ls()

    def exposed_register(self, host, port):
        _register(host, port)

    def exposed_unregister(self, host, port):
        _unregister(host, port)

    def on_connect(self):
        pass

    def on_disconnect(self):
        pass
