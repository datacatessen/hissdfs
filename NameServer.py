# System imports
import collections, logging, random, rpyc, sys, threading, time
from socket import gethostname
# Local imports
from Utils import connect, start_rpyc_server

'''
The namespace maintains a multi-map of filename -> { block id : [ hostnames ] }, 
where the map of block id to hostname list is an OrderedDict
'''
namespace = dict()

block_to_file = dict()

'''
This is a set of dataservers currently connected to the NameServer
'''
dataserver_meta = {}

''' Threaded operations '''


def start_name_service(config):
    port = int(config['nameserver.port'])
    NameServer._hostname = gethostname()
    NameServer._port = port

    t = threading.Thread(target=start_rpyc_server, args=(NameServer, port))
    t.daemon = True
    t.start()
    try:
        while t.isAlive():
            _check_heartbeats()
            t.join(1)
    except KeyboardInterrupt:
        pass


def _check_heartbeats():
    for (servername, heartbeat) in dataserver_meta.items():
        if heartbeat + 10 < time.time():
            logging.warning("Server %s has not check in for 10 seconds, unregistering" % servername)
            _unregister(servername.split(':')[0], int(servername.split(':')[1]))


''' Utility Functions '''


def _random_id():
    return '_'.join(["blk", str(random.randint(0, sys.maxint))])


def _random_dataserver():
    return random.sample(dataserver_meta.keys(), 1)[0]


''' Data Server as Client '''


def _register(host, port):
    servername = "%s:%d" % (host, port)
    if servername not in dataserver_meta:
        dataserver_meta[servername] = time.time()
        logging.info("Registered dataserver at %s:%s" % (host, port))
        logging.info("List of dataservers is %s" % dataserver_meta.keys())
    else:
        logging.warning("Server has already been registered with the service")


def _unregister(host, port):
    servername = "%s:%d" % (host, port)
    if servername not in dataserver_meta:
        logging.warning("Call to unregister %s:%s, but no server exists" % (host, port))
    else:
        logging.info("Unregistered dataserver at %s:%s" % (host, port))
        del dataserver_meta[servername]


''' File System Commands '''


def _create(file_name):
    if not _exists(file_name):
        if len(dataserver_meta) == 0:
            raise Exception("Num dataservers is zero, cannot create file")

        namespace[file_name] = dict()

        blk_id = _random_id()
        block_info = dict()
        block_info['id'] = blk_id
        block_info['host'] = _random_dataserver()
        block_info['file'] = file_name
        print block_info['host']
        ''' Update namespace block mapping to have this new block'''
        namespace[file_name] = collections.OrderedDict({blk_id: set()})
        block_to_file[blk_id] = file_name

        logging.debug("returning block info %s" % block_info)
        return block_info
    else:
        raise Exception("File %s already exists" % file_name)


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
        raise Exception("File %s already exists" % file_name)


def _rm(file_name):
    if _exists(file_name):
        logging.debug("rm %s" % file_name)
        file_blocks = namespace[file_name]
        for blk_id in file_blocks:
            for host in file_blocks[blk_id]:
                ds_conn = connect(host)
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


class NameServer(rpyc.Service):
    def exposed_ping(self):
        return 'pong'

    def exposed_heartbeat(self, host, port):
        servername = "%s:%d" % (host, port)
        logging.debug("Received heartbeat from %s" % servername)
        dataserver_meta[servername] = time.time()
        pass

    def exposed_block_report(self, host, port, report):
        servername = "%s:%d" % (host, port)
        logging.debug("Received block report from %s, %s" % (servername, report))

        for blk_id in report:
            if blk_id in block_to_file:
                namespace[block_to_file[blk_id]][blk_id].add(servername)
            else:
                logging.error("Unknown block %s in report from %s" % (blk_id, servername))
        print namespace

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
