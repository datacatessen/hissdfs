# System imports
import copy, json, logging, pickle, random, rpyc, sys, threading, uuid
from collections import OrderedDict
from os import path
from socket import gethostname
# Local imports
from Utils import connect, mkdirp, now, split_hostport, start_rpyc_server

'''
The namespace maintains a multi-map of filename -> { block id : set(server_ids) },
where the map of block id to hostname list is an OrderedDict
'''
namespace = dict()

block_to_file = dict()

'''
This is a set of dataservers currently connected to the NameServer
dictionary is a multi-map of id -> { address : <host:port>, heartbeat : <timestamp>, status : "ALIVE|DEAD" }
'''
dataserver_metadata = {}

''' Threaded operations '''


def start_name_service(config):
    global CONF
    CONF = config
    hostname, port = split_hostport(CONF['nameserver.address'])

    if hostname != gethostname():
        logging.error(
            "Attempt to start nameserver on the wrong node, expected %s but I am %s" % (hostname, gethostname()))

    NameServer._hostname = hostname
    NameServer._port = port

    _load_namespace()

    t = threading.Thread(target=start_rpyc_server, args=(NameServer, port))
    t.daemon = True
    t.start()
    try:
        while t.isAlive():
            _check_heartbeats()
            _check_replication()
            t.join(5)
    except KeyboardInterrupt:
        _save_namespace()
        pass


def _check_heartbeats():
    for (id, metadata) in dataserver_metadata.items():
        assert isinstance(metadata, dict)
        if metadata["status"] != "DEAD" and metadata["heartbeat"] + 10 < now():
            logging.warning("Server %s has not check in for 10 seconds, unregistering" % id)
            _unregister(id, metadata["address"])


def _check_replication():
    num_replicas = 3
    for blocklist in namespace.values():
        assert isinstance(blocklist, OrderedDict)
        for (blk_id, ids) in blocklist.items():
            assert isinstance(ids, set)
            if len(ids) != num_replicas:
                logging.warning("Num replications for block %s is not %d, but %d" % (blk_id, num_replicas, len(ids)))
                _add_replicas(blk_id, ids)


def _add_replicas(blk_id, ids, num_replicas=3):
    if len(ids) == 0:
        logging.error("Unable to replicate %s because there are currently 0 replicas" % blk_id)
        return

    replicas_to_add = min(num_replicas - len(ids), len(dataserver_metadata) - len(ids))

    if replicas_to_add == 0:
        logging.error("Unable to replicate %s because there is no host is available for replication" % blk_id)
        return

    logging.debug("Adding %d replicas for %s" % (replicas_to_add, blk_id))

    src = dataserver_metadata[random.sample(ids, 1)[0]]["address"]
    new_ids = list(ids)

    for i in range(0, replicas_to_add):
        (id, metadata) = _random_dataserver(exclude=new_ids)
        conn = connect(src)
        conn.root.replicate(blk_id, metadata["address"])
        logging.info("Replicating %s from %s to %s" % (blk_id, src, metadata["address"]))
        new_ids.append(id)


def _save_namespace():
    mkdirp(CONF["nameserver.root.dir"])
    f = open(path.join(CONF["nameserver.root.dir"], "fsimage"), 'w')

    ns_copy = copy.deepcopy(namespace)

    # purge the block mappings
    for blockmapping in ns_copy.values():
        for set_hosts in blockmapping.values():
            set_hosts.clear()

    pickle.dump(ns_copy, f)
    pickle.dump(block_to_file, f)
    f.close()
    logging.info("Saved namespace metadata to %s" % f.name)


def _load_namespace():
    file = path.join(CONF["nameserver.root.dir"], "fsimage")
    if path.exists(file) and path.getsize(file) != 0:
        f = open(file, 'r')
        namespace.update(pickle.load(f))
        block_to_file.update(pickle.load(f))
        logging.info("Loaded namespace from %s" % file)
    else:
        logging.info("No namespace to load")


''' Utility Functions '''


def _random_block_id():
    return '_'.join(["blk", str(random.randint(0, sys.maxint))])


def _random_dataserver(exclude=list()):
    id = random.sample(dataserver_metadata.keys(), 1)[0]
    while id in exclude or dataserver_metadata[id]["status"] == "DEAD":
        id = random.sample(dataserver_metadata.keys(), 1)[0]
    return (id, dataserver_metadata[id])


def _get_address_from_id(host_id):
    if host_id in dataserver_metadata:
        return dataserver_metadata[host_id]["address"]
    else:
        return None


''' Data Server as Client '''


def _register(id, address):
    if id not in dataserver_metadata:
        dataserver_metadata[id] = {"address": address, "heartbeat": 0, "status": "DEAD"}
    else:
        if dataserver_metadata[id]["status"] == "DEAD":
            logging.info("Server %s, addr %s is now registered as alive" % (id, address))
            dataserver_metadata[id]["status"] = "ALIVE"
            dataserver_metadata[id]["heartbeat"] = now()
            logging.info(
                "List of dataservers is %s" % json.dumps(dataserver_metadata, indent=4, separators=(',', ':')))
        else:
            logging.warning("Server wants to be registered, but we see it as alive")


def _unregister(id, address):
    if id not in dataserver_metadata:
        logging.warning("Call to unregister server %s, addr %s, but no server exists" % (id, address))
    else:
        if dataserver_metadata[id]["address"] != address:
            logging.error("ID/address mismatch, will not unregister")

        dataserver_metadata[id]["status"] = "DEAD"
        for blocklist in namespace.values():
            for (blk_id, hosts) in blocklist.items():
                assert isinstance(hosts, set)
                if id in hosts:
                    hosts.remove(id)

        logging.info("Server %s, addr %s is now registered as dead and all blocks invalidated" % (id, address))
        logging.info(
            "List of dataservers is %s" % json.dumps(dataserver_metadata, indent=4, separators=(',', ':')))


''' File System Commands '''


def _create(file_name):
    if not _exists(file_name):
        if len(dataserver_metadata) == 0:
            raise Exception("Num dataservers is zero, cannot create file")

        namespace[file_name] = dict()

        blk_id = _random_block_id()
        block_info = dict()
        block_info["id"] = blk_id
        block_info["address"] = _random_dataserver()[1]["address"]
        block_info["file"] = file_name

        ''' Update namespace block mapping to have this new block'''
        namespace[file_name] = OrderedDict({blk_id: set()})
        block_to_file[blk_id] = file_name

        logging.debug("returning block info %s" % block_info)
        return block_info
    else:
        raise Exception("File %s already exists" % file_name)


def _fetch_metadata(file_name):
    if _exists(file_name):
        logging.debug("fetch_metadata %s" % file_name)

        retval = OrderedDict()
        for (blk_id, hosts) in namespace[file_name].items():
            addresses = set()
            for host_id in hosts:
                addresses.add(_get_address_from_id(host_id))

            retval[blk_id] = addresses
        return retval
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

    def exposed_heartbeat(self, id):
        logging.debug("Received heartbeat from %s" % id)
        dataserver_metadata[id]["heartbeat"] = now()
        dataserver_metadata[id]["status"] = "ALIVE"
        pass

    def exposed_block_report(self, id, report):
        logging.debug("Received block report from %s, %s" % (id, report))

        changed = False
        for blk_id in report:
            if blk_id in block_to_file:
                dataserver_set = namespace[block_to_file[blk_id]][blk_id]
                if id not in dataserver_set:
                    dataserver_set.add(id)
                    changed = True
            else:
                logging.error("Unknown block %s in report from %s" % (blk_id, id))

        if changed:
            _save_namespace()

    def exposed_make_id(self, address):
        for id in dataserver_metadata:
            if address == dataserver_metadata[id]["address"]:
                logging.error("Request to make ID for address %s, but one already exists %s" % (address, id))
                return None
        return str(uuid.uuid4())

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

    def exposed_register(self, id, address):
        _register(id, address)

    def exposed_unregister(self, id, address):
        _unregister(id, address)

    def on_connect(self):
        pass

    def on_disconnect(self):
        pass
