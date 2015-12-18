# NameServer.py
import copy, json, logging, pickle, random, rpyc, sys, threading, uuid
from collections import OrderedDict
from os import path
from socket import gethostname
from rpyc.utils.server import ThreadedServer
from Utils import connect, mkdirp, now, split_hostport

'''
The namespace maintains a multi-map of filename -> { block id : set(server_ids) },
where the map of block id to hostname list is an OrderedDict
'''
namespace = dict()

'''
This dictionary is a reverse index, mapping the block IDs to the filename for performance reasons
'''
block_to_file = dict()

'''
This is a set of dataservers currently connected to the NameServer
dictionary is a multi-map of id -> { address : <host:port>, heartbeat : <timestamp>, status : "ALIVE|DEAD" }
'''
dataserver_metadata = {}

''' BLOCK GENERATION '''


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


''' REPLICATION '''


def _check_replication():
    num_replicas = 3
    for blocklist in namespace.values():
        assert isinstance(blocklist, OrderedDict)
        for (blk_id, ids) in blocklist.items():
            assert isinstance(ids, set)
            if len(ids) != num_replicas:
                logging.warning("Num replications for block %s is not %d, but %d" % (blk_id, num_replicas, len(ids)))
                _add_replicas(blk_id, ids, num_replicas)


def _num_alive_servers():
    count = 0
    for id in dataserver_metadata.keys():
        if dataserver_metadata[id]["status"] == "ALIVE":
            count += 1
    return count


def _add_replicas(blk_id, ids, num_replicas):
    if len(ids) == 0:
        logging.error("Unable to replicate %s because there are currently 0 replicas" % blk_id)
        return

    replicas_to_add = min(num_replicas - len(ids), _num_alive_servers() - len(ids))

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


''' HEARTBEATS '''


def _check_heartbeats():
    for (id, metadata) in dataserver_metadata.items():
        assert isinstance(metadata, dict)
        if metadata["status"] != "DEAD" and metadata["heartbeat"] + 15 < now():
            logging.warning("Server %s has not check in for 15 seconds, unregistering" % id)
            _unregister(id, metadata["address"])


def _unregister(id, address):
    if id not in dataserver_metadata:
        logging.warning("Call to unregister server %s, addr %s, but no server exists" % (id, address))
    else:
        dataserver_metadata[id]["status"] = "DEAD"
        for blocklist in namespace.values():
            for (blk_id, hosts) in blocklist.items():
                assert isinstance(hosts, set)
                if id in hosts:
                    hosts.remove(id)

        logging.info("Server %s, addr %s is now DEAD and all blocks invalidated" % (id, address))
        logging.info(
            "List of dataservers is %s" % json.dumps(dataserver_metadata, indent=4, separators=(',', ':')))


''' SAVING NAMESPACE '''


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


def start_rpyc_nameserver(port):
    ThreadedServer(NameServer, port=port).start()


def start_name_service(config):
    global CONF
    CONF = config
    hostname, port = split_hostport(CONF['nameserver.address'])

    if hostname != "localhost" and hostname != gethostname():
        logging.error(
            "Attempt to start nameserver on the wrong node, expected %s but I am %s" % (hostname, gethostname()))

    _load_namespace()

    NameServer._hostname = hostname
    NameServer._port = port

    t = threading.Thread(target=start_rpyc_nameserver, args=[port])
    t.daemon = True
    t.start()

    try:
        while t.isAlive():
            _check_replication()
            t.join(5)
    except KeyboardInterrupt:
        _save_namespace()


class NameServer(rpyc.Service):
    def exposed_ping(self):
        logging.info("received ping")
        return 'pong'

    def exposed_touch(self, file_name):
        if not self.exposed_exists(file_name):
            logging.debug("touch %s" % file_name)
            namespace[file_name] = OrderedDict()
        else:
            raise Exception("File %s already exists" % file_name)

    def exposed_exists(self, file_name):
        logging.debug("exists %s" % file_name)
        return file_name in namespace

    def exposed_ls(self):
        logging.debug("ls")
        return namespace.keys()

    def exposed_rm(self, file_name):
        if self.exposed_exists(file_name):
            logging.debug("rm %s" % file_name)
            # Implement deleting blocks from DataServer
            del namespace[file_name]
            return True
        else:
            return False

    def exposed_new_ds_id(self, address):
        return str(uuid.uuid4())

    def exposed_register(self, id, address):
        logging.info("registered data server %s at %s" % (id, address))
        dataserver_metadata[id] = {"address": address, "status": "ALIVE", "heartbeat": now()}
        logging.info(
            "List of dataservers is %s" % json.dumps(dataserver_metadata, indent=4,
                                                     separators=(',', ':')))

    def exposed_unregister(self, id, address):
        _unregister(id, address)

    def exposed_new_block(self, file_name):
        if len(dataserver_metadata) == 0:
            raise Exception("Num dataservers is zero, cannot create file")

        '''Create a new block at a random DataServer and update the block -> file mapping'''
        blk_id = _random_block_id()
        block_info = {"id": blk_id, "address": _random_dataserver()[1]["address"], "file": file_name}
        block_to_file[blk_id] = file_name

        if not file_name in namespace:
            block_id_to_hosts = OrderedDict()
            namespace[file_name] = block_id_to_hosts
        else:
            block_id_to_hosts = namespace[file_name]

        block_id_to_hosts[blk_id] = set()

        logging.debug("returning block info %s" % block_info)
        return block_info

    def exposed_fetch_metadata(self, file_name):
        if file_name in namespace:
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

    def exposed_heartbeat(self, id):
        logging.debug("Received heartbeat from %s" % id)
        dataserver_metadata[id]["heartbeat"] = now()
        dataserver_metadata[id]["status"] = "ALIVE"
