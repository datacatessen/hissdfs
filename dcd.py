#!/usr/bin/python

# System imports
import logging, json, sys
from os import path
from socket import gethostname
from rpyc.utils.server import ThreadedServer
# Local imports
from Utils import _connect
from Utils import _mkdirp
from DataServer import DataServer
from NameServer import NameServer

required_params = ['nameserver.host', 'nameserver.port', 'dataserver.data.dir']


def _init_logging(config):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    root.addHandler(ch)


def _validate_config(config):
    for key in required_params:
        if not key in config:
            print "missing %s in config" % key
            sys.exit(1)


def _register(config, host, port):
    nameserver_host = config['nameserver.host']
    nameserver_port = config['nameserver.port']
    conn = _connect(nameserver_host, nameserver_port)
    if conn.root.register(host, port):
        return True
    else:
        raise Exception(
            "Failed to register service %s:%d to nameserver at %s:%d" % (host, port, nameserver_host, nameserver_port))


def _unregister(config, host, port):
    nameserver_host = config['nameserver.host']
    nameserver_port = config['nameserver.port']
    conn = _connect(nameserver_host, nameserver_port)
    if conn.root.unregister(host, port):
        return True
    else:
        raise Exception(
            "Failed to unregister service %s:%d to nameserver at %s:%d" % (
                host, port, nameserver_host, nameserver_port))


def start_name_service(config):
    port = int(config['nameserver.port'])
    NameServer._hostname = gethostname()
    NameServer._port = port

    s = ThreadedServer(NameServer, port=port)
    s.start()


def start_data_service(config, port):
    data_dir = path.abspath(config['dataserver.data.dir'])

    _mkdirp(data_dir)

    DataServer._hostname = gethostname()
    DataServer._port = port
    DataServer._data_dir = data_dir

    _mkdirp(path.join(data_dir, 'storage'))

    retval = _register(config, DataServer._hostname, DataServer._port)
    print retval
    if not retval:
        print "Failed to register service, check name server logs"
        sys.exit(1)
    else:
        s = ThreadedServer(DataServer, port=port)
        s.start()
        _unregister(config, DataServer._hostname, DataServer._port)


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print "usage: python dcd.py <config> <service> [options]"
        sys.exit(1)

    config_file = sys.argv[1]
    service = sys.argv[2]

    if not path.exists(config_file):
        print "config file %s not found" % config_file
        sys.exit(1)

    config = json.load(open(config_file, 'r'))
    _validate_config(config)
    _init_logging(config)

    if service == "nameserver":
        start_name_service(config)
    elif service == "dataserver":
        if len(sys.argv) != 4:
            print "dataserver service options: port"
            sys.exit(1)
        else:
            start_data_service(config, int(sys.argv[3]))
    else:
        print "error: unknown service %s" % service
