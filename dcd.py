#!/usr/bin/python

# System imports
import logging, json, sys, threading, time
from os import path
from socket import gethostname
from socket import error as socket_error
from rpyc.utils.server import ThreadedServer
from rpyc.core.protocol import DEFAULT_CONFIG
# Local imports
from Utils import _connect, _mkdirp
from DataServer import DataServer
from NameServer import NameServer, _check_heartbeats

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
    conn.root.register(host, port)


def _unregister(config, host, port):
    nameserver_host = config['nameserver.host']
    nameserver_port = config['nameserver.port']
    conn = _connect(nameserver_host, nameserver_port)
    conn.root.unregister(host, port)


def _start_server(service, port):
    logging.debug("Started service")
    ThreadedServer(service, port=port).start()
    logging.debug("Service over")


def start_name_service(config):
    port = int(config['nameserver.port'])
    NameServer._hostname = gethostname()
    NameServer._port = port

    t = threading.Thread(target=_start_server, args=(NameServer, port))
    t.daemon = True
    t.start()
    try:
        while t.isAlive():
            _check_heartbeats()
            t.join(1)
    except KeyboardInterrupt:
        pass


def start_data_service(config, port):
    data_dir = path.join(path.abspath(config['dataserver.data.dir']), 'storage')

    _mkdirp(data_dir)

    DataServer._hostname = gethostname()
    DataServer._port = port
    DataServer._data_dir = data_dir
    DataServer._config = config

    t = threading.Thread(target=_start_server, args=(DataServer, port))
    t.daemon = True
    t.start()

    conn = None
    while conn == None:
        try:
            conn = _connect(gethostname(), port)
            _register(config, DataServer._hostname, DataServer._port)
        except socket_error:
            time.sleep(3)
            pass

    try:
        while t.isAlive():
            conn.root.heartbeat()
            t.join(3)
    except KeyboardInterrupt:
        logging.info("Caught KeyboardInterrupt, unregistering")
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
