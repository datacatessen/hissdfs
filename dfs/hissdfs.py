#!/usr/bin/python
# hissdfs.py

import json, logging, sys
from os import path
from Client import run_client
from NameServer import start_name_service
from DataServer import start_data_service

required_params = ['dataserver.root.dir', 'nameserver.address', 'nameserver.root.dir', 'log.level']


def _init_logging(config):
    root = logging.getLogger()
    root.setLevel(logging.getLevelName(config['log.level']))
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    root.addHandler(ch)


def _validate_config(config):
    for key in required_params:
        if not key in config:
            logging.error("Missing %s in config" % key)
            sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "usage: python hissdfs.py <config> <service> [options]"
        sys.exit(1)

    config_file = sys.argv[1]

    if not path.exists(config_file):
        logging.error("config file %s not found" % config_file)
        sys.exit(1)

    config = json.load(open(config_file, 'r'))
    _validate_config(config)
    _init_logging(config)

    service = sys.argv[2]

    if service == "nameserver":
        start_name_service(config)
    elif service == "dataserver":
        if len(sys.argv) == 4:
            start_data_service(config, int(sys.argv[3]))
        else:
            print "dataserver service requires an additional parameter, <port>"
    elif service == "dfs":
        run_client(config, sys.argv[3:])
    else:
        logging.error("Unknown service %s" % service)
