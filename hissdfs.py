#!/usr/bin/python

# System imports
import json, logging, sys
from os import path
# Local imports
from DataServer import start_data_service
from NameServer import start_name_service
from Utils import connect

required_params = ['nameserver.address', 'dataserver.root.dir', 'log.level']


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
    if len(sys.argv) <= 1:
        print "usage: python dcd.py <config> <service> [options]"
        sys.exit(1)

    config_file = sys.argv[1]
    service = sys.argv[2]

    if not path.exists(config_file):
        logging.error("config file %s not found" % config_file)
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
        logging.error("Unknown service %s" % service)
