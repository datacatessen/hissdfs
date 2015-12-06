import errno, logging, rpyc, time, os
from rpyc.utils.server import ThreadedServer


def start_rpyc_server(service, port):
    logging.debug("Started service")
    ThreadedServer(service, port=port).start()
    logging.debug("Service over")


def connect(address):
    hostname, port = split_hostport(address)

    try:
        a = rpyc.connect(str(hostname), int(port))
        return a
    except Exception as e:
        logging.warning(' '.join(['There was a problem connecting to', hostname, str(port)]))
        raise e


def mkdirp(path):
    path = path.strip()
    if len(path) == 0:
        raise ValueError("I can't make a directory with no name")

    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise exc


def get_timestamp():
    '''Returns the current timestamp.'''

    return time.time()


def cat_host(hostname, port):
    '''Returns a single string containing the host and the port '1.2.3.4:444' from a hostname '1.2.3.4' and a port 444'''

    return str(hostname) + ':' + str(port)


def split_hostport(hostnameport):
    '''A convenience function that takes a string "x.x.x.x:yyyy' and splits it into "x.x.x.x", yyyy'''

    h, p = hostnameport.split(':')
    return h, int(p)
