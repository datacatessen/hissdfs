import errno, logging, rpyc, time, os
from rpyc.utils.server import ThreadedServer


def start_rpyc_server(service, port):
    logging.debug("Started service")
    ThreadedServer(service, port=port).start()
    logging.debug("Service over")


def connect(hostname, port=None):
    '''Connects to a rpyc slave. It can either take the form of _connect('1.2.3.4:1234') or _connect('1.2.3.4', 1234)'''

    if port is None:
        hostname, port = hostname.split(':')

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
