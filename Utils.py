import errno, time, os


def _mkdirp(path):
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


def _get_timestamp():
    '''Returns the current timestamp.'''

    return time.time()
