'''
This module implements the CAL logging client protocol in pure python.

'''
import math
import numbers
import struct
from datetime import datetime
import time
import socket
import weakref
import heapq
import threading
import traceback
    
import os
import functools
import logging
import sys
import collections
try:
    import __builtin__
except:
    import builtins as __builtin__
try:
    import gevent
except:
    gevent = None

try:
    import contextvars
    tls = contextvars.ContextVar('correlation_id')
except:
    tls = None


_DEV_CAL_PRINT_LOGS = True

# from core/lang/fnv/hash_64.c
# fnv is multiply-then-xor


def fnv_hash(text):
    'NON-cryptographic hash function; do not use for MAC or any other crypto application'
    sofar = 0xcbf29ce484222325  # initial value from fnv.h
    if isinstance(text, bytes):
        try:
            text = str(text, 'utf8', 'replace')
        except:
            text = str(text)   # python2
    for c in text:
        sofar *= 0x100000001b3  # special prime
        sofar &= 2 ** 64 - 1
        sofar ^= ord(c)
    return sofar


def fnva_hash(text):
    sofar = 0x84222325cbf29ce4
    for c in text:
        sofar ^= ord(c)
        sofar *= 0x100000001b3  # special prime
        sofar &= 2 ** 64 - 1
    return sofar


def get_local(name):
    '''needs generalization for real asyncio usage'''
    if gevent:
        the_locals = getattr(gevent.getcurrent(), 'locals', None)
    else:
        t = threading.current_thread()
        the_locals = getattr(t, 'my_locals', None)
    if the_locals:
        return the_locals.get(name)
    return None


def set_local(name, val):
    if gevent:
        cur = gevent.getcurrent()
        if not hasattr(cur, 'locals'):
            cur.locals = {}
        the_locals = cur.locals
    else:
        t = threading.current_thread()
        if not hasattr(t, 'my_locals'):
            t.my_locals = {}
        the_locals = t.my_locals
    the_locals[name] = val


def unset_local(name):
    if gevent:
        the_locals = getattr(gevent.getcurrent(), 'locals', None)
    else:
        t = threading.current_thread()
        the_locals = getattr(t, 'my_locals', None)
    if the_locals:
        del the_locals[name]


def get_cur_correlation_id():
    cur = get_local('correlation_id')
    if cur:
        return cur
    # this is reproducing CalUtility.cpp
    # TODO: where do different length correlation ids come from in CAL logs?
    t = time.time()
    corr_val = "{0}{1}{2}{3}".format(socket.gethostname(),
                                           os.getpid(), int(t), int(t % 1 * 10 ** 6))
    corr_id = "{0:x}{1:x}".format(
        fnv_hash(corr_val) & 0xFFFFFFFF,
        int(t % 1 * 10 ** 6) & 0xFFFFFFFF)
    set_local(b'correlation_id', corr_id)
    return corr_id


def sql_hash(query_text):
    'hash SQL queries for CAL'
    h = fnva_hash(query_text)
    hi = h >> 32
    lo = h & (2 ** 32 - 1)
    h = hi ^ lo
    return h

_THE_CLIENT = None


def init_client(pool, environment='PayPal', host='127.0.0.1', port=1118,
                min_status='0'):
    global _THE_CLIENT
    global event
    _THE_CLIENT = Client(pool, environment, host, port, min_status)
    event = _THE_CLIENT.event


# init() function for backwards-compatibility / API stability
_NOARG = object()  # sentinel object for not-passed parameter


def init(ip='127.0.0.1', port=1118, env='PayPal', pool=None,
         connect=_NOARG, raise_errors=_NOARG, log_dir=_NOARG, log_file=_NOARG):
    if pool is None:
        raise ValueError("pool must be a valid string")
    init_client(pool, env, ip, port)


def set_build_label(build_label):
    global create_machine_header
    old_machine_header = create_machine_header
    create_machine_header = functools.partial(old_machine_header, build_label=build_label)

DEBUG    = '0'
WARNING  = '3'
ERROR    = '2'
CRITICAL = '1'
UNKNOWN  = 'U'  # NOTE: only here for consistency; Python client doesn't use 'U'

INTERNAL_ERROR = '1'
CLIENT_ERROR = '2'

_INT = struct.Struct('>I')
_SINT = struct.Struct('>i')
_INTINT = struct.Struct('>II')

app_start_time = time.strftime("%H:%M:%S.", time.gmtime()) + str(int((time.time() % 1) * 100))


def timestamp():
    now = datetime.now()
    centiseconds = "{0:02.0f}".format(math.trunc(now.microsecond / 10000.0))
    return now.strftime("%H:%M:%S.") + centiseconds


def add_corr_id(data):
    if not data:
        data = ""
    if "corr_id_" not in data:
        if data:
            data += "&"
        corr_id = get_cur_correlation_id()
        if corr_id:
            data += "corr_id_=" + get_cur_correlation_id()
    return data


def pack_message(msg, thread_id, escape_whitespace=True):

    try:
        if type(msg) is unicode:
            ascii_msg = msg.encode('ascii', 'backslashreplace')
        else:
            ascii_msg = msg  # ?
    except:
        if type(msg) is bytes:
            ascii_msg = msg.decode('utf-8', 'replace')
        else:
            ascii_msg = msg  # ?
        
    if ascii_msg.endswith('\r\n'):
        ascii_msg = ascii_msg[:-2]
    if escape_whitespace:
        ascii_msg = ascii_msg.replace('\n', '\\n').replace('\r', '\\r')
    ascii_msg = ascii_msg[:4094] + '\r\n'
    try:
        rv = block_header(ascii_msg, thread_id) + bytes(ascii_msg, 'utf8')
    except:
        rv = block_header(ascii_msg, thread_id) + ascii_msg
    return rv
    # there might be a better place to escape to ascii, but this is what we got.


_DEFAULT_LABEL = ''


def create_machine_header(pool_name, env_name, ip, build_label=_NOARG, start_time=None):
    global _DEFAULT_LABEL
    if build_label == _NOARG:
        build_label = _DEFAULT_LABEL = 'python;***;default'
    start_time_format = "%d-%m-%Y %H:%M:%S"
    if start_time is None:
        start_time_formatted = time.strftime(start_time_format)
    else:
        start_time_formatted = start_time  # PUNT

    machine_header = '\r\n'.join([
        "SQLLog for %s:%s:%s" % (pool_name, socket.gethostname(),ip),
        "Environment: %s" % env_name,
        "Label: %s" % build_label,
        "Start: %s" % start_time_formatted])
    return machine_header


def block_header(msg, thread_id):
    if type(thread_id) != type(b''):
        thread_id = _SINT.pack(thread_id & 0x7FFFFFFF)  # mask down to 32 bits
    thread_header = thread_id + _INTINT.pack(  # pack 3 unsigned ints in network (big-endian) order
        int(time.time()), len(msg))
    # http://xotools.ca1.paypal.com/gitsource/xref/Infrastructure__infra__master/github/all/infra/utility/environment/log/CalSocketHandler.cpp#60
    # timestamp is seconds since Jan 1, 1970
    return thread_header


def _msg(*a):
    try:
        rv = bytes("\t".join((timestamp(),) + a) + "\r\n", 'utf8', 'replace')
    except:
        rv = "\t".join((timestamp(),) + a) + "\r\n"
    return  rv

def heartbeat_msg(cal_type, name, status, data):
    return b"H" + _msg(cal_type, name, status, data)


def event_msg(cal_type, name, status, data):
    return b"E" + _msg(cal_type, name, status, data)


def start_trans(cal_type, name):
    return b"t" + _msg(cal_type, name)


def data_trans(data):
    return b"F" + data + b"\r\n"


def end_trans(cal_type, name, status, duration, data):
    return b"T" + _msg(cal_type, name, status, duration, data)


def atomic_trans(cal_type, name, status, duration, data):
    return b"A" + _msg(cal_type, name, status, duration, data)


def sql_msg(query, in_sql_hash):
    return "$" + str(in_sql_hash) + "\t" + query + "\r\n"


NAME_CHAR_MAP = {'heartbeat_msg': 'H',
                 'event_msg': 'E',
                 'start_trans': 't',
                 'data_trans': 'F',
                 'end_trans': 'T',
                 'atomic_trans': 'A',
                 'sql_msg': '$'}


# For reference, C++ protocol implementation is located here:
#  http://xotools.ca1.paypal.com/gitsource/xref/Infrastructure__infra__master/github/all/infra/utility/environment/log/CalMessages.cpp
# basically, messages are of the form [char][timestamp]\t[type]\t[name]\t[status]\t[data]\r\n

# TODO: escape \t, \r, \n (do we really need this?)
# TODO: limit field length


class Aliaser(object):
    '''
    Assigns arbitrary weakref-able objects the smallest possible unique
    integer IDs, such that no two objects have the same ID at the same
    time.
    '''
    def __init__(self):
        self.mapping = weakref.WeakKeyDictionary()
        self.ref_map = {}
        self.free = []

    def get(self, a):
        if a in self.mapping:  # if mapping exists, return it
            return self.mapping[a][0]
        if self.free:  # if there are any free numbers, use the smallest
            nxt = heapq.heappop(self.free)
        else:  # if there are no free numbers, use the next highest number
            nxt = len(self.mapping)
        ref = weakref.ref(a, self._clean)
        self.mapping[a] = (nxt, ref)
        self.ref_map[ref] = nxt
        return nxt

    def drop(self, a):
        freed, ref = self.mapping[a]
        del self.mapping[a]
        del self.ref_map[ref]
        heapq.heappush(self.free, freed)

    def _clean(self, ref):
        try:
            heapq.heappush(self.free, self.ref_map[ref])
            del self.ref_map[ref]
        except:
            pass  # shutdown errors

    def __contains__(self, a):
        return a in self.mapping

    def __iter__(self):
        return self.mapping.itervalues()

    def __len__(self):
        return self.mapping.__len__()

    def iteritems(self):
        return self.mapping.iteritems()


if hasattr(logging, 'NullHandler'):
    NullHandler = logging.NullHandler
else:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

PRINT_EXCEPTIONS = False  # disable cal sending exception printing

_NUM_PER_HEARTBEAT = 5


def make_heartbeat():
    return pack_message(heartbeat_msg('STATE', 'LOG', '0', '(no-data)'), 7)


try:
    from os import getpgrp
except Exception:
    def getpgrp():
        return os.getpid()


class Client(object):
    def __init__(self, pool, environment='PayPal', host='127.0.0.1', port=1118,
                 min_status='0'):
        '''
        Parameters
        ----------
        pool : string
           The name of the currently running application.
        environment : str, optional
           Always 'PayPal'.
        host : str, optional
           The ip of the host to connect to.  Default '127.0.0.1'.
        port : int, optional
           The port to connect to.  Always 1118.
        min_status : str or int, optional
           The minimum status to log, for cal messages which have status.
        '''
        self.host = host
        self.port = port
        self.pool = pool
        self.environment = environment
        self.aliaser = Aliaser()
        self.source_cache = _SourceCache()
        self.inited_thread_ids = set()
        self.sock = None
        self.stopping = False
        self.pid = os.getpid()  # keep track of pid to detect fork()-ing
        self.pgid = getpgrp()
        self.called_by = traceback.format_stack()
        self.actor = StdThreadActor(self._send, make_heartbeat,
                                    called_by=self.called_by)
        self.min_status = str(min_status)
        self.overflow_threadids = collections.defaultdict(OverflowThreadid)
        self.actor.start()
        return

    def get_cur_thread_id(self):
        if gevent:
            r = (os.getpid() << 16) + self.aliaser.get(gevent.getcurrent())
        else:
            r = (os.getpid() << 16) + self.aliaser.get(threading.current_thread())
        return r

    def send(self, msg_type, cal_type=None, name=None, status=None,
             data=None, duration=None, thread_id=None, sql_hash=None,
             log_src=True):
        if msg_type.__class__ is (lambda: None).__class__:  # if is function
            msg_type = msg_type.__name__
        msg_type = globals()[msg_type]

        # normalize status of form '0', '1', '2' or 0, 1, 2
        
        if isinstance(status, numbers.Number):
            status = str(status)

        if (msg_type is event_msg and status and self.min_status != '0' and
                status > self.min_status):
            return

        if thread_id is None:
            # NOTE:assume there will not be more than 2**16 greenlets running at once
            thread_id = self.get_cur_thread_id()
        elif thread_id == 0:
            thread_id = 7  # 0 puts cal-daemon into "cgi-mode", which is bad

        if isinstance(data, dict):
            data = _url_encode_dict(data)

        if log_src and data is not None and msg_type is not sql_msg:
            if data:
                data += "&"
            if sys.version_info.major == 2:
                data += "src=" + self.source_cache.get_frame_src(sys._getframe()) 
            else:
                data += "src=" + self.source_cache.get_frame_src(sys._getframe()) 

        if msg_type in (event_msg, heartbeat_msg, end_trans, atomic_trans):
            # don't forget -- strings are immutable so modifications to data string
            # must be done before kw dict is assembled
            if msg_type is end_trans and self.environment == 'PayPal':
                data = add_corr_id(data)
            kw = {"cal_type":   cal_type,   "name":  name,
                  "status": status, "data":  data}
            if msg_type in (end_trans, atomic_trans):
                kw['duration'] = duration
            msg = msg_type(**kw)
        elif msg_type is start_trans:
            msg = start_trans(cal_type, name)
        elif msg_type is data_trans:
            msg = msg_type(data)
        elif msg_type is sql_msg:
            msg = msg_type(data, sql_hash)
        else:
            raise ValueError("unrecognized cal message type " + str(msg_type))

        if thread_id & 0xFFFF > 64:
            packed = pack_message(msg, thread_id | 0xFFFF)
            out = self.overflow_threadids[thread_id].message(packed)
        else:
            packed = pack_message(msg, thread_id)
            out = [packed]

        if os.getpid() != self.pid:
            self.actor = StdThreadActor(self._send, make_heartbeat,
                                        called_by=self.called_by)
            self.pid = os.getpid()
            self.pgid = getpgrp()  # shouldn't change, but just in case

        for packed in out:
            self.actor.send(packed)

    def event(self, cal_type, name, status, data, log_src=True):
        self.send(msg_type=event_msg, cal_type=cal_type, name=name, status=status,
                  data=data, log_src=log_src)

    def heartbeat(self, cal_type, name, status, data):
        self.send(msg_type=heartbeat_msg, cal_type=cal_type, name=name, status=status,
                  data=data)

    def atomic_trans(self, cal_type, name, status, data, duration, thread_id=None):
        self.send(msg_type=atomic_trans, cal_type=cal_type, name=name, status=status,
                  data=data, duration=duration, thread_id=thread_id)

    def end_trans(self, cal_type, name, status, data, duration):
        self.send(msg_type=end_trans, cal_type=cal_type, name=name, status=status,
                  data=data, duration=duration)

    def start_trans(self, cal_type, name):
        self.send(msg_type=start_trans, cal_type=cal_type, name=name)

    def sql_msg(self, data, in_sql_hash=None):
        if in_sql_hash is None:
            in_sql_hash = sql_hash(data)
        self.send(msg_type=sql_msg, data=data, sql_hash=in_sql_hash)

    def data_trans(self, data):
        self.send(msg_type=data_trans, data=data)

    def trans(self, *a, **kw):
        kw.update({'client': self})
        return trans(*a, **kw)

    def metric(self, name, val):
        'leverages existing stats gathered on transaction duration'
        self.atomic_trans('METRIC', name, 0, '', str(val))

    def biz_event(self, name, data):
        'leverages special data path for events of type BIZ'
        self.event('BIZ', name, 0, data)

    transaction = trans

    def atrans(self, *a, **kw):
        kw.update({'client': self, 'atomic': True})
        return trans(*a, **kw)

    def release_threadid(self):
        '''
        release a greenlet\'s reserved thread-id, for greenlets
        that are going to be idle for a long time with no outstanding
        transactions (e.g. cache refresh loops, keep-alives)
        '''
        if hasattr(self, 'aliaser') and self.aliaser:
            if gevent:
                if gevent.getcurrent() in self.aliaser:
                    self.aliaser.drop(gevent.getcurrent())
            else:
                if threading.current_thread().ident in self.aliaser:
                    self.aliaser.drop(threading.current_thread().ident)

    def reconnect(self):
        self.inited_thread_ids = set()
        if self.sock:
            try:
                self.sock.close()
            except socket.error:
                pass  # just trying to be nice to server by closing socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))

    def close(self):
        if self.stopping:
            return
        self.stopping = True
        self.actor.stop()
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except socket.error:
                pass  # just trying to be nice to server by closing socket
            try:
                self.sock.close()
            except socket.error:
                pass  # just trying to be nice to server by closing socket
            self.sock = None

    def _send(self, data):
        i = 0
        while 1:
            try:
                if not self.sock:
                    self.reconnect()
                if data[:4] not in self.inited_thread_ids:
                    ip, port = self.sock.getsockname()
                    machine_header = pack_message(create_machine_header(
                        self.pool, self.environment, ip), data[:4], False)
                    self.sock.sendall(machine_header)
                    self.inited_thread_ids.add(data[:4])
                self.sock.sendall(data)
                break
            except Exception:
                if self.stopping:
                    return
                # if not in deferred mode, abort quickly on failure
                # ... abort when i = 3: 10 + 20 + 40 = 70 ms max wait
                # 10.24 seconds max delay in attempting to reconnect to publisher
                time.sleep(WAIT_FACTOR * (2 ** min(i, 10)))
                i += 1
                try:
                    self.reconnect()
                except socket.error:  # in case opening connection times out
                    pass


class OverflowThreadid(object):
    '''
    Stores up messages until a root transaction
    is closed, for handling too many thread-ids
    '''
    def __init__(self):
        self.stored = []
        self.depth = 0

    def message(self, msg):
        'called every time a new message arrives; returns to-be-sent messages'
        self.stored.append(msg)
        if msg[12] == 't':
            self.depth += 1
        elif msg[12] == 'T':
            self.depth -= 1
        sendable = ()
        if self.depth == 0:
            sendable, self.stored = self.stored, []
        if len(self.stored) > 512:  # back-stop against memory leak
            self.stored = []
        return sendable


class _SourceCache(object):
    '''
    Helper class for improving the performance of getting the src
    attribute for a given frame by caching.
    '''
    def __init__(self):
        self.code_sources = {}

    def get_frame_src(self, frame):
        src = None
        f_nxt = frame.f_back
        steps = 0
        while 1:
            steps += 1
            if steps > 15:
                break
            src = self._fetch(frame.f_code)
            if src is not None:
                break
            if f_nxt is None:  # try to "jump" greenlets if no more frames
                f_nxt = getattr(gevent.getcurrent(), 'calling_frame', None)
                if f_nxt is None:
                    break  # nothing we can do, end of the line
            frame = f_nxt
            f_nxt = frame.f_back
        if src is None:
            src = '<frame>{0}'
        return src.format(frame.f_lineno)

    def _fetch(self, code):
        if code not in self.code_sources:
            fn = code.co_filename  # also in loop below
            if (((len(fn.split(os.sep)) > 2) and
                 (fn.split(os.sep)[-2] in ["hera", "contrib", "gevent", "juno"])) or
                    (sys.prefix.split(os.sep)[:4] == fn.split(os.sep)[:4])):
                self.code_sources[code] = None
            elif fn == '<string>':
                return '<string>{0}'  # don't hold reference to dynamic code
            else:
                file_name = "/".join(fn.split(os.sep)[-1:])
                line_info = "{0}:{{0}}".format(file_name)
                self.code_sources[code] = line_info
        return self.code_sources[code]


WAIT_FACTOR = 0.010  # 10ms


# TODO: set a threadlocal to detect if already in the middle of sending a message,
# to avoid infinite loops?
class StdThreadActor(object):
    def __init__(self, process, 
                 default=None, default_time=30.0, called_by=''):  # default time should be tunable
        # default is heartbeat
        self.process = process
        self.default = default
        self.running = False
        self.stopping = False
        self.called_by = called_by
        self.wait = 1
        self._last_default = time.time()
        self.default_time = default_time

    def start(self):
        pass

    def stop(self):
        pass

    def send(self, item):
        if item and _DEV_CAL_PRINT_LOGS:
            if b'STAGE\tLOG' not in item:
                if sys.version_info.major == 2:
                    print ("id: {} CAL-PRINT: {}".format(struct.unpack('>I', item[:4]),
                                                         str(item[12:]).strip("\n")))
                else:
                    print ("id: {} CAL-PRINT: {}".format(struct.unpack('>I', item[:4]),
                                                         str(item[12:], 'utf-8').strip("\n")))
#               print ctx._origin   # uncomment to discover rogue CAL-PRINTs


    @classmethod
    def std_thread_actor_test(cls):
        processed = []

        def print_and_flush(d):
            processed.append(d)
        a = StdThreadActor(print_and_flush)
        if gevent:
            gevent.spawn(lambda: [a.send(i) for i in range(50)])
            gevent.sleep()
        # race condition maybe, but who cares, it is just a test
        for i, j in enumerate(processed):
            assert i == j, "out of order processing: processed[" + str(i) + "] == " + str(j)
        return a, processed


class NullCalClient(Client):
    """Just throw away everything"""
    def __init__(self):
        self.aliaser = Aliaser()

    def stopping(self):
        pass

    def send(self, *a, **kw):
        pass


class trans(object):
    '''
    A context-manager which wraps the enclosed block in a CAL transaction.
    Also, a decorator which wraps the decorated function in a CAL transaction.
    '''

    EXC_CAL_STATUS = '1'

    def __init__(self, type, name, status='0', msg=None,
                 atomic=False, extra=None, **more_extra):
        self.msg = msg or {}
        self.name = name
        self.type = type.upper()
        self.atomic = atomic
        # due to being used as a decorator, we want to keep the original
        # "client" parameter around so it can be passed through when the
        # decorated function is called; this has come up
        self.status = str(status)

        self.start_time = None
        self.end_time = None
        self.extra = extra or {}
        self.extra.update(more_extra)

    def __enter__(self):
        # if parent transaction exists, add self to children
        self.exc_info = (None, None, None)
        # NOTE: tested creation of parent/child pointer objects like this
        # including GC, a tree of depth 5 with 10 children per node (100k total nodes)
        # constructed and cleaned up in 0.24 seconds; without parent pointer was 0.22 seconds
        # so, reference loop here is not worth worrying about
        self.start_time = time.time()
        if not self.atomic:
            _THE_CLIENT.start_trans(self.type, self.name)
        return self

    def __exit__(self, *exc_info):
        exc_info = exc_info if exc_info[0] else self.exc_info
        if type(exc_info[1]).__name__ == 'GreenletExit':
            green_exit = True
            self.exc_info = (None, None, None)
            exc_info = self.exc_info
        else:
            green_exit = False
        # end transaction
        self.end_time = time.time()
        duration = (self.end_time - self.start_time) * 1000.0
        duration = str(duration)  # odd language
        # keep things unicode for now; cal will down-case to ascii at lower layers if needed
        if isinstance(self.msg, dict):
            data = self.extra
            data.update(self.msg)
            if exc_info[0] and "m_err" not in data:
                if hasattr(exc_info[1], 'm_err'):
                    data['m_err'] = str(exc_info[1].m_err)  # allow custom Exception class to pass in m_err
                else:
                    data["m_err"] = type(exc_info[1]).__name__  # limit to small number of distinct things
            if green_exit:
                data["green_exit"] = True
            data = _url_encode_dict(data)
        elif isinstance(self.msg, bytes):  # ensure message is unicode
            data = str(self.msg, 'utf8', 'replace')
        else:
            data = self.msg

        data = add_corr_id(data)
        self.status = str(self.status)
        if exc_info[0]:
            if hasattr(exc_info[1], 'cal_status'):
                self.status = exc_info[1].cal_status
            elif self.status[0] in ('0', '3'):
                self.status = self.EXC_CAL_STATUS
                # ensure cal status
                # is at least ERROR
            traces = getattr(exc_info[1], '__greenlet_traces', None) or []
            trace_str = traceback.format_exception(*exc_info)
            exc_str = trace_str[-1]
            trace_str = "".join(trace_str[:-1])
            traces.append(trace_str)
            traces = "\n".join(reversed(traces)) + "\n" + exc_str
            if data:
                data += "&"
            data += "\n\tmsg=" + traces.replace("\n", "\n\t")  # can't use m_err here

        params = (self.type, self.name, self.status, data, duration)
        if self.atomic:
            _THE_CLIENT.atomic_trans(*params)
        else:
            _THE_CLIENT.end_trans(*params)


    def start(self):
        self.__enter__()

    def end(self, msg=None, exc_info=(None, None, None)):
        if msg is not None:
            self.msg = msg
        self.__exit__(*exc_info)

    def set_exc_info(self, exc_info=None):
        '''
        Set the exception info that will be used to populate the CAL data
        when the transaction ends to either the passed 3-tuple, or whatever
        exception is currently being handled.
        '''
        if exc_info:
            self.exc_info = exc_info
        else:
            self.exc_info = sys.exc_info()

    def __call__(self, f):

        @functools.wraps(f)
        def g(*a, **kw):
            # copy.copy deemed evil
            with self.__class__(self.type.upper(), self.name,
                                status=self.status, msg=self.msg,
                                atomic=self.atomic, extra=self.extra):
                return f(*a, **kw)
        g.wrapped_func = f
        return g


def _url_encode_dict(data):
    datalist = []  # newer Python compilers can optimize string concat
    for k, v in data.items():  # but we need to support some older ones
        datalist.append(str(k) + "=" + str(v))
    return "&".join(datalist)

def uninit(*kw, **args):
    print ("Client not initialized, please call cal.init()")

event = uninit
