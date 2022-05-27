"""Common code between cryptoclient and OCC clients.

   This module handles the connection management, including keep-alives for
   OCC, which means it's buffering the read and write calls through a queue to a separate
   thread

   The connection tho is made in a callback provided to the OCC client or the default
   connection handling in occ.py

   The code is intended to be run in Python2 and Python3, so there's some odd try/excepts
   for that purpose.  
"""

from __future__ import print_function
from __future__ import absolute_import, unicode_literals


import time
import collections
import hmac
import hashlib
import weakref

try:
    import Queue
except:
    import queue as Queue

import socket
import ssl

from . import corr_id
from . import ll
from . import cal

import ns_help

ml = ll.LLogger(trace_mod=False)  # true to enable debugging this module

_PUMPS = []


class NetString(object):
    def __init__(self, sock, protocol_name=None, username_override=None, do_handshake=False):
        if protocol_name:
            self.protocol_name = protocol_name
        else:
            self.protocol_name = OCC_PROTOCOL_NAME
        self.socket = sock
        self.socket.settimeout(None)
        self.buf = b''
        self._nest_level = 0
        self._nestings = []
        self._send_buf = []
        self.ns_q = Queue.Queue()
        self.timeout = 30.0  # eh
        self.last_data_send_time = 0
        self.live = True
        self.username_override = username_override
        self._spawn_pump_if_need()
        if do_handshake:
            self.handshake()

    def start_netstring(self):
        if (self._nest_level):
            raise Exception("Netstring API ERROR: can't nest nested netstrings")
        self._nest_level = 1

    def stop_netstring(self):
        if (self._nest_level != 1):
            raise Exception("Netstring API ERROR: can't nest nested netstrings")
        self._nest_level = 0
        if self._nestings:
            value = "".join(self._nestings)
            self._nestings = []
        else:
            value = None
        code = 0
        ns = self._make_ns(code, value)
        self._send_buf.append(ns)

    def getpeername(self):
        return self.socket.sock.getpeername()

    def wants_extra(self):
        return True

    def handshake(self):
        pass # nothing to do for opensource client

    def _make_ns(self, code, value):
        if value is None:
            payload = str(code)
        else:
            payload = str(code) + " " + str(value)
        return str(len(payload)) + ":" + payload + ","

    def write(self, code, value=None):
        ml.ld3("Netstring sending code {0}, {{{1}}}", code, value)
        ns = self._make_ns(code, value)
        if self._nest_level:
            self._nestings.append(ns)
        else:
            self._send_buf.append(ns)

    def flush(self):
        if self._nest_level:
            ml.ld4("Skipping flush for nested")
            return
        ml.ld4("Netstring now flushing socket {0!r}.", self.socket)
        if self._send_buf:
            self.last_data_send_time = time.time()
            try:
                d = bytes("".join(self._send_buf), 'utf-8')
            except:
                d = "".join(self._send_buf)
            try:
                ml.ld2("SSL: OUT: {}", str(d,'utf-8'))
            except:
                ml.ld2("SSL: OUT: {}", str(d))
                
            self.socket.sendall(d)
        self._send_buf = []

    def _spawn_pump_if_need(self):
        import threading
        global _PUMPS
        if hasattr(self, "_netstring_sock_pump"):
            _PUMPS = [l for l in _PUMPS if l is not self._netstring_sock_pump]
            if self._netstring_sock_pump.is_alive():
                ml.ld3("Not restarting good thread")
                return
        self._netstring_sock_pump = threading.Thread(target=_pump_netstring_sock, args=(weakref.proxy(self),))
        self._netstring_sock_pump_exception = None
        self._netstring_sock_pump.setDaemon(True)
        ml.ld2("Starting IO thread")
        self._netstring_sock_pump.start()
        _PUMPS.append(self._netstring_sock_pump)

    def read(self):
        q_depth = self.ns_q.qsize()
        ml.ld4("read called with q depth {}", q_depth)
        if q_depth == 0:
            e = self._netstring_sock_pump_exception
            self._spawn_pump_if_need()
            if e:
                raise e
        try:
            ml.ld4("About to wait on Q {}", self.ns_q)
            rv = self.ns_q.get(timeout=self.timeout)
            ml.ld3("Got from Q: {0}/{1}", rv, type(rv))
            if isinstance(rv, Exception):
                raise rv
            return rv
        except Queue.Empty:
            ml.ld3("Queue empty")
            e = self._netstring_sock_pump_exception
            self._spawn_pump_if_need()
            if e:
                raise e
            raise socket.error("timed out after " + str(self.timeout) + " seconds")

    def xid(self, format_id, global_transaction_id, branch_qualifier):
        return  # TODO -- generate proper transaction id

    def killsocket(self):
        sock = self.sock
        if hasattr(sock, '_sock'):
            ml.ld2("Killing socket {0}/FD {1}", id(sock), sock._sock.fileno())
        else:
            ml.ld2("Killing socket {0}", id(sock))
        try:
            # TODO: better ideas for how to get SHUT_RDWR constant?
            sock.shutdown(socket.SHUT_RDWR)
        except (socket.error):
            pass  # just being nice to the server, don't care if it fails
        except Exception as e:
            cal.event("INFO", "SOCKET", "0",
                      "unexpected error closing socket: " + repr(e))
        try:
            sock.close()
        except (socket.error):
            pass  # just being nice to the server, don't care if it fails
        except Exception as e:
            cal.event("INFO", "SOCKET", "0",
                      "unexpected error closing socket: " + repr(e))

    def disconnect(self, xid=None):
        global _PUMPS
        self.live = False
        self.killsocket()
        self.socket.sock.shutdown()
        _PUMPS = [l for l in _PUMPS if l is not self._netstring_sock_pump]
        self._netstring_sock_pump.kill()

    def fileno(self):
        return self.socket.sock.fileno()

    def settimeout(self, timeout):
        self.timeout = timeout

    def close(self):
        self.disconnect()

    def shutdown(self, how):
        self.disconnect()

    def recv_until(self, delim, recur=False):
        if delim in self.buf:
            msg, self.buf = self.buf.split(delim, 1)
            return msg
        if not recur:
            ml.ld4("About to socket recv")
            self.socket.settimeout(self.timeout)
            s_recv = self.socket.recv(2048)
            ml.ld4("Socket recv ended with {0}", s_recv)
            self.buf = self.buf + s_recv
            return self.recv_until(delim, True)
        else:
            raise socket.error("Recv until " + str(delim) + " failed")
        
    def recv(self, n):
        ml.ld4("Read called with buffer {0}", self.buf)
        if self.buf and len(self.buf) > n:
            msg = self.buf[:n]
            self.buf = self.buf[n:]
            return msg
        to_recv = n
        if self.buf:
            to_recv = n - len(self.buf)
        msg = self.buf
        if to_recv > 0:
            msg = msg + self.socket.recv(to_recv) 
        self.buf = b''
        ml.ld2("Read got {0} bytes", len(msg))
        if len(msg) == 0:
            raise socket.error("EOF on {}".format(str(self.socket)))

        return self.buf + msg
        

def _pump_netstring_sock(self):
    'internal helper that uses a weakref to pump the socket'
    import socket
    ml.ld("Pump netstring sock started {}", self.socket)
    try:
        while 1:
            # format is <size as string>:<numeric type as string><space><data till size - 2>,
            size_b = self.recv_until(b':')
            size = int(size_b)
            ml.ld2("READ IN len: {}", size)
            message = self.recv(size)
            ml.ld3("READ IN: {}", message)
            assert self.recv(1) == b',', "netstring missing trailing ','"
            netstring = message.split(b" ", 1)  # return code -space- message, or just return code
            netstring[0] = int(netstring[0])
            if len(netstring) == 1:
                netstring.append(b"")
            stack = collections.deque([netstring])
            while stack:  # handle arbitrary depth netstrings, although in practice they only go 1 deep
                sub_ns = stack.popleft()
                if sub_ns[0] == 0:  # NOTE:  ((A, B), C) => A,B,C NOT C, A, B
                    stack.extendleft(reversed(ns_help.get_netstrings(sub_ns[1])))
                elif sub_ns[0] != SERVER_ALIVE:  # throw away keep-alives
                    ml.ld4("Adding to ns_q" + str(sub_ns))
                    self.ns_q.put(sub_ns)
                elif (self.protocol_name != OCC_PROTOCOL_NAME):
                    ml.ld("Adding to ns_q" + sub_ns)
                    self.ns_q.put(sub_ns)
    except ReferenceError:
        pass  # working as designed
    except (socket.error, ssl.SSLError) as e:
        try:
            if self.live:
                ml.ld4("Adding {} to ns_q {}/{}", e, e.errno, self.socket)
                self.ns_q.put(e)
                self._netstring_sock_pump_exception = e
                return
        except ReferenceError:
            pass  # dang

OCC_PROTOCOL_NAME    = b"occ 1"
SERVER_CHALLENGE     = 1001
CLIENT_PROTOCOL_NAME_NOAUTH = 2001
CLIENT_PROTOCOL_NAME        = 2002
CLIENT_USERNAME             = 2003
CLIENT_CHALLENGE_RESPONSE   = 2004
CLIENT_CURRENT_CLIENT_TIME  = 2005
CLIENT_CAL_CORRELATION_ID   = 2006
CONNECTION_CVAL_USERNAME    = b"username"
PCON_ENCRYPTED_AUTH_SECRET  = b"encrypted_auth_key"
PCON_PRIVATE_KEY_NAME       = b"b64_private_key"
SERVER_CONNECTION_ACCEPTED  = 1002
SERVER_CONNECTION_REJECTED_PROTOCOL     = 1003
SERVER_CONNECTION_REJECTED_UNKNOWN_USER = 1004
SERVER_CONNECTION_REJECTED_FAILED_AUTH  = 1005
SERVER_PING_COMMAND                     = 1008
SERVER_ALIVE                            = 1009
SERVER_CONNECTION_REJECTED_CLIENT_TIME  = 1010
