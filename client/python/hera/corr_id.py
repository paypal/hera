"""
   generate CAL correlation IDs and eventually abstract where the
   corr ID is stored (TLS, greenlet, contextvars etc)
"""

from __future__ import print_function
from __future__ import absolute_import, unicode_literals

import time
import socket
import os

from . import ll

ml = ll.LLogger(trace_mod=False)

# import contextvars

# tls = contextvars.ContextVar("correlation_id")


# from core/lang/fnv/hash_64.c
# fnv is multiply-then-xor
def fnv_hash(text):
    'NON-cryptographic hash function; do not use for MAC or any other crypto application'
    sofar = 0xcbf29ce484222325 # initial value from fnv.h
    for c in text:
        sofar *= 0x100000001b3 # special prime
        sofar &= 2 ** 64 - 1
        sofar ^= ord(c)
    return sofar

# just for fun, could also be a single reduce call: reduce(lambda
# sofar,c: ((sofar*0x100000001b3)& 2**64-1)^ord(c), text,
# 0xcbf29ce484222325) but that would probably be less readable


# fnva is xor-then-multiply
def fnva_hash(text):
    sofar = 0x84222325cbf29ce4
    for c in text:
        sofar ^= ord(c)
        sofar *= 0x100000001b3 # special prime
        sofar &= 2 ** 64 - 1
    return sofar


def get_cur_correlation_id():
    #    cur = tls.get(None)
    #    if cur:
    #        ml.ld4( "Returning corr id {}", cur)
    #        return cur
    # TODO: where do different length correlation ids come from in CAL logs?
    t = time.time()
    corr_val = "{0}{1}{2}{3}".format(socket.gethostname(),
                                     os.getpid(), int(t), int(t % 1 * 10 ** 6))
    corr_id = "{0:x}{1:x}".format(
        fnv_hash(corr_val) & 0xFFFFFFFF,
        int(t % 1 * 10 ** 6) & 0xFFFFFFFF)
    ml.ld2("Generated corr_id {0}", corr_id)
    # tls.set(corr_id)
    ml.ld4("getting corr id {}", corr_id)
    return corr_id


def set_cur_correlation_id(corr_id):
    ml.ld4("Setting corr id {}", corr_id)
    # tls.set(corr_id)


def unset_cur_correlation_id():
    ml.ld4("Unsetting corr id")
    # tls.resset()
