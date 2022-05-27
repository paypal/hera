"""
Low-Level Logging

A module to allow a ton of data (e.g. all SSL unencrypted and encrypted IO) to
be logged but not actually slow the server down unless the thing is being traced
or the whole server is logging super verbose.

Use like:

import ll
import faststat

ml = ll.LLogger()

....

ml.la("format string {0} {1}", var0, var1)  # always log

ml.ld("format string 2 {0}", var0)  # log most often
ml.ld("format string 3 {0}", var0)  # log most often
ml.ld2("format string 4 {0}", var0)  # log less often
ml.ld3("format string 5 {0}", var0)  # log only at very high verbosity
ml.ld4("format string 5 {0}", var0)  # log only at highest verbosity (including greenlet switch)

For best efficiency, use !r in format string, rather than calling str() or repr() on
arguments.

caustinlane@paypal.com for details.

"""

from __future__ import print_function
from __future__ import absolute_import, unicode_literals

import inspect
from collections import defaultdict
from datetime import datetime
import os
import sys

the_file = sys.stdout

log_msgs = defaultdict(int)


LOG_LEVELS = {'NEVER': -1,
              'NONE': 0,
              'DEBUG': 1,
              'DEBUG2': 2,
              'DEBUG3': 3,
              'DEBUG4': 4
              }


_log_level = LOG_LEVELS['NONE']


def print_log_summary():
    """Prints out the hash map of format strings and counts of usage."""
    return ["%s: %d\n".format(k, v) for k, v in log_msgs.items()]


def get_log_level():
    """Set global low level log level"""
    return _log_level


def set_log_level(level):
    """Set global low lovel log level"""
    global _log_level
    if level is None:
        level = 0
    level = max(level, LOG_LEVELS['NEVER'])
    level = min(level, LOG_LEVELS['DEBUG4'])
    _log_level = level


def use_the_file(name="lll.txt"):
    """Use a file instead of stdout
       Relative to cwd unless starts with /"""
    global the_file
    if name[0] == "/":
        path = name
    else:
        path = os.getcwd() + "/./" + name
    the_file = open(path, "a")


def use_std_out():
    """Use stdout instead of a file - just for tests"""
    global the_file
    the_file = sys.stdout


def log_failure(bad_str):
    """Stats on failed logs"""
    try:
        import context
        context.get_context().stats["log.failure"].add(1)
        context.get_context().stats["log.failure." + bad_str].add(1)
        if context.get_context().log_failure_print:
            if context.get_context().stats["log.failure"].n < 10:
                print("log failure - " + bad_str)
    except:
        pass


class LLogger(object):
    """Instantiate this to get the logger object; it grabs module data"""

    def __init__(self, tag="", trace_mod=False):
        mod = inspect.getmodule(inspect.stack()[1][0])
        if mod:
            self.caller_mod = mod.__file__.split(".")[-2].upper()
        else:
            self.caller_mod = "UNKNOWN"
        self.trace_mod = trace_mod
        self.la = self.log_always
        self.ld = self.log_debug
        self.ld2 = self.log_debug2
        self.ld3 = self.log_debug3
        self.ld4 = self.log_debug4
        self.tag = tag

    def log_always(self, *args, **kw):
        """Log unless never"""
        global log_msgs
        log_msgs[self.caller_mod + "--" + args[0]] += 1
        if self.trace_mod or _log_level >= 0:
            try:
                msg = args[0].format(*tuple(args[1:]))
                if "CAL-PRINT" in msg:
                    print(msg, file=the_file)
                else:
                    print("%s %s ():%s" % (datetime.now().strftime("%d/%H:%M:%S.%f"),
                                           self.caller_mod,
                                           self.tag),
                          msg,
                          file=the_file)
            except:
                log_failure(args[0])

    def log_debug(self, *args, **kw):
        """Log only with -d"""
        log_msgs[self.caller_mod + "--" + args[0]] += 1
        if self.trace_mod or _log_level >= 1:
            try:
                msg = args[0].format(*tuple(args[1:]))
                print("%s %s D ():%s" % (datetime.now().strftime("%d/%H:%M:%S.%f"),
                                         self.caller_mod, 
                                         self.tag), msg, file=the_file)
            except Exception as e:
                print (str(e))
                log_failure(args[0])

    def log_debug2(self, *args, **kw):
        """Log only with -dd"""
        log_msgs[self.caller_mod + "--" + args[0]] += 1
        if self.trace_mod or _log_level >= 2:
            try:
                msg = args[0].format(*tuple(args[1:]))
                print("%s %s D2 ():%s" % (datetime.now().strftime("%d/%H:%M:%S.%f"),
                                          self.caller_mod,
                                          self.tag), msg, file=the_file)
            except:
                log_failure(args[0])

    def log_debug3(self, *args, **kw):
        """Log only with -ddd"""

        log_msgs[self.caller_mod + "--" + args[0]] += 1
        if self.trace_mod or _log_level >= 3:
            try:
                msg = args[0].format(* tuple(args[1:]))
                print("%s %s D3 ():%s" % (datetime.now().strftime("%d/%H:%M:%S.%f"),
                                          self.caller_mod,
                                          self.tag), msg, file=the_file)
            except:
                log_failure(args[0])

    def log_debug4(self, *args, **kw):
        """Log only with -dddd"""
        global log_msgs
        log_msgs[self.caller_mod + "--" + args[0]] += 1
        if self.trace_mod or _log_level >= 4:
            try:
                msg = args[0].format(* tuple(args[1:]))
                print("%s %s D4 ():%s" % (datetime.now().strftime("%d/%H:%M:%S.%f"),
                                          self.caller_mod,
                                          self.tag), msg, file=the_file)
            except:
                log_failure(args[0])
