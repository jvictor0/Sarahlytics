import time
import threading
import traceback
from database import db_utils
from database import tables

g_log_lock = threading.Lock()

class Worker(object):
    def __init__(self, frequency=None):
        self.last = time.time()
        self.frequency = frequency
        self.name = self.__class__.__name__
        self.con = db_utils.Connect()
        self.tables = tables.Tables()

    def Log(self, msg):
        with g_log_lock:
            preamble = "[%s] (%s) " % (self.name, time.strftime("%H:%M:%S", time.gmtime()))
            print preamble + msg.replace("\n","\n" + (" " * len(preamble)))

    def Ready(self):
        if self.frequency is None:
            return False
        else:
            return time.time() > self.last + self.frequency

    def DoWork(self):
        self.last = time.time()
        try:
            self.DoWorkInternal()
        except Exception:
            self.Log(traceback.format_exc())
            raise
    
    def DoWorkInternal(self):
        assert False, "override me"
