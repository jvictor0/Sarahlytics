import worker
from database import tag_gc
import time

class TagGCWorker(worker.Worker):
    def __init__(self, frequency=12*60*60):
        super(TagGCWorker, self).__init__(frequency=frequency)

    def DoWorkInternal(self):
        t0 = time.time()
        tag_gc.GCTags(self.con)
        self.Log("took %f secs to GC videos_facts_tags" % (time.time() - t0))
