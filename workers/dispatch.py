import ingest_workers
import init_db
import sys

def WorkerFactory(name):
    if name == "database_init":
        return init_db.DatabaseInitWorker()
    elif name == "prepopulate":
        return init_db.PrepopulateWorker()
    elif name == "video_observe":
        return ingest_workers.VideoObserverWorker()
    elif name == "channel_observe":
        return ingest_workers.ChannelObserverWorker()
    elif name == "video_gather":
        return ingest_workers.VideoGatherWorker()
    elif name == "search":
        return ingest_workers.SearchWorker()
    else:
        assert False, name

        
if __name__ == "__main__":
    name = sys.argv[1]
    worker = WorkerFactory(name)
    worker.DoWork()
