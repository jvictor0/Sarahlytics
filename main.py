from workers import ingest_workers
import time

if __name__ == "__main__":
    workers = []

    workers.append(ingest_workers.SearchWorker())
    workers.append(ingest_workers.ChannelObserverWorker())
    workers.append(ingest_workers.ImportantVideoObserverWorker())
    workers.append(ingest_workers.VideoObserverWorker())
    
    while True:
        for w in workers:
            if w.Ready():
                w.DoWork()
