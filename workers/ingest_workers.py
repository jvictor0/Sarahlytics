import worker
from database import db_utils
from database import tables
from api import fetch

class VideoObserverWorker(worker.Worker):
    def __init__(self, frequency=60 * 15):
        super(VideoObserverWorker, self).__init__(frequency=frequency)

    def DoWorkInternal(self):
        vid_rows = self.tables.videos_facts.GetVideosToObserve(self.con, 50)
        observations = fetch.ObserveVideos(vid_rows)
        self.tables.videos_facts.Insert(self.con, observations)

class VideoGatherWorker(worker.Worker):
    def __init__(self, frequency=60*15, max_daily_quota=30000):
        super(VideoGatherWorker, self).__init__(frequency=frequency)
        self.max_quota_per_work = max_daily_quota / (60 * 60 * 24 / frequency)

    def DoWorkInternal(self):
        channels_rows = self.tables.channels.ChannelsToProcess(self.con, channel=False, videos=True, limit=50)
        channels = [cr['channel_id'] for cr in channels_rows]
        stop_before = self.tables.videos_facts.GetMostRecentChannelVideo(self.con, channels)
        videos = fetch.FetchVideosForChannels(channels, max_pages_per_channel=10, stop_before=stop_before, max_quota=self.max_quota_per_work)
        self.tables.videos_facts.Insert(self.con, videos)
        self.tables.channels.Process(self.con, channels, channel=False, videos=True)
