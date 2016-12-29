import worker
from database import db_utils
from database import tables
from api import fetch
from api import config
from api import api
import random

class VideoObserverWorker(worker.Worker):
    def __init__(self, frequency=60*15):
        super(VideoObserverWorker, self).__init__(frequency=frequency)

    def DoWorkInternal(self):
        vid_rows = self.tables.videos_facts.GetVideosToObserve(self.con, 50)
        observations = fetch.ObserveVideos(vid_rows)
        self.tables.videos_facts.Insert(self.con, observations)

class VideoGatherWorker(worker.Worker):
    def __init__(self, frequency=60*5, max_daily_quota=30000):
        super(VideoGatherWorker, self).__init__(frequency=frequency)
        self.max_quota_per_work = max_daily_quota / (60 * 60 * 24 / frequency)

    def DoWorkInternal(self):
        channels_rows = self.tables.channels.ChannelsToProcess(self.con, channel=False, videos=True, limit=50)
        channels = [cr['channel_id'] for cr in channels_rows]
        stop_before = self.tables.videos_facts.GetMostRecentChannelVideo(self.con, channels)
        videos = fetch.FetchVideosForChannels(channels, max_pages_per_channel=10, stop_before=stop_before, max_quota=self.max_quota_per_work)
        the_channels = [v["channelId"] for v in videos]
        self.tables.videos_facts.Insert(self.con, videos)
        self.tables.channels.Process(self.con, the_channels, channel=False, videos=True)

class ChannelObserverWorker(worker.Worker):
    def __init__(self, frequency=60*15):
        super(ChannelObserverWorker, self).__init__(frequency=frequency)

    def DoWorkInternal(self):
        channels_rows = self.tables.channels.ChannelsToProcess(self.con, channel=True, videos=False, limit=50)
        channels = [cr['channel_id'] for cr in channels_rows]
        observations = fetch.ObserveChannels(channels, content_details=False)
        the_channels = [c["id"] for c in observations]
        self.tables.channels_facts.Insert(self.con, observations)
        self.tables.channels.Process(self.con, the_channels, channel=True, videos=False)

class SearchWorker(worker.Worker):
    def __init__(self, frequency=60*60*3):
        super(SearchWorker, self).__init__(frequency=frequency)
        self.search_terms = [st for st in config.search_terms]
        random.shuffle(self.search_terms)

    def DoWorkInternal(self):
        st = self.search_terms[0]
        self.search_terms = self.search_terms[1:] + [st]

        videos = api.Search(q=st, search_type="video", max_pages=100)
        self.tables.channels.Insert(self.con, [v["snippet"]["channelId"] for v in videos])

        channels = api.Search(q=st, search_type="channel", max_pages=100)
        self.tables.channels.Insert(self.con, [c["id"]["channelId"] for c in channels])
