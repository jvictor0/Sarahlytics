import worker
from database import db_utils
from database import tables
from api import fetch
from api import config
from api import api
import random

class VideoObserverWorker(worker.Worker):
    def __init__(self, frequency=60*5, max_daily_quota=300000):
        super(VideoObserverWorker, self).__init__(frequency=frequency)
        self.max_quota_per_work = max_daily_quota / (60 * 60 * 24 / frequency)

    def DoWorkInternal(self):
        vid_rows = self.tables.videos_facts.GetVideosToObserve(self.con, api.VideosForCost(self.max_quota_per_work / 2))
        observations = fetch.ObserveVideos(vid_rows)
        channels = list(set([vr["channel_id"] for vr in vid_rows]))
        channel_observations = fetch.ObserveChannels(channels, content_details=False)
        now = db_utils.Now(self.con)
        self.tables.videos_facts.Insert(self.con, observations, now=now)
        self.tables.channels_facts.Insert(self.con, channel_observations, now=now)
        self.Log("found %d to observe, observed %d videos over %d channels" % (len(vid_rows), len(observations), len(channels)))

class ImportantVideoObserverWorker(worker.Worker):
    def __init__(self, frequency=60*60):
        super(ImportantVideoObserverWorker, self).__init__(frequency=frequency)

    def DoWorkInternal(self):
        vid_rows = list(self.tables.videos_facts.VideosBy(self.con, config.important_channels))
        video_id_set = set([vr["video_id"] for vr in vid_rows])
        channels_observations = fetch.ObserveChannelsWithLatestVideos(config.important_channels, content_details=False)
        for c, uploads in channels_observations:
            for u in uploads:
                if u["video_id"] not in video_id_set:
                    vid_rows.append(u)
        observations = fetch.ObserveVideos(vid_rows)
        now = db_utils.Now(self.con)
        self.tables.videos_facts.Insert(self.con, observations, now=now)
        self.tables.channels_facts.Insert(self.con, [c for c,u in channels_observations], now=now)
        self.Log("found %d to observe, observed %d videos" % (len(vid_rows), len(observations)))

class ChannelObserverWorker(worker.Worker):
    def __init__(self, frequency=60*5, max_daily_quota=300000):
        super(ChannelObserverWorker, self).__init__(frequency=frequency)
        self.max_quota_per_work = max_daily_quota / (60 * 60 * 24 / frequency)

    def DoWorkInternal(self):
        quota = [0]
        gathered_channels = 0
        gathered_videos = 0
        gathered_from = 0
        stop_before = self.tables.videos_facts.GetMostRecentChannelVideo(self.con)
        while quota[0] < self.max_quota_per_work:
            channels_rows = self.tables.channels.ChannelsToProcess(self.con, limit=50)
            if len(channels_rows) == 0:
                break
            channels = [cr['channel_id'] for cr in channels_rows]
            gathered_channels += len(channels)            
            the_channels, videos = fetch.FetchVideosForChannels(channels, max_pages_per_channel=10, quota=quota, stop_before=stop_before, max_quota=self.max_quota_per_work)
            gathered_from += len(the_channels)
            gathered_videos += len(videos)
            channel_ids = [c["id"] for c in the_channels]
            self.tables.videos_facts.Insert(self.con, videos)
            self.tables.channels_facts.Insert(self.con, the_channels)
            self.tables.channels.Process(self.con, channel_ids)
        self.Log("Gathering for %d channels, found %d new videos over %d channels" % (gathered_channels, gathered_videos, gathered_from))

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

        self.Log("searched for term '%s', found %d videos and %d channels" % (st, len(videos), len(channels)))
