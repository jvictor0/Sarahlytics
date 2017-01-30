import db_utils
import tables
import joined
import temporal_band

class VideoObservation:
    def __init__(self, row):
        self.row = row
        self.lags = {}

    def NumLags(self):
        return len(self.lags)

    def ChannelId(self):
        return self.row["channel_id"]

    def VideoId(self):
        return self.row["video_id"]

    def AddLag(self, lag_time, lag_row):
        assert lag_time not in self.lags, (lag_time, self.lags)
        self.lags[lag_time] = lag_row

class VideoObservations:
    def __init__(self, lag_times):
        self.videos = {}
        self.lag_times = lag_times

    def FilterIncompleteRows(self):
        self.videos = {k:v for k,v in self.videos.iteritems() if len(self.lag_times) != v.NumLags()}

    def Ids(self):
        return [v.VideoId() for v in self.videos]

    def AddVideo(self, row):
        vp = VideoObservation(row)
        assert vp.VideoId() not in self.videos
        self.videos[vp.VideoId()] = vp

    def AddLag(self, lag_time, row):
        self.videos[row["video_id"]].AddLag(lag_time, row)

class LagTime:
    def __init__(self, time_secs, bound_secs=None):
        self.time_secs = time_secs
        if bound_secs is None:
            bound_secs = time_secs / 10
        self.bound_secs = bound_secs

    def TemporalBand(self, in_list):
        return temporal_band.TemporalBand(in_list=in_list, observed_at_secs=self.time_secs, observed_at_bounds_secs=self.bound_secs)
        
class VideoObservationsExtractor:
    def __init__(self, video_time_band, observe_time, lag_times):
        self.video_time_band = video_time_band
        self.observe_time = observe_time
        self.lag_times = lag_times
        self.observations = VideoObservations(lag_times)

    def GetInitialVideos(self, con):
        initial_videos_temporal_band = temporal_band.TemporalBand(time_band=self.video_time_band,
                                                                  observed_at_secs=self.observed_time.time_secs,
                                                                  observed_at_bounds_secs=self.observed_time.bound_secs).Query()
        initial_videos_query = joined.Joined(videos_facts=initial_videos_temporal_band, videos_facts_tags=None, closest_temporal='published_at').Query()
        # we need to pull predictors of this
        #
        rows = con.query(initial_videos_query)
        # ... will finish this later
        #
