import json_table
import db_utils
import temporal_band
import joined

class Channels:
    def __init__(self):
        pass

    def ToSQL(self):
        return """
        create table channels(
           channel_id blob primary key, 
           processed datetime default null)
        """

    def Create(self, con):
        con.query(self.ToSQL())

    def Insert(self, con, channel_ids):
        con.query("insert ignore into channels(channel_id) values" + ",".join(["('%s')" % cid for cid in channel_ids]))

    def Process(self, con, channel_ids):
        if len(channel_ids) == 0:
            return
        q = "update channels set "
        now = db_utils.Now(con)
        q += "processed = '%s' " % now
        q += "where channel_id in (%s)" % ",".join(["'%s'" % cid for cid in channel_ids])
        con.query(q)

    def ChannelsToProcess(self, con, limit):
        q = """
        select channel_id, processed
        from channels
        order by processed
        limit %d
        """
        q = q % (limit)

        return con.query(q)

class ChannelsFacts(json_table.JSONTable):
    def __init__(self):
        super(ChannelsFacts, self).__init__("channels_facts",
                                            [json_table.JSONColumn("channel_id", "blob", False, ["id"]),
                                             json_table.JSONColumn("channel_title", "blob", True, ["snippet", "title"]),
                                             json_table.JSONColumn("country", "blob", True, ["snippet", "country"]),
                                             json_table.JSONColumn("etag", "blob", True, ["etag"]),                                            
                                             json_table.JSONColumn("channel_view_count", "bigint", True, ["statistics", "viewCount"]),
                                             json_table.JSONColumn("comment_count", "bigint", True, ["statistics", "commentCount"]),
                                             json_table.JSONColumn("video_count", "bigint", True, ["statistics", "videoCount"]),
                                             json_table.JSONColumn("subscriber_count", "bigint", True, ["statistics", "subscriberCount"]),
                                             json_table.JSONColumn("created", "datetime", True, ["snippet", "publishedAt"]),
                                             json_table.JSONColumn("f", "tinyint", False, ["f"])],
                                            ["channel_id","ts"],
                                            ["channel_id"])
                                             
class VideosFacts(json_table.JSONTable):
    def __init__(self):
        super(VideosFacts, self).__init__("videos_facts",
                                          [json_table.JSONColumn("channel_id", "blob", False, ["channelId"]),
                                           json_table.JSONColumn("channel_title", "blob", True, ["snippet", "channelTitle"]),
                                           json_table.JSONColumn("video_id", "blob", False, ["id"]),
                                           json_table.JSONColumn("video_title", "blob", True, ["snippet", "title"]),
                                           json_table.JSONColumn("category_id", "blob", True, ["snippet", "categoryId"]),
                                           json_table.JSONColumn("default_audio_language", "blob", True, ["snippet", "defaultAudioLanguage"]),
                                           json_table.JSONColumn("default_language", "bigint", True, ["snippet", "defaultLanguage"]),
                                           json_table.JSONColumn("etag", "blob", True, ["etag"]),
                                           json_table.JSONColumn("published_at", "datetime", False, ["publishedAt"]),
                                           json_table.JSONColumn("view_count", "bigint", True, ["statistics", "viewCount"]),
                                           json_table.JSONColumn("like_count", "bigint", True, ["statistics", "likeCount"]),
                                           json_table.JSONColumn("dislike_count", "bigint", True, ["statistics", "dislikeCount"]),
                                           json_table.JSONColumn("favorite_count", "bigint", True, ["statistics", "favoriteCount"]),
                                           json_table.JSONColumn("comment_count", "bigint", True, ["statistics", "commentCount"]),
                                           json_table.JSONColumn("f", "tinyint", False, ["f"])],
                                          ["channel_id","video_id","ts"],
                                          ["channel_id"])

        self.tags = json_table.NormalizedArrayTable("tag", "blob", True, self, ["snippet","tags"], ["channel_id","video_id","tag","ts"])

    def TemporalBand(self, **kwargs):
        return temporal_band.TemporalBand(video_name="videos_facts", **kwargs).Query()
        
    def VideosMostRecent(self):
        return """
        select channel_id, video_id, max(published_at) as published_at, max(ts) as ts
        from videos_facts
        group by channel_id, video_id
        """

    def MostRecentChannelVideo(self, cids=None):
        if cids is None:
            where_clause = ""
        else:
            where_clause = "where channel_id in (%s)" % ",".join(["'%s'" % cid for cid in cids])
        q = """
        select channel_id, max(published_at) as most_recent_date
        from videos_facts
        %s
        group by channel_id
        """
        return q % where_clause

    def GetMostRecentChannelVideo(self, con, cids=None):
        if len(cids) == 0:
            return {}
        return {r["channel_id"] : db_utils.DateTime(r["most_recent_date"]) for r in con.query(self.MostRecentChannelVideo(cids=cids))}

    def GetVideosToObserve(self, con, limit):
        now = db_utils.Now(con)

        # Want to obeserve videos exponentially less as we got on.
        # Specifically, The time between observations should be 2^(num_days) hours
        #
        time_since_published = "timestampdiff(minute, published_at, '%s')" % now
        two_days = "pow(1.75, %s / (60 * 24))" % time_since_published
        hours_limit = "least(%s / (60 * 4), %s)" % (time_since_published, two_days)
        hours_since = "(timestampdiff(minute, ts, '%s') / 60)" % now
        
        q = """
        select channel_id, video_id, published_at, ts, %(hours_limit)s as hours_limit, %(hours_since)s as hours_since
        from (%(most_recent_videos)s) videos_most_recent
        where %(hours_limit)s < %(hours_since)s
        order by published_at desc
        limit %(limit)d
        """
        q = q % {"most_recent_videos" : self.VideosMostRecent(),
                 "limit" : limit,
                 "hours_limit" : hours_limit,
                 "hours_since" : hours_since}
        return con.query(q)

    def VideosBy(self, con, channel_ids):
        q = """
        select channel_id, video_id, max(published_at) published_at
        from videos_facts
        where channel_id in (%s)
        group by channel_id, video_id
        """
        return con.query(q % ",".join(["'%s'" % c for c in channel_ids]))

                                             
class Tables:
    def __init__(self):
        self.channels = Channels()
        self.channels_facts = ChannelsFacts()
        self.videos_facts = VideosFacts()

    def Create(self, con):
        self.channels.Create(con)
        self.channels_facts.Create(con)
        self.videos_facts.Create(con)

    def Joined(self, **kwargs):
        return joined.Joined(**kwargs).Query()
