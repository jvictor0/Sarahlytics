import json_table
import db_utils
import temporal_band

class Channels:
    def __init__(self):
        pass

    def ToSQL(self):
        return """
        create table channels(
           channel_id blob primary key, 
           channel_processed datetime default null, 
           videos_processed datetime default null)
        """

    def Create(self, con):
        con.query(self.ToSQL())

    def Insert(self, con, channel_ids):
        con.query("insert ignore into channels(channel_id) values" + ",".join(["('%s')" % cid for cid in channel_ids]))

    def Process(self, con, channel_ids, channel, videos):
        if len(channel_ids) == 0:
            return
        q = "update channels set "
        now = db_utils.Now(con)
        if channel:
            q += "channel_processed = '%s' " % now
            if videos:
                q += ", "
        if videos:
            q += "videos_processed = '%s' " % now
        q += "where channel_id in (%s)" % ",".join(["'%s'" % cid for cid in channel_ids])
        con.query(q)

    def ChannelsToProcess(self, con, channel, videos, limit):
        if channel and videos:
            ob = "least(channel_processed, videos_processed)"
        elif channel:
            ob = "channel_processed"
        elif videos:
            ob = "videos_processed"
        q = """
        select channel_id, channel_processed, videos_processed
        from channels
        order by %s
        limit %d
        """
        q = q % (ob, limit)

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
        
        two_days = "pow(1.75, timestampdiff(minute, published_at, '%s') / (60 * 24))" % now
        hours_since = "(timestampdiff(minute, ts, '%s') / 60)" % now
        
        q = """
        select channel_id, video_id, published_at, ts, %(two_days)s as two_days, %(hours_since)s as hours_since
        from (%(most_recent_videos)s) videos_most_recent
        where %(two_days)s < %(hours_since)s
        order by published_at desc
        limit %(limit)d
        """
        return con.query(q % {"most_recent_videos" : self.VideosMostRecent(),
                              "limit" : limit,
                              "two_days" : two_days,
                              "hours_since" : hours_since})

                                             
class Tables:
    def __init__(self):
        self.channels = Channels()
        self.channels_facts = ChannelsFacts()
        self.videos_facts = VideosFacts()

    def Create(self, con):
        self.channels.Create(con)
        self.channels_facts.Create(con)
        self.videos_facts.Create(con)

    def Joined(self, channels_facts='base', videos_facts='base', videos_facts_tags='base', closest_temporal=True):
        result = "select\n    "
        cols = []
        joins = []
        if channels_facts is not None:
            cols.append("channels_facts.ts as channels_facts_ts")
            cols.append("channels_facts.f as channels_facts_f")
            for c in self.channels_facts.columns:
                if c.name not in ["f"]:
                    cols.append("channels_facts.%s" % c.name)
        if videos_facts is not None:
            cols.append("videos_facts.ts as videos_facts_ts")
            cols.append("videos_facts.f as videos_facts_f")
            if channels_facts is not None:
                joins.append("channels_facts.channel_id = videos_facts.channel_id")
                if closest_temporal:
                    cols.append("rank() over (partition by videos_facts.channel_id, videos_facts.video_id order by abs(timestampdiff(second, channels_facts.ts, videos_facts.ts))) as channel_video_time_rank")
            for c in self.videos_facts.columns:
                if channels_facts is not None and c.name in [cfc.name for cfc in self.channels_facts.columns]:
                    continue
                cols.append("videos_facts.%s" % c.name)
        if videos_facts_tags is not None:
            if closest_temporal:
                cols.append("rank() over (partition by videos_facts.channel_id, videos_facts.video_id order by abs(timestampdiff(second, videos_facts_tags.ts, videos_facts.ts))) as video_tag_time_rank")
            assert videos_facts is not None
            joins.append("videos_facts_tags.channel_id = videos_facts.channel_id")
            joins.append("videos_facts_tags.video_id = videos_facts.video_id")
            cols.append("videos_facts_tags.ts as videos_facts_tags_ts")
            cols.append("videos_facts_tags.tag")
        result += ",\n    ".join(cols) + "\n"
        result += "from "

        tabs = []
        
        if channels_facts == "base":
            tabs.append("channels_facts")        
        elif channels_facts is not None:
            tabs.append("(%s) channels_facts" % db_utils.Indent(channels_facts))

        if videos_facts == "base":
            tabs.append("videos_facts")
        elif videos_facts is not None:
            tabs.append("(%s) videos_facts" % db_utils.Indent(videos_facts))

        if videos_facts_tags == "base":
            tabs.append("videos_facts_tags")
        elif videos_facts_tags is not None:
            tabs.append("(%s) videos_facts_tags" % db_utils.Indent(videos_facts_tags))

        result += "\njoin\n".join(tabs) + "\n"
            
        result += "on " + " and ".join(joins)

        if closest_temporal:
            preds = []
            if channels_facts is not None:
                preds.append("channel_video_time_rank = 1")
            if videos_facts_tags is not None:
                preds.append("video_tag_time_rank = 1")                
            true_result = db_utils.Dedent("""
            select * from
            (%s) sub
            where %s            
            """)
            return true_result % (db_utils.Indent(result), " and ".join(preds))
        else:
            return result
