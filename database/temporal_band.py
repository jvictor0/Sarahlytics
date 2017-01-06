import db_utils
import datetime

class TemporalBand:
    def __init__(self, table_name="videos_facts",
                 con=None,
                 earliest=None, latest=None,
                 earliest_ago=None, latest_ago=None,
                 min_views=None,
                 observed_at_secs=7*24*60*60,
                 observed_at_bounds_secs=24*60*60):
        if earliest is None and earliest_ago is not None:
            assert con is not None
            self.earliest = str(db_utils.DateTime(db_utils.Now(con)) - datetime.timedelta(seconds=earliest_ago))
        else:
            self.earliest = earliest
        if latest is None and latest_ago is not None:
            assert con is not None
            self.latest = str(db_utils.DateTime(db_utils.Now(con)) - datetime.timedelta(seconds=earliest_ago))
        else:
            self.latest = latest
        self.observed_at_secs = observed_at_secs
        self.observed_at_bounds_secs = observed_at_bounds_secs
        self.min_views = min_views

    def InnerWhereClause(self):
        preds = []
        if self.observed_at_bounds_secs is not None:
            preds.append("%s < %s" % (self.ObservedDiff(), self.observed_at_bounds_secs))
        if self.earliest is not None:
            preds.append("published_at >= '%s'" % self.earliest)
        if self.latest is not None:
            preds.append("published_at <= '%s'" % self.latest)
        if self.min_views is not None:
            preds.append("view_count >= %d" % self.min_views)
        if len(preds) == 0:
            return ""
        else:
            return "where " + "\n      and ".join(preds)

    def ObservedDiff(self):
        return "abs(timestampdiff(second, published_at, ts) - %d)" % self.observed_at_secs
                
    def Query(self):
        q = db_utils.Dedent("""
        select * from 
        (
            select *, rank() over (partition by channel_id, video_id order by %(observed_diff)s) as r
            from videos_facts
            %(inner_where_clause)s
        ) sub
        where r = 1
        """)
        return q % {"innerwhere_clouase":self.InnerWhereClause(),
                    "observed_diff":self.ObservedDiff()}

