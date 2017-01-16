import db_utils
import datetime
import copy

class Time:
    def __init__(self, con=None, time=None, time_ago=None):
        if time is None and time_ago is not None:
            if con is not None:
                self.time = "'%s'" % str(db_utils.DateTime(db_utils.Now(con)) - datetime.timedelta(seconds=time_ago))
            else:
                self.time = "%s - interval %d second" % (db_utils.NowExpr(), time_ago)
        else:
            self.time = time

    def Expr(self):
        return self.time

class TimeBand:
    def __init__(self, earliest=None, latest=None):
        self.earliest = earliest
        self.latest = latest

    def Expr(self, to_comp):
        if self.earliest is not None and self.latest is not None:
            return "%s between %s and %s" % (to_comp, self.earliest.Expr(), self.latest.Expr())
        elif self.earliest is not None:
            return "%s >= %s" (to_comp, self.earliest.Expr())
        elif self.latest is not None:
            return "%s <= %s" (to_comp, self.latest.Expr())
        else:
            assert False, "y tho?"

class TemporalBand:
    def __init__(self, table_name="videos_facts",
                 time_band=None,
                 min_views=None,
                 observed_at_secs=7*24*60*60,
                 observed_at_bounds_secs=24*60*60,
                 in_list=None,
                 predicates=[]):
        self.time_band = time_band
        self.observed_at_secs = observed_at_secs
        self.observed_at_bounds_secs = observed_at_bounds_secs
        self.min_views = min_views
        self.in_list = in_list
        self.predicates = predicates

    def InnerWhereClause(self):
        preds = copy.copy(self.predicates)
        if self.observed_at_bounds_secs is not None:
            preds.append("%s < %s" % (self.ObservedDiff(), self.observed_at_bounds_secs))
        if self.time_band is not None:
            preds.append(self.time_band.Expr("published_at"))
        if self.min_views is not None:
            preds.append("view_count >= %d" % self.min_views)
        if self.in_list is not None:
            preds.append("video_id in (%s)" % ",".join(["'%s'" % vid for vid in self.in_list]))
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
            select *, row_number() over (partition by channel_id, video_id order by %(observed_diff)s) as r
            from videos_facts
            %(inner_where_clause)s
        ) sub
        where r = 1
        """)
        return q % {"inner_where_clause":self.InnerWhereClause(),
                    "observed_diff":self.ObservedDiff()}

