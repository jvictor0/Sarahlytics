import tables
import db_utils


class Joined:
    def __init__(self,
                 channels_facts='base',
                 videos_facts='base',
                 videos_facts_tags='base',
                 closest_temporal='ts',
                 predicates=[],
                 filter_f=True):
        self.tables = tables.Tables()
        self.channels_facts = channels_facts
        self.videos_facts = videos_facts
        self.videos_facts_tags = videos_facts_tags
        self.closest_temporal = closest_temporal
        self.predicates = predicates
        if filter_f:
            if self.channels_facts is not None:
                self.predicates.append("channels_facts_f")
            if self.videos_facts is not None:
                self.predicates.append("videos_facts_f")

    def DiffRank(self, secondary):
        parts = ["videos_facts.channel_id", "videos_facts.video_id"]
        return "row_number() over (partition by %s order by abs(timestampdiff(second, %s, videos_facts.%s)))" % (",".join(parts), secondary, self.closest_temporal)
        
    def Query(self):
        result = "select\n    "
        cols = []
        joins = []
        if self.channels_facts is not None:
            cols.append("channels_facts.ts as channels_facts_ts")
            cols.append("channels_facts.f as channels_facts_f")
            for c in self.tables.channels_facts.columns:
                if c.name not in ["f"]:
                    cols.append("channels_facts.%s" % c.name)
        if self.videos_facts is not None:
            cols.append("videos_facts.ts as videos_facts_ts")
            cols.append("videos_facts.f as videos_facts_f")
            if self.channels_facts is not None:
                joins.append("channels_facts.channel_id = videos_facts.channel_id")
                if self.closest_temporal:
                    cols.append(self.DiffRank("channels_facts.ts") + " as channel_video_time_rank")
            for c in self.tables.videos_facts.columns:
                if self.channels_facts is not None and c.name in [cfc.name for cfc in self.tables.channels_facts.columns]:
                    continue
                cols.append("videos_facts.%s" % c.name)
        if self.videos_facts_tags is not None:
            if self.closest_temporal:
                cols.append(self.DiffRank("videos_facts_tags.ts") + " as video_tag_time_rank")
            assert self.videos_facts is not None
            joins.append("videos_facts_tags.channel_id = videos_facts.channel_id")
            joins.append("videos_facts_tags.video_id = videos_facts.video_id")
            cols.append("videos_facts_tags.ts as videos_facts_tags_ts")
            cols.append("videos_facts_tags.tag")

        result += ",\n    ".join(cols) + "\n"
        result += "from "
        
        result += self.FromClause()
        
        result += "on " + " and ".join(joins)

        result += self.WhereClause()

        if self.closest_temporal is not None:
            preds = []
            if self.channels_facts is not None:
                preds.append("channel_video_time_rank = 1")
            if self.videos_facts_tags is not None:
                preds.append("video_tag_time_rank = 1")                
            true_result = db_utils.Dedent("""
            select * from
            (%s) sub
            where %s            
            """)
            return true_result % (db_utils.Indent(result), " and ".join(preds))
        else:
            return result

    def WhereClause(self):
        if len(self.predicates) == 0:
            return ""
        return "\nwhere " + "\n  and ".join(self.predicates) + "\n"
        
    def FromClause(self):
        tabs = []
    
        if self.channels_facts == "base":
            tabs.append("channels_facts")        
        elif self.channels_facts is not None:
            tabs.append("(%s) channels_facts" % db_utils.Indent(self.channels_facts))

        if self.videos_facts == "base":
            tabs.append("videos_facts")
        elif self.videos_facts is not None:
            tabs.append("(%s) videos_facts" % db_utils.Indent(self.videos_facts))

        if self.videos_facts_tags == "base":
            tabs.append("videos_facts_tags")
        elif self.videos_facts_tags is not None:
            tabs.append("(%s) videos_facts_tags" % db_utils.Indent(self.videos_facts_tags))

        return "\n" + "join\n".join(tabs) + "\n"

    def Execute(self, con):
        return con.query(self.Query())
