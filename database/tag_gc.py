import db_utils
import tables

def GCTags(con):
    con.query("alter table videos_facts_tags rename videos_facts_tags_bak")
    tables.Tables().videos_facts.tags.Create(con)
    con.query("""
              insert into videos_facts_tags(channel_id, video_id, tag, ts) 
              select channel_id, video_id, tag, min(ts) 
              from videos_facts_tags_bak
              group by channel_id, video_id, tag""")
    con.query("drop table videos_facts_tags_bak")

if __name__ == "__main__":
    GCTags(db_utils.Connect())
