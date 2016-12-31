import tables
import db_utils

def DropViews(con):
    while True:
        views = [r["table_name"] for r in con.query("select table_name from information_schema.views where table_schema='sarahlytics'")]
        if len(views) == 0:
            break
        for v in views:
            try:
                con.query("drop view if exists %s" % v)
            except Exception:
                pass

def CreateView(con, name, select_query):
    con.query("create view %s as %s" % (name, select_query))
            
def CreateViews(con):
    DropViews(con)
    tabs = tables.Tables()
    CreateView(con, "videos_mr", tabs.videos_facts.MostRecent())
    CreateView(con, "channels_mr", tabs.channels_facts.MostRecent())


if __name__ == "__main__":
    con = db_utils.Connect()
    CreateViews(con)
