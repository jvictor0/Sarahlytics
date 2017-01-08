import plotting
from database import db_utils
from database import tables
import api.config

def PostFrequencyQuery():
    hour_of_week = "hour(published_at) + 24 * (dayofweek(published_at) - 1)"
    q = """
    select 
        %(hour_of_week)s as hour_of_week,
        count(*) as total, 
        sum(view_count > 1000) as over_1000_views,
        sum(view_count > 10000) as over_10000_views,
        sum(view_count > 100000) as over_100000_views,
        sum(subscriber_count > 1000) as over_1000_subs,
        sum(subscriber_count > 10000) as over_10000_subs,
        sum(subscriber_count > 100000) as over_100000_subs,
        avg(view_count) as avg_view,
        avg(view_count / subscriber_count) avg_view_per_sub,
        avg(log(view_count)) as avg_log_view,
        avg(log(view_count / subscriber_count)) avg_log_view_per_sub
    from (%(joined)s) sub
    where subscriber_count > 300
    group by 1
    order by 1
    """
    tabs = tables.Tables()
    joined = tabs.Joined(videos_facts=tabs.videos_facts.MostRecent(), videos_facts_tags=None)    
    return db_utils.Dedent(q) % {"hour_of_week" : hour_of_week,
                                 "joined" : db_utils.Indent(joined)}

def SimpleScale(rows, x):
    new_rows = []
    maxs = {y:float(z) for y,z in rows[0].iteritems() if y != x}
    for r in rows:
        for y in maxs.keys():
            maxs[y] = max(maxs[y], float(r[y]))
    for r in rows:
        new_row = {x:r[x]}
        for y in maxs.keys():
            new_row[y] = float(r[y]) / maxs[y]
        new_rows.append(new_row)
    return new_rows

def PlotPostFrequency(con=None, rows=None, save=False, scale=True, views=True, subs=True):
    if con is None:
        con = db_utils.Connect()
    if rows is None:
        rows = con.query(PostFrequencyQuery())
    title_suffix = ""
    name_suffix = ""
    if subs and not views:
        title_suffix = " by subscribers"
        name_suffix = "_subs"
    if views and not subs:
        title_suffix = " by views"
        name_suffix = "_views"
    title  = "post frequency over the week" + title_suffix
    name = "post_frequency_week" + name_suffix if save else None
    if scale:
        rows = SimpleScale(rows, "hour_of_week")
        title = "scaled " + title
        if save:
            name = "scaled_" + name
    plot = plotting.WeekPlotter(title=title, name=name)
    if scale:
        plot.YLabel("scaled number of videos")
    else:
        plot.YLabel("number of videos")
    ys = ["total"]
    if views:
        if scale:
            ys.append("over_100000_views")
        else:
            ys.extend(["over_1000_views", "over_10000_views", "over_100000_views"])
    if subs:
        if scale:
            ys.append("over_100000_subs")
        else:
            ys.extend(["over_1000_subs", "over_10000_subs", "over_100000_subs"])
    for y in ys:
        label = "total" if y == "total" else "videos with " + y.replace("_", " ")
        plot.Plot(rows, label=label, y=y, x='hour_of_week')
    plot.Done()

def PlotPostStraightup(con=None, rows=None, y="avg_view"):
    if con is None:
        con = db_utils.Connect()
    if rows is None:
        rows = con.query(PostFrequencyQuery())
    plot = plotting.WeekPlotter(title=y.replace("_", " ") + " over a week", name=y+"_week")
    plot.Plot(rows, x="hour_of_week", y=y, label=y.replace("_", " "))
    plot.YLabel(y.replace("_","  "))
    plot.Done()
            
    
if __name__ == "__main__":
    con = db_utils.Connect()
    rows = con.query(PostFrequencyQuery())
    PlotPostFrequency(con=con, rows=rows, save=True, scale=True, subs=False)
    PlotPostFrequency(con=con, rows=rows, save=True, scale=True, views=False)
    PlotPostFrequency(con=con, rows=rows, save=True, scale=False, subs=False)
    PlotPostFrequency(con=con, rows=rows, save=True, scale=False, views=False)
    PlotPostStraightup(con=con, rows=rows, y="avg_view")
    PlotPostStraightup(con=con, rows=rows, y="avg_log_view")
    PlotPostStraightup(con=con, rows=rows, y="avg_view_per_sub")
    PlotPostStraightup(con=con, rows=rows, y="avg_log_view_per_sub")
