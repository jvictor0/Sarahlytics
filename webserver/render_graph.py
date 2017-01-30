from database import db_utils
from database import tables
from analytics import time_series
import simplejson
import datetime

def MicroTimestampToDatetime(ts):
    ts = ts / (1000 * 1000)
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

class GraphRenderer:
    def __init__(self, http_handler, div_name, groups, matrix, properties):
        self.div_name = div_name
        self.http_handler = http_handler
        self.groups = groups
        self.matrix = matrix
        self.properties = properties
        self.wfile = http_handler.wfile

    def Render(self):
        self.wfile.write("<script src='//cdnjs.cloudflare.com/ajax/libs/dygraph/1.0.1/dygraph-combined.js' type='text/javascript'></script>\n")
        self.wfile.write("<div id='%s' style='width: 860px; height: 140px; margin-top: 10px;'></div>\n" % self.div_name)
        self.wfile.write("<script type='text/javascript'>\n")
        self.wfile.write("\ng  = new Dygraph(")
        self.wfile.write("\ndocument.getElementById('%s')," % self.div_name)
        self.wfile.write("\n\"DateTime," + ",".join(self.groups) + "\\n\"")
        for t,m in self.matrix:
            self.wfile.write(" + \"" + t + "," + ",".join([str(mi) for mi in m]) + "\\n\"")
        self.wfile.write("," + simplejson.dumps(self.properties) + ");\n")
        self.wfile.write("</script>")
        self.wfile.write("<div id='%s_labels' style='width: 860px; height: 140px; margin-top: 10px;'></div>\n" % self.div_name)

class VideoGraphRenderer:
    def __init__(self, http_handler, params):
        self.http_handler = http_handler
        self.channels = params["channels"].split(",") if "channels" in params else None
        self.videos = params["videos"].split(",") if "videos" in params else None
        self.con = db_utils.Connect()
        self.tables = tables.Tables()
        self.rows = None
        self.interpolator = None

    def Render(self):
        groups, matrix = self.GetMatrix()
        matrix = [(MicroTimestampToDatetime(ts), m) for ts,m in matrix]
        props = {
            "title" : "Video Views",
            "legend" : "always",
            "ylabel" : "Views",
            "labelsKMB" : True,
        }
        gr = GraphRenderer(self.http_handler, "video_views", groups, matrix, props)
        gr.Render()
        
    def GetVideosRows(self):
        projects = {
            "channel_id" : "channel_id",
            "channel_title" : "channel_title",
            "video_id" : "video_id",
            "video_title" : "video_title",
            "unix_timestamp(convert_tz(ts, 'gmt', 'system')) * 1000000" : "t",
            "view_count" : "view_count"}
        if self.rows is None:
            self.rows = self.con.query(self.tables.videos_facts.Get(projects, channels=self.channels, videos=self.videos))
        return self.rows

    def GetMatrix(self):
        if self.interpolator is None:
            self.interpolator = time_series.Interpolator("video_id", "view_count")
            self.interpolator.Interpolate(self.GetVideosRows())
        return self.interpolator.GroupMatrix()

    

    
