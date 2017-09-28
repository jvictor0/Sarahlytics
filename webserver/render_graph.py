from database import db_utils
from database import tables
from analytics import time_series
import simplejson
import os.path
import datetime
import time

def MicroTimestampToDatetime(ts):
    ts = ts / (1000 * 1000)
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")

def TimestampToDatetime(ts):
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")

def EscapeTitleForGraph(title):
    return title.replace(",","").replace('"',"")

def Color(name, alpha):
    ord = hash(name) % (16**6)
    return "rgba(%d, %d, %d, %f)" % (ord % 256, (ord / 256) % 256, (ord / (256**2)) % 256, alpha)


class LegacyDigraphGraphRenderer:
    def __init__(self, http_handler, div_name, groups, matrix, properties, indep_var="DateTime"):
        self.div_name = div_name
        self.http_handler = http_handler
        self.groups = groups
        self.matrix = matrix
        self.properties = properties
        self.wfile = http_handler.wfile
        self.indep_var = indep_var

    def Render(self):
        self.wfile.write("<script src='//cdnjs.cloudflare.com/ajax/libs/dygraph/1.0.1/dygraph-combined.js' type='text/javascript'></script>\n")
        self.wfile.write("<div id='%s' style='width: 860px; height: 660px; margin-top: 100px;'></div>\n" % self.div_name)
        self.wfile.write("<script type='text/javascript'>\n")
        self.wfile.write("\ng  = new Dygraph(")
        self.wfile.write("\ndocument.getElementById('%s')," % self.div_name)
        self.wfile.write("\n\"" + self.indep_var + "," + ",".join(map(EscapeTitleForGraph, self.groups)) + "\\n\"")
        for t,m in self.matrix:
            self.wfile.write(" + \"" + t + "," + ",".join([str(mi) for mi in m]) + "\\n\"")
        self.wfile.write("," + simplejson.dumps(self.properties) + ");\n")
        self.wfile.write("</script>")
        self.wfile.write("<div id='%s_labels' style='width: 860px; height: 660px; margin-top: 100px;'></div>\n" % self.div_name)
        
class ChartJsGraphRenderer:
    def __init__(self, http_handler, groups, matrix, stacked=False):
        self.http_handler = http_handler
        self.groups = groups
        self.matrix = matrix
        self.wfile = http_handler.wfile
        self.stacked = stacked
        
    def DataSetJSON(self):
        return [{
            "label" : self.groups[i],
            "pointRadius" : 2,
            "fill" : (True if i == 0 else '-1') if self.stacked else False,
            "borderColor" : Color(self.groups[i], 1.0),
            "backgroundColor" : Color(self.groups[i], 0.5),
            "data" : [r[1][i] for r in self.matrix]
        } for i in xrange(len(self.groups))]

    def OptionsJSON(self):
        return {
            "title": {
                "text": "woohoo"
            },
            "scales": {
                "xAxes": [{
                    "type": "time",
                    "time": {
                        "format": "YYYY-MM-DD HH:mm"
                    },
                    "scaleLabel": {
                        "display" : True,
                        "labelString": "Date"
                    }
                }],
                "yAxes": [{
                    "stacked" : self.stacked,
                    "scaleLabel": {
                        "display" : True,
                        "labelString": "value"
                    }
                }]
            }
        }

    def ConfigJSON(self):
        return {
            "type": 'line',
            "data": {
                "labels" : [r[0] for r in self.matrix],
                "datasets" : self.DataSetJSON()
            },
            "options" : self.OptionsJSON()
        }

    def RenderScript(self):
        self.wfile.write("var config = %s;\n" % simplejson.dumps(self.ConfigJSON(), indent=4 * ' '))
        self.wfile.write("""
        window.onload = function() 
        {
	    var ctx = document.getElementById("canvas").getContext("2d");
	    window.myLine = new Chart(ctx, config);        
	};
        """)
        
    def Render(self):
	self.wfile.write('<script src="https://cdnjs.cloudflare.com/ajax/libs/moment.js/2.13.0/moment.min.js"></script>\n')
	self.wfile.write('<script src="http://www.chartjs.org/dist/2.7.0/Chart.js"></script>\n')
	self.wfile.write('<script src="http://www.chartjs.org/samples/latest/utils.js"></script>\n')
        self.wfile.write('<body><div style="width:75%;"><canvas id="canvas"></canvas></div><script>\n')
        self.RenderScript()
        self.wfile.write('</script></body>')


        
def GetSQL(filename):
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "queries", filename)
    with open(path, "r") as f:
        return f.read()
    
class SqlMultiLineGraphRenderer:
    def __init__(self, http_handler, params):
        filename = None if "filename" not in params else params["filename"]
        self.http_handler = http_handler
        if sql is None:
            assert filename is not None
            sql = GetSQL(filename)
        self.params = params
        self.sql = sql % params
        if "verbose" in self.params:
            print self.sql
        if "title" not in self.params:
            self.params["title"] = "untitled"

    def Render(self):
        con = db_utils.Connect()
        rows = con.query(self.sql)
        indep_var = rows.fieldnames[0]
        groups = list(rows.fieldnames[1:])
        props = {
            "title" : "Video Views",
            "legend" : "always",
            "ylabel" : "Views",
            "labelsKMB" : True,
        }
        matrix = [(r[indep_var], [float(r[cn]) for cn in groups]) for r in rows]
        if "timestamp" in self.params:
            matrix = [(TimestampToDatetime(int(a)), b) for a,b in matrix]
        GraphRenderer(self.http_handler, self.params["title"], groups, matrix, props).Render()

        
class VideoGraphRenderer:
    def __init__(self, http_handler, params):
        self.http_handler = http_handler
        self.channels = params["channels"].split(",") if "channels" in params else None
        self.videos = params["videos"].split(",") if "videos" in params else None
        self.bin_hours = int(params["bin_hours"]) if "bin_hours" in params else 12
        self.per_hour = bool(int(params["per_hour"])) if "per_hour" in params else False
        self.stacked = bool(int(params["stacked"])) if "stacked" in params else False
        self.min_time = "convert_tz('%s', 'system', 'gmt')" % params["min_time"] if "min_time" in params else None
        if self.min_time is None and "hours_ago" in params:
            self.min_time = "convert_tz(now() - interval %s hour, 'system', 'gmt')" % params["hours_ago"]
        self.max_time = "convert_tz('%s', 'system', 'gmt')" % params["max_time"] if "max_time" in params else None
        if self.max_time is None and "total_hours" in params:
            assert self.min_time is not None
            self.max_time = "%s + interval %s hour" % params["total_hours"]
        self.con = db_utils.Connect()
        if "videos_query_file" in params:
            assert self.videos is None
            sql = GetSQL(params["videos_query_file"])
            self.videos = [r["video_id"] for r in self.con.query(sql % params)]
        self.tables = tables.Tables()
        self.rows = None
        self.interpolator = None

    def Render(self):
        pre_groups, matrix = self.GetMatrix()
        titles_order = self.con.query("select video_id, video_title from videos_facts where channel_id in (%s) and video_id in (%s) group by channel_id, video_id order by published_at"
                                    % (",".join(["'%s'" % k for k in self.channels]), ",".join(["'%s'" % k for k in self.videos])))
        indices = [pre_groups.index(r["video_id"]) for r in titles_order if r["video_id"] in pre_groups]
        groups = [r["video_title"] for r in titles_order if r["video_id"] in pre_groups]
        matrix = [(MicroTimestampToDatetime(ts), [m[ix] for ix in indices]) for ts,m in matrix]
        gr = ChartJsGraphRenderer(self.http_handler, groups, matrix, stacked=self.stacked)
        gr.Render()
        
    def GetVideosRows(self):
        projects = {
            "video_id" : "video_id",
            "unix_timestamp(convert_tz(ts, 'gmt', 'system')) * 1000000" : "t",
            "view_count" : "view_count"}
        if self.rows is None:
            self.rows = self.con.query(self.tables.videos_facts.Get(projects, channels=self.channels, videos=self.videos, min_time=self.min_time, max_time=self.max_time))
        indices = [self.rows.fieldnames.index("video_id"), self.rows.fieldnames.index("t"), self.rows.fieldnames.index("view_count")]
        return [time_series.DataPoint(r[indices[0]], int(r[indices[1]]), float(r[indices[2]])) for r in self.rows.values]

    def GetMatrix(self):
        rows = self.GetVideosRows()
        if self.interpolator is None:
            self.interpolator = time_series.Interpolator()
            self.interpolator.Interpolate(rows)
            if self.per_hour:
                diff  = self.interpolator.Differentiate(min_time=1000*1000*60*60*self.bin_hours)
                self.interpolator = time_series.Interpolator()
                self.interpolator.Interpolate(diff)
        return self.interpolator.GroupMatrix()

    

    
