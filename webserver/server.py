import sys
import BaseHTTPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler
import os
import os.path
import render_graph
import urlparse
import api.config
import config
import time

def SIN(dct, k, v):
    if k not in dct:
        dct[k] = v

class Handler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        up = urlparse.urlparse(self.path)
        params = urlparse.parse_qs(up.query)
        params = {k: v[0] for k, v in params.iteritems()}
        t0 = time.time()
        if up.path == "/videos_graph":
            self.SendOkHeader()
            render_graph.VideoGraphRenderer(self, params).Render()
        elif up.path == "/sqlmlg":
            self.SendOkHeader()
            render_graph.SqlMultiLineGraphRenderer(self, params).Render()
        elif up.path == "/recent_top_videos":
            self.SendOkHeader()
            params["videos_query_file"] = "top_recent_videos.sql"
            SIN(params, "limit", 30)
            SIN(params, "channel_id", api.config.my_channel)
            SIN(params, "hours", str(60 * 24))
            SIN(params, "bin_hours", str(int(params["hours"]) / 60))
            SIN(params, "per_hour", "1")
            SIN(params, "stacked", "1")
            SIN(params, "channels", params["channel_id"])
            if "min_time" not in params and "max_time" not in params:
                SIN(params, "hours_ago", str(int(params["hours"])))
            render_graph.VideoGraphRenderer(self, params).Render()
        else:
            self.send_error(404, "wtf")
        print "took %f secs" % (time.time() - t0)
    protocol_version = "HTTP/1.0"

    def SendOkHeader(self):
        self.send_response(200)
        self.send_header("Content-type","text/html")
        self.end_headers()
        
    
    
def Main():
    port = config.port
    server_address = (config.host, port)
    
    httpd = BaseHTTPServer.HTTPServer(server_address, Handler)
    
    sa = httpd.socket.getsockname()
    print "Serving HTTP on", sa[0], "port", sa[1], "..."
    httpd.serve_forever()

    
if __name__ == "__main__":
    Main()
