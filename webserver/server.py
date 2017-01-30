import sys
import BaseHTTPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler
import os
import os.path
import render_graph
import urlparse

class Handler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        up = urlparse.urlparse(self.path)
        params = urlparse.parse_qs(up.query)
        params = {k: v[0] for k, v in params.iteritems()}
        print params
        if up.path == "/videos_graph":
            self.SendOkHeader()
            render_graph.VideoGraphRenderer(self, params).Render()
        else:
            self.send_error(404, "wtf")
        
    protocol_version = "HTTP/1.0"

    def SendOkHeader(self):
        self.send_response(200)
        self.send_header("Content-type","text/html")
        self.end_headers()
        
    
    
def Main():
    port = 8000
    server_address = ('127.0.0.1', port)
    
    httpd = BaseHTTPServer.HTTPServer(server_address, Handler)
    
    sa = httpd.socket.getsockname()
    print "Serving HTTP on", sa[0], "port", sa[1], "..."
    httpd.serve_forever()

    
if __name__ == "__main__":
    Main()
