import sys
import json
from time import sleep
from threading import Thread
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

class MyHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/ruok':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write('imok')

def read_cfg():
    with open('slave.cfg') as f:
        return json.load(f)

if __name__ == '__main__':
    cfg = read_cfg()
    host = cfg["host"]
    port = cfg["port"]
    httpd = HTTPServer((host, port), MyHTTPRequestHandler)
    httpd.serve_forever()

