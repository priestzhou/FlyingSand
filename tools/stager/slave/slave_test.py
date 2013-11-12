import unittest
from threading import Thread
from time import sleep
import os
import os.path as op
import shutil
import httplib
import json
import psutil
from threading import Thread
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
import slave

class MyHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/ver/prog.clj':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write('''\
(ns prog)
(defn -main [& args]
    (spit "prog.out" "1" :append true)
    (Thread/sleep 5000)
    (spit "prog.out" "0" :append true)
)
''')
        elif self.path == '/ver/prog.py':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write('''\
from time import sleep
with open('prog.out', 'a') as f:
    f.write('1')
sleep(5)
with open('prog.out', 'a') as f:
    f.write('0')
''')
        else:
            self.send_response(404)

class TestSlave(unittest.TestCase):
    def put(self, path, message):
        conn = httplib.HTTPConnection('localhost', 11111)
        conn.request('PUT', path, message, {"Content-Type": "application/json"})
        resp = conn.getresponse()
        self.assertEqual(resp.status, 200)
        return resp.read()

    def get(self, path):
        conn = httplib.HTTPConnection('localhost', 11111)
        conn.request('GET', '/ruok')
        resp = conn.getresponse()
        self.assertEqual(resp.status, 200)
        return resp.read()

    @classmethod
    def setUpClass(cls):
        cls.httpd = HTTPServer(('localhost', 11110), MyHTTPRequestHandler)
        def start_server():
            cls.httpd.serve_forever()
        t = Thread(target=start_server)
        t.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()

    def setUp(self):
        self.root_dir = os.tempnam()
        os.makedirs(self.root_dir)
        print 'root', self.root_dir
        shutil.copy('tools/stager/slave/slave.py', self.root_dir)
        shutil.copy('common/extlib/clojure-1.5.1.jar', self.root_dir)
        with open(op.join(self.root_dir, 'slave.cfg'), 'w') as f:
            f.write('{"host": "localhost", "port": 11111}\n')
        with open(op.join(self.root_dir, 'apps.cfg'), 'w') as f:
            f.write('{}\n')
        self.slave = psutil.Popen(['python2.7', '-B', 'slave.py'],
            cwd=self.root_dir)
        sleep(1)

    def tearDown(self):
        self.slave.kill()
        self.slave.wait()
        sleep(0.1)

    def test_ruok(self):
        r = self.get('/ruok')
        self.assertEqual(r, 'imok')

    def test_files(self):
        fs = list(slave.files({
            'app': "a", 'ver': "w",
            'sources': ['http://h0/', 'http://h1/'],
            'files': {
                "b": "x",
                "c": "y",
                "d": "z"
            }
        }))
        self.assertEqual(fs, [
                ["http://h0/w/x", "cache/w/x", "apps/a/w/b"],
                ["http://h1/w/y", "cache/w/y", "apps/a/w/c"],
                ["http://h0/w/z", "cache/w/z", "apps/a/w/d"]
        ])

    def test_start_stop(self):
        # start an app
        self.put('/apps/', json.dumps([
                {
                    'app': "app", 'ver': "ver", "cfg-ver": "0",
                    'sources': ['http://localhost:11110/'],
                    'files': {"prog.clj": "prog.clj"},
                    "executable": {
                        'executable-type': 'clj',
                        'class-paths': [],
                        'main': 'prog',
                        'args': []
                    }
                }
            ]))
        sleep(3) # it takes ~2s to start jvm up
        # stop the app
        self.put('/apps/', json.dumps([]))
        # if it was not stopped, then
        sleep(5) # 5s to run prog
        sleep(1) # 1s to let slave notice
        sleep(3) # and restart
        with open(op.join(slave.app_root('app', 'ver', self.root_dir), 'prog.out')) as f:
            self.assertEqual(f.read(), '1')

    def test_start_stop_py(self):
        # start an app
        self.put('/apps/', json.dumps([
                {
                    'app': "app", 'ver': "ver", "cfg-ver": "0",
                    'sources': ['http://localhost:11110/'],
                    'files': {"prog.py": "prog.py"},
                    "executable": {
                        'executable-type': 'py',
                        'version': '2.7',
                        'main': 'prog.py',
                        'args': []
                    }
                }
            ]))
        sleep(1)
        # stop the app
        self.put('/apps/', json.dumps([]))
        # if it was not stopped, then
        sleep(5) # 5s to run prog
        sleep(1) # 1s to let slave notice
        sleep(3) # and restart
        with open(op.join(slave.app_root('app', 'ver', self.root_dir), 'prog.out')) as f:
            self.assertEqual(f.read(), '1')

    def test_prepare_clj(self):
        # start an app
        self.put('/apps/', json.dumps([
                {
                    'app': "app", 'ver': "ver", "cfg-ver": "0",
                    'sources': ['http://localhost:11110/'],
                    'files': {"prog.py": "prog.py"},
                    'prepare': {
                        'executable-type': 'clj',
                        'class-paths': [],
                        'main': 'prepare',
                        'args': ['staging'],
                        'script': '''\
(ns prepare)
(defn -main [& args]
    (spit "prepare.out" (pr-str (vec args)) :append true)
)
'''},
                    "executable": {
                        'executable-type': 'py',
                        'version': '2.7',
                        'main': 'prog.py',
                        'args': []
                    }
                }
            ]))
        sleep(3) # it takes ~2s to start jvm up
        # stop the app
        self.put('/apps/', json.dumps([]))
        with open(op.join(slave.app_root('app', 'ver', self.root_dir), 'prepare.out')) as f:
            self.assertEqual(f.read(), '["staging"]')

    def test_prepare_py(self):
        # start an app
        self.put('/apps/', json.dumps([
                {
                    'app': "app", 'ver': "ver", "cfg-ver": "0",
                    'sources': ['http://localhost:11110/'],
                    'files': {"prog.py": "prog.py"},
                    'prepare': {
                        'executable-type': 'py',
                        'version': '2.7',
                        'main': 'prepare.py',
                        'args': ['staging'],
                        'script': '''\
import sys
with open('prepare.out', 'w') as f:
    f.write(str(sys.argv[1:]))
'''},
                    "executable": {
                        'executable-type': 'py',
                        'version': '2.7',
                        'main': 'prog.py',
                        'args': []
                    }
                }
            ]))
        sleep(1)
        # stop the app
        self.put('/apps/', json.dumps([]))
        with open(op.join(slave.app_root('app', 'ver', self.root_dir), 'prepare.out')) as f:
            self.assertEqual(f.read(), "['staging']")

    def test_failover(self):
        # start an app
        self.put('/apps/', json.dumps([
                {
                    'app': "app", 'ver': "ver", "cfg-ver": "0",
                    'sources': ['http://localhost:11110/'],
                    'files': {"prog.py": "prog.py"},
                    "executable": {
                        'executable-type': 'py',
                        'version': '2.7',
                        'main': 'prog.py',
                        'args': []
                    }
                }
            ]))
        sleep(1)
        self.slave.kill()
        self.slave.wait()
        self.slave = psutil.Popen(['python2.7', '-B', 'slave.py'],
            cwd=self.root_dir)
        sleep(5) # 5s to run prog
        sleep(1) # 1s to let slave notice
        sleep(3) # and restart
        # stop the app
        self.put('/apps/', json.dumps([]))
        sleep(1)
        with open(op.join(slave.app_root('app', 'ver', self.root_dir), 'prog.out')) as f:
            self.assertEqual(f.read(), '101')

    def test_restart(self):
        # start an app
        self.put('/apps/', json.dumps([
                {
                    'app': "app", 'ver': "ver", "cfg-ver": "0",
                    'sources': ['http://localhost:11110/'],
                    'files': {"prog.py": "prog.py"},
                    "executable": {
                        'executable-type': 'py',
                        'version': '2.7',
                        'main': 'prog.py',
                        'args': []
                    }
                }
            ]))
        sleep(1)
        sleep(5) # 5s to run prog
        sleep(1) # 1s to let slave notice
        # stop the app
        self.put('/apps/', json.dumps([]))
        sleep(1)
        with open(op.join(slave.app_root('app', 'ver', self.root_dir), 'prog.out')) as f:
            self.assertEqual(f.read(), '101')

if __name__ == '__main__':
    unittest.main()
