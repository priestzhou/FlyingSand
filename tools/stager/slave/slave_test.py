import unittest
from threading import Thread
from time import sleep
import os
import os.path as op
import shutil
import httplib
import json
import psutil
import slave

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

    def setUp(self):
        self.root_dir = os.tempnam()
        os.makedirs(self.root_dir)
        shutil.copy('tools/stager/slave/slave.py', self.root_dir)
        shutil.copy('common/extlib/clojure-1.5.1.jar', self.root_dir)
        with open(op.join(self.root_dir, 'slave.cfg'), 'w') as f:
            f.write('{"host": "localhost", "port": 11111}\n')
        self.slave = psutil.Popen(['python2.7', '-B', 'slave.py'],
            cwd=self.root_dir)
        sleep(1)

    def tearDown(self):
        self.slave.kill()

    def test_ruok(self):
        r = self.get('/ruok')
        self.assertEqual(r, 'imok')

    def test_start_stop(self):
        # start an app
        self.put('/apps/', json.dumps([
                {
                    'app': "a", 'ver': "x",
                    'sources': ['http://localhost:11110/'],
                    'files': {"prog.clj": "y"},
                    'prepare': {
                        'executable-type': 'java',
                        'class-paths': [],
                        'main-class': 'prepare',
                        'args': ['staging'],
                        'script': '''\
(ns prepare)
(defn -main [& args]
    (spit "%s/prepare.out" "1" :append true)
)
''' % (op.join(self.root_dir, 'a', 'x'))
                    },
                    "executable": {
                        'executable-type': 'java',
                        'class-paths': [],
                        'main-class': 'prog',
                        'args': []
                    }
                }
            ]))
        sleep(3) # it takes ~2s to start jvm up
        # stop the app
        self.put('/apps/', json.dumps([]))
        sleep(5) # 5s to run prog
        sleep(1) # 1s to let slave notice
        sleep(3) # and restart
        with open(op.join(self.root_dir, 'a', 'x', 'prog.out')) as f:
            self.assertEqual(f.read(), '1')

if __name__ == '__main__':
    unittest.main()
