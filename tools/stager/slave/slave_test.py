import unittest
from threading import Thread
from time import sleep
import os
import os.path as op
import shutil
import httplib
import psutil
import slave

class TestSlave(unittest.TestCase):
    def send(self, path, message):
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


if __name__ == '__main__':
    unittest.main()
