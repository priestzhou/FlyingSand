import json
from itertools import *
import os
import os.path as op
import shutil
from time import sleep
from threading import Thread, Lock
from Queue import Queue, Empty
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from logging import debug, info, warning, error
import logging
from httplib import HTTPConnection
from urlparse import urlparse, urlunparse
import hashlib
import psutil
from subprocess import STDOUT

logging.basicConfig(filename='slave.log', level = logging.INFO,
    format='[%(asctime)s] %(levelname)s [%(message)s]')

def files(opt):
    hosts = opt["sources"]
    app = opt["app"]
    ver = opt["ver"]
    for url, dst in zip(cycle(hosts), sorted(opt["files"].keys())):
        src = opt["files"][dst]
        src = op.join(ver, src)
        yield [url+src, op.join('cache', src), op.join('apps', app, ver, dst)]

def sha1(f):
    digest = hashlib.sha1()
    with open(f, 'rb') as f:
        x = f.read(512 * 1024)
        while x:
            digest.update(x)
            x = f.read(512 * 1024)
    return digest.hexdigest()

def prepare_dirs(d):
    if not op.exists(d):
        os.makedirs(d)

def app_root(app, ver, cwd=None):
    if cwd is None:
        cwd = os.getcwd()
    app_rt = op.join(cwd, 'apps', app, ver)
    return app_rt

def download(fs):
    for url, cache, dst in fs:
        prepare_dirs(op.dirname(cache))
        prepare_dirs(op.dirname(dst))
        if not op.exists(cache):
            url = urlparse(url)
            host = url.hostname
            port = url.port
            url = urlunparse(['', '', url.path, url.params, url.query, ''])
            conn = HTTPConnection(host, port)
            try:
                conn.request('GET', url)
                resp = conn.getresponse()
                if resp.status != 200:
                    error("error while fetching files: %d", resp.status)
                    raise Exception("fetch file %s: %d" % (cache, resp.status))
                else:
                    with open(cache, 'wb') as f:
                        shutil.copyfileobj(resp, f)
                    shutil.copy(cache, dst)
            finally:
                conn.close()
        else:
            digest = sha1(cache)
            url = urlparse(url)
            host = url.hostname
            port = url.port
            url = urlunparse(['', '', url.path, url.params, url.query, ''])
            conn = HTTPConnection(host, port)
            try:
                conn.request('GET', url, '', {"If-None-Match": digest})
                resp = conn.getresponse()
                if resp.status == 200:
                    info('cache missing: %s', cache)
                    with open(cache, 'wb') as f:
                        shutil.copyfileobj(resp, f)
                    shutil.copy(cache, dst)
                elif resp.status == 304:
                    info('cache hit: %s', cache)
                    shutil.copy(cache, dst)
                else:
                    error("error while fetching files: %d", resp.status)
                    raise Exception("fetch file %s: %d" % (cache, resp.status))
            finally:
                conn.close()

def execute(app, ver, opt):
    cwd = os.getcwd()
    app_rt = app_root(app, ver)
    if opt["executable-type"] == 'clj':
        cmd = ['java', '-cp',
            ':'.join([op.join(cwd, 'clojure-1.5.1.jar'), app_rt]
                + [op.join(app_rt, x) for x in opt["class-paths"]]),
            'clojure.main', '-m', opt['main']
        ] + opt['args']
        info('start app %s/%s: %s', app, ver, cmd)
    elif opt["executable-type"] == 'py':
        cmd = ['python' + opt['version'], opt['main']] + opt['args']
        info('start app %s/%s: %s', app, ver, cmd)
    else:
        assert False, "unknown executable-type"
    cout = open(op.join(app_rt, 'std.out'), 'w')
    p = psutil.Popen(cmd, cwd=app_rt, stdout=cout, stderr=STDOUT, close_fds=True)
    return p

def prepare(opt):
    app = opt["app"]
    ver = opt["ver"]
    prepare = opt.get("prepare", None)
    if prepare:
        rt = app_root(app, ver)
        prepare_dirs(rt)
        if prepare['executable-type'] == 'clj':
            with open(op.join(rt, 'prepare.clj'), 'w') as f:
                f.write(prepare["script"])
        elif prepare['executable-type'] == 'py':
            with open(op.join(rt, 'prepare.py'), 'w') as f:
                f.write(prepare["script"])
        p = execute(app, ver, prepare)
        exitcode = p.wait()
        if exitcode != 0:
            error('fail to prepare')
            raise Exception('fail to prepare')
    info('%s/%s prepared', app, ver)

workitems = Queue()
running_apps_lock = Lock()
running_apps = {}

def start(opt):
    download(files(opt))
    prepare(opt)
    app = opt["app"]
    ver = opt["ver"]
    with open(op.join(app_root(app, ver), 'cfg.ver'), 'w') as f:
        f.write(opt["cfg-ver"])
    opt["proc"] = execute(app, ver, opt["executable"])

def stop(opt):
    p = opt.get("proc", None)
    if p:
        opt["proc"].kill()
        opt["proc"].wait()
        del opt["proc"]
    info('%s/%s stoped', opt["app"], opt["ver"])

def update(item):
    global running_apps_lock
    global running_apps
    try:
        info("accept a new list of apps: %s", item)
        dejson = json.loads(item)
        with open('apps.cfg', 'w') as f:
            f.write(item)
            f.write('\n')
        new_apps = {}
        for x in dejson:
            app = x["app"]
            new_apps[app] = x
        with running_apps_lock:
            for app, opt in running_apps.items():
                if app not in new_apps:
                    stop(opt)
                elif opt["ver"] != new_apps[app]["ver"]:
                    stop(opt)
            for app, opt in new_apps.items():
                if app not in running_apps:
                    start(opt)
                elif opt["cfg-ver"] != running_apps[app]["cfg-ver"]:
                    stop(running_apps[app])
                    start(opt)
                else:
                    opt['proc'] = running_apps[app]['proc']
            running_apps = new_apps
    except ValueError:
        error("fail to dejsonize: %s", item)

def patrol():
    info('patrol')
    with running_apps_lock:
        for app, opt in running_apps.items():
            p = opt.get("proc", None)
            if p and p.is_running():
                try:
                    exitcode = p.wait(timeout=0.1)
                    warning('restart %s/%s', app, opt['ver'])
                    start(opt)
                except psutil.TimeoutExpired:
                    pass
            else:
                warning('restart %s/%s', app, opt['ver'])
                start(opt)

def worker():
    global workitems
    while True:
        try:
            item = workitems.get(True, 1)
            update(item)
        except Empty:
            patrol()

class MyHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/ruok':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write('imok')

    def do_PUT(self):
        global workitems
        if self.path == '/apps/':
            length = int(self.headers['Content-Length'])
            workitems.put(self.rfile.read(length))
            self.send_response(200)

def fetch_app_ver(p):
    x = p
    ver = op.basename(x)
    x = op.dirname(x)
    app = op.basename(x)
    x = op.dirname(x)
    if op.join(os.getcwd(), 'apps') != x:
        error('unknown cwd in process %s', p)
        raise Exception('unknown path')
    return app, ver

def failover():
    error('start failover')
    init_pid = 1
    with running_apps_lock:
        with open('apps.cfg') as f:
            items = json.load(f)
        for x in items:
            running_apps[x['app']] = x
        for p in psutil.process_iter():
            if p.name in ['python2.7', 'python3.3', 'java'] and p.ppid == init_pid:
                try:
                    app, ver = fetch_app_ver(p.getcwd())
                    if app not in running_apps or ver != running_apps[app]['ver']:
                        warning('unknown process for %s/%s. killing...', app, ver)
                        p.kill()
                        p.wait()
                    else:
                        info('take care %s/%s again', app, ver)
                        running_apps[app]['proc'] = p
                except:
                    pass
    info('finish failover')

def read_cfg():
    with open('slave.cfg') as f:
        return json.load(f)

if __name__ == '__main__':
    failover()
    cfg = read_cfg()
    host = cfg["host"]
    port = cfg["port"]
    t = Thread(target=worker)
    t.start()
    httpd = HTTPServer((host, port), MyHTTPRequestHandler)
    httpd.serve_forever()

