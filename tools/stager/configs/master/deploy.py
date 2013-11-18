#!/usr/bin/python3m

import os
import os.path
import shutil

files = {
    '.': [
        'build/tools/stager/master.jar',
        'tools/stager/configs/master/start.sh',
        'tools/stager/configs/log4j.properties',
    ],
    'publics': [
        'tools/stager/configs/master/index.html', 'front/resources/',
    ],
    'publics/js': [
        'build/tools/stager/master_front.js',
    ],
    'slave': [
        'build/tools/stager/slave/slave.py'
    ]
}

def prepare_dir(d):
    if not os.path.exists(d):
        os.makedirs(d)

def deploy(files, root):
    for dst, srcs in files.items():
        dst = os.path.join(root, dst)
        prepare_dir(dst)
        for src in srcs:
            assert os.path.exists(src)
            if os.path.isfile(src):
                shutil.copy(src, dst)
                print(src, '->', dst)
            else:
                for d, _, fs in os.walk(src):
                    t = os.path.join(dst, os.path.relpath(d, src))
                    prepare_dir(t)
                    for f in fs:
                        f = os.path.join(d, f)
                        shutil.copy(f, t)
                        print(f, '->', t)

if __name__ == '__main__':
    home = os.environ['HOME']
    root = os.path.join(home, 'stager')
    deploy(files, root)
