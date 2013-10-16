import os
import os.path
import shutil
import hashlib
import subprocess
import re

env = Environment()

def emptyDir(env, dir):
    dir = env.Dir(dir)
    print 'empty %s/' % dir.path
    dir = dir.abspath
    if os.path.exists(dir):
        shutil.rmtree(dir)
    os.makedirs(dir)

env.AddMethod(emptyDir)

def walkDir(env, dir):
    d = dir.abspath
    for rt, dirs, files in os.walk(d):
        if '.git' in dirs:
            dirs.remove('.git')
        for f in files:
            f = os.path.join(rt, f)
            yield dir.File(os.path.relpath(f, d))

env.AddMethod(walkDir)

env['BUILD_DIR'] = env.Dir('build')

def scanModules(env):
    mods = [x for x in os.listdir('.') if os.path.isdir(x)]
    mods = [x for x in mods if os.path.exists(os.path.join(x, 'SConscript'))]
    mods = [Dir(x) for x in mods]
    mods = [x for x in mods if x != env['BUILD_DIR']]
    return mods

def getBuildInfo_OneModule(mod):
    if not os.path.isdir(env['BUILD_DIR'].abspath):
        os.makedirs(env['BUILD_DIR'].abspath)
    out = os.path.abspath(os.path.join('build', hashlib.md5(mod.abspath).hexdigest()))
    try:
        with open(out, 'w') as f:
            subprocess.check_call(['git', 'show', '--format=%H'], 
                stdout=f, stderr=subprocess.STDOUT,
                cwd=mod.abspath)
        with open(out) as f:
            for x in f:
                return mod.path, x.strip()
    except:
        return mod.path, 'unknown'

def getBuildInfo(mods):
    return dict(getBuildInfo_OneModule(x) for x in mods)

env['BUILD_INFO'] = getBuildInfo(scanModules(env))

def cleanLinks(builddir):
    for rt, dirs, files in os.walk(builddir):
        for f in files:
            f = os.path.join(rt, f)
            if os.path.islink(f):
                os.remove(f)

def cleanTmpDirs(builddir):
    pat = re.compile('^[0-9a-f]{32}$')
    for rt, dirs, _ in os.walk(builddir):
        for d in dirs:
            if pat.match(d):
                dirs.remove(d)
                shutil.rmtree(os.path.join(rt, d))

def copyTree(srcDir):
    assert srcDir.isdir()

    def walk(srcDir):
        for rt, dirs, files in os.walk(srcDir):
            if '.git' in dirs:
                dirs.remove('.git')
            for f in files:
                yield os.path.join(rt, f)

    srcs = [x for x in walk(srcDir.abspath)]
    dsts = [os.path.join(env['BUILD_DIR'].abspath, os.path.relpath(x)) for x in srcs]

    dirs = set(os.path.dirname(x) for x in dsts)
    for x in dirs:
        if os.path.exists(x) and not os.path.isdir(x):
            shutil.rmtree(x)
    for x in dirs:
        if not os.path.exists(x):
            os.makedirs(x)
    for src, dst in zip(srcs, dsts):
        os.symlink(src, dst)

def prepareBuildDir(env):
    mods = scanModules(env)
    if not os.path.isdir(env['BUILD_DIR'].abspath):
        os.makedirs(env['BUILD_DIR'].abspath)
    cleanLinks(env['BUILD_DIR'].abspath)
    cleanTmpDirs(env['BUILD_DIR'].abspath)
    for src in mods:
        print 'copy %s into %s/' % (src, env['BUILD_DIR'].path)
        copyTree(src)
    with open(env['BUILD_DIR'].File('SConscript').abspath, 'w') as f:
        f.write("""\
Import('env')
env.SConscript(dirs=[%s], exports='env')
""" % (', '.join("'%s'" % x.path for x in mods)))

prepareBuildDir(env)

env['EXTLIB'] = env['BUILD_DIR'].Dir('common/extlib')
env['CLOJURE'] = env['EXTLIB'].File('clojure-1.5.1.jar')
env['SCALA'] = [env['EXTLIB'].File(x) for x in [
        'scalacheck.jar', 
        'scala-compiler.jar',
        'scala-dbc.jar', 
        'scala-library.jar',
        'scala-partest.jar',
        'scalap.jar',
        'scala-swing.jar',
        'akka-actors.jar',
        'diffutils.jar',
        'scala-actors.jar',
        'scala-actors-migration.jar',
        'scala-reflect.jar',
        'typesafe-config.jar',
    ]]
env['CLOJURE_SCRIPT'] = [env['EXTLIB'].File(x) for x in [
        'clojure-1.5.1.jar',
        'cljs-1847.jar',
        'tools.reader-0.7.5.jar',
        'data.json-0.2.2.jar',
        'google-closure-compiler.jar',
        'google-closure-library-0.0-20130212-95c19e7f0f5f.jar',
        'google-closure-library-third-party-0.0-20130212-95c19e7f0f5f.jar',
        'dommy-0.1.2-SNAPSHOT.jar',
    ]]

env['MANIFEST'] = {'Manifest-Version': '1.0',
        'Built-By': 'FlyingSand.com',
        'Created-By': 'scons 2.3.0',
    }
env['MANIFEST'].update(env['BUILD_INFO'])

libDependencies = {
    env.File('$EXTLIB/clj-time-0.5.2.jar'): [
        env.File('$EXTLIB/joda-time-2.2.jar')
    ],
    env.File('$EXTLIB/ring-1.1.8.jar'): [
        env.File('$EXTLIB/commons-codec-1.8.jar'),
        env.File('$EXTLIB/clj-time-0.5.2.jar'),
        env.File('$EXTLIB/joda-time-2.2.jar'),
        env.File('$EXTLIB/commons-fileupload-1.3.jar'),
        env.File('$EXTLIB/javax.servlet-api.jar'),
        env.File('$EXTLIB/commons-io-2.4.jar'),
        env.File('$EXTLIB/ns-tracker-0.2.1.jar'),
        env.File('$EXTLIB/hiccup-1.0.3.jar'),
        env.File('$EXTLIB/clj-stacktrace-0.2.6.jar'),
    ],
    env.File('$EXTLIB/ns-tracker-0.2.1.jar'): [
        env.File('$EXTLIB/tools.namespace-0.2.3.jar'),
    ],
    env.File('$EXTLIB/ring-jetty-adapter-1.1.8.jar'): [
        env.File('$EXTLIB/ring-1.1.8.jar'),
        env.File('$EXTLIB/jetty-server-7.6.11.v20130520.jar'),
        env.File('$EXTLIB/jetty-util-7.6.11.v20130520.jar'),
        env.File('$EXTLIB/jetty-io-7.6.11.v20130520.jar'),
        env.File('$EXTLIB/jetty-http-7.6.11.v20130520.jar'),
        env.File('$EXTLIB/jetty-continuation-7.6.11.v20130520.jar'),
    ],
    env.File('$EXTLIB/compojure-1.1.5.jar'): [
        env.File('$EXTLIB/ring-1.1.8.jar'),
        env.File('$EXTLIB/clout-1.1.0.jar'),
        env.File('$EXTLIB/core.incubator-0.1.3.jar'),
        env.File('$EXTLIB/tools.macro-0.1.2.jar'),
    ],
    env.File('$BUILD_DIR/testing.jar'): [
        env.File('$BUILD_DIR/argparser.jar'),
    ],
    env.File('$EXTLIB/zookeeper-client-3.4.5.jar'): [
        env.File('$EXTLIB/log4j-1.2.15.jar'),
        env.File('$EXTLIB/slf4j-api-1.7.5.jar'),
        env.File('$EXTLIB/slf4j-log4j12-1.7.5.jar'),
    ],
    env.File('$EXTLIB/kafka-client-0.8.0-beta1.jar'): [
        env.File('$EXTLIB/zookeeper-client-3.4.5.jar'),
        env.File('$EXTLIB/slf4j-api-1.7.5.jar'),
        env.File('$EXTLIB/slf4j-log4j12-1.7.5.jar'),
        env.File('$EXTLIB/log4j-1.2.15.jar'),
        env.File('$EXTLIB/scala-library.jar'),
        env.File('$EXTLIB/zkclient-0.3.jar'),
        env.File('$EXTLIB/jopt-simple-3.2.jar'),
        env.File('$EXTLIB/snappy-java-1.0.4.1.jar'),
        env.File('$EXTLIB/metrics-core-2.2.0.jar'),
        env.File('$EXTLIB/metrics-annotation-2.2.0.jar'),
    ],
    env.File('$EXTLIB/spark-core_2.9.3-0.7.3.jar'): [
        env.File('$EXTLIB/lib-for-spark/netty-3.5.3.Final.jar'),
        env.File('$EXTLIB/lib-for-spark/slf4j-log4j12-1.7.2.jar'),
        env.File('$EXTLIB/lib-for-spark/slf4j-api-1.7.2.jar'),
        env.File('$EXTLIB/lib-for-spark/jetty-server-7.6.8.v20121106.jar'),
        env.File('$EXTLIB/lib-for-spark/jetty-util-7.6.8.v20121106.jar'),
        env.File('$EXTLIB/lib-for-spark/jetty-http-7.6.8.v20121106.jar'),
        env.File('$EXTLIB/lib-for-spark/jetty-io-7.6.8.v20121106.jar'),        
        env.File('$EXTLIB/serializable-fn-0.0.3.jar'),
        env.File('$EXTLIB/lib-for-spark/jetty-continuation-7.6.8.v20121106.jar'),        
        env.File('$EXTLIB/lib-for-spark/scala-2.9.3-library.jar'),
        env.File('$EXTLIB/lib-for-spark/mesos-0.9.0-incubating.jar'),
        env.File('$EXTLIB/lib-for-spark/hadoop-core-2.0.0-mr1-cdh4.3.0.jar'),
        env.File('$EXTLIB/lib-for-spark/guava-11.0.1.jar'),
        env.File('$EXTLIB/lib-for-spark/akka-actor-2.0.3.jar'),
        env.File('$EXTLIB/lib-for-spark/akka-remote-2.0.3.jar'),
        env.File('$EXTLIB/lib-for-spark/config-0.3.1.jar'),
        env.File('$EXTLIB/lib-for-spark/log4j-1.2.17.jar'),
        env.File('$EXTLIB/lib-for-spark/akka-slf4j-2.0.3.jar'),
        env.File('$EXTLIB/lib-for-spark/protobuf-java-2.4.1.jar'),
        env.File('$EXTLIB/lib-for-spark/compress-lzf-0.8.4.jar'),
        env.File('$EXTLIB/lib-for-spark/fastutil-6.4.4.jar'),
        env.File('$EXTLIB/lib-for-spark/servlet-api-2.5-6.1.14.jar'),
        env.File('$EXTLIB/lib-for-spark/spray-server-1.0-M2.1.jar'),
        env.File('$EXTLIB/lib-for-spark/spray-base-1.0-M2.1.jar'),
        env.File('$EXTLIB/lib-for-spark/spray-util-1.0-M2.1.jar'),
        env.File('$EXTLIB/lib-for-spark/mimepull-1.6.jar'),
        env.File('$EXTLIB/lib-for-spark/spray-io-1.0-M2.1.jar'),
        env.File('$EXTLIB/lib-for-spark/spray-can-1.0-M2.1.jar'),
        env.File('$EXTLIB/lib-for-spark/parboiled-scala-1.0.2.jar'),
        env.File('$EXTLIB/lib-for-spark/parboiled-core-1.0.2.jar'),
        env.File('$EXTLIB/lib-for-spark/commons-logging-1.1.1.jar'),
        env.File('$EXTLIB/lib-for-spark/commons-configuration-1.6.jar'),
        env.File('$EXTLIB/lib-for-spark/commons-lang-2.6.jar'),
        env.File('$EXTLIB/lib-for-spark/asm-all-3.3.1.jar'),
        env.File('$EXTLIB/lib-for-spark/hadoop-core-2.0.0-mr1-cdh4.3.0.jar'),
        env.File('$EXTLIB/lib-for-spark/hadoop-hdfs-2.0.0-cdh4.3.0.jar'),
        env.File('$EXTLIB/lib-for-spark/hadoop-common-2.0.0-cdh4.3.0.jar'),
        env.File('$EXTLIB/lib-for-spark/hadoop-auth-2.0.0-cdh4.3.0.jar'),
        env.File('$EXTLIB/lib-for-spark/hadoop-client-2.0.0-mr1-cdh4.3.0.jar'),
        env.File('$EXTLIB/lib-for-spark/commons-cli-1.2.jar'),
    ],    
    env.File('$BUILD_DIR/kfktools.jar'): [
        env.File('$EXTLIB/kafka-client-0.8.0-beta1.jar'),
        env.File('$BUILD_DIR/utilities.jar'),
    ],
    env.File('$BUILD_DIR/zktools.jar'): [
        env.File('$BUILD_DIR/utilities.jar')
    ],
    env.File('$BUILD_DIR/logging.jar'): [
        env.File('$EXTLIB/log4j-1.2.15.jar'),
        env.File('$EXTLIB/slf4j-api-1.7.5.jar'),
        env.File('$EXTLIB/slf4j-log4j12-1.7.5.jar'),
    ],
    env.File('$EXTLIB/hadoop-hdfs-2.0.0-cdh4.3.0.jar'): [
        env.File('$EXTLIB/hadoop-common-2.0.0-cdh4.3.0.jar'),
        env.File('$EXTLIB/commons-logging-1.1.1.jar'),
        env.File('$EXTLIB/commons-lang-2.5.jar'),
        env.File('$EXTLIB/guava-11.0.2.jar'),
        env.File('$EXTLIB/commons-configuration-1.6.jar'),
        env.File('$EXTLIB/hadoop-auth-2.0.0-cdh4.3.0.jar'),
        env.File('$EXTLIB/log4j-1.2.15.jar'),
        env.File('$EXTLIB/slf4j-api-1.7.5.jar'),
        env.File('$EXTLIB/slf4j-log4j12-1.7.5.jar'),
        env.File('$EXTLIB/commons-cli-1.2.jar'),
        env.File('$EXTLIB/protobuf-java-2.4.0a.jar'),
        env.File('$EXTLIB/lib-for-hadoop/jackson-core-asl-1.8.8.jar'),
        env.File('$EXTLIB/lib-for-hadoop/jackson-mapper-asl-1.8.8.jar'),
    ],
    env.File('$EXTLIB/korma-0.3.0-RC5.jar'): [
        env.File('$EXTLIB/lib-for-shark/java.jdbc-0.2.3.jar'),
        env.File('$EXTLIB/c3p0-0.9.2.1.jar'),
        env.File('$EXTLIB/mysql-connector-java-5.1.6.jar'),
	env.File('$EXTLIB/mchange-commons-java-0.2.3.4.jar'),
    ],
}

def parseClojurePackage(src):
    pkg = src.split(os.sep)
    pkg[-1] = os.path.splitext(pkg[-1])[0]
    return '.'.join(pkg).replace('_', '-')

def compileClojure(env, workdir, root, cljs, kwargs):
    if cljs:
        print 'compile clojures in %s into %s' % (root.path, workdir.path)
        warnOnReflection = 'true' if kwargs.get('warnOnReflection', False) else 'false'
        subprocess.check_call(['java', 
            '-cp', '%s' % 
                (':'.join(
                    [root.abspath, workdir.abspath] + 
                    [x.abspath for x in kwargs['libs']]
                )), 
            '-Dclojure.compile.path=%s' % workdir.abspath,
            '-Dclojure.compile.warn-on-reflection=%s' % warnOnReflection,
            'clojure.lang.Compile',
            ] + [
                parseClojurePackage(root.rel_path(x)) for x in cljs
            ])

def compileScala(env, workdir, root, scalas, javas, kwargs):
    if scalas:
        print 'compile scalas in %s into %s' % (root.path, workdir.path)
        subprocess.check_call(['java', 
            '-cp', '%s' %
                (':'.join(
                    [root.abspath, workdir.abspath] + 
                    [x.abspath for x in kwargs['libs']]
                )),
            '-Dscala.home=%s' % env['EXTLIB'].abspath,
            '-Dscala.usejavacp=true',
            'scala.tools.nsc.Main',
            '-d', '%s' % workdir.abspath,
            '-target:jvm-1.7',
            '-deprecation',
            '-feature',
            ] + [
                root.rel_path(x) for x in (scalas | javas)
            ],
            cwd=root.abspath
            )

def compileJava(env, workdir, root, javas, kwargs):
    if javas:
        print 'compile javas in %s into %s' % (root.path, workdir.path)
        subprocess.check_call(['javac', '-g', '-d', workdir.abspath, 
                '-cp', '%s' % 
                    (':'.join(
                        [root.abspath, workdir.abspath] + 
                        [x.abspath for x in kwargs['libs']]
                    )),
                '-Xlint:unchecked',
                '-sourcepath', root.abspath,
            ] + [
                x.abspath for x in javas
            ])

def copyDirectly(workdir, root, fs):
    for f in fs:
        dst = workdir.File(root.rel_path(f))
        print 'copy %s to %s' % (f.path, dst.path)
        if not os.path.exists(os.path.dirname(dst.abspath)):
            os.makedirs(os.path.dirname(dst.abspath))
        shutil.copy(f.abspath, dst.abspath)

def compile(env, workdir, srcDir, kwargs):
    assert 'attempt' in kwargs
    assert kwargs['attempt'] > 0
    root = env.Dir(os.path.dirname(srcDir.path))
    fs = [x for x in env.walkDir(srcDir)]
    cljs = set(x for x in fs if x.path.endswith('.clj'))
    javas = set(x for x in fs if x.path.endswith('.java'))
    scalas = set(x for x in fs if x.path.endswith('.scala'))
    others = [x for x in fs if not (x in cljs or x in javas or x in scalas)]
    copyDirectly(workdir, root, others)
    broken = True
    for _ in range(kwargs['attempt'] - 1):
        if not broken:
            break
        broken = False
        try:
            compileScala(env, workdir, root, scalas, javas, kwargs)
        except:
            broken = True
        try:
            compileJava(env, workdir, root, javas, kwargs)
        except:
            broken = True
        try:
            compileClojure(env, workdir, root, cljs, kwargs)
        except:
            broken = True
    if broken:
        compileScala(env, workdir, root, scalas, javas, kwargs)
        compileJava(env, workdir, root, javas, kwargs)
        compileClojure(env, workdir, root, cljs, kwargs)

def unjar(env, workdir, kwargs):
    for x in kwargs['libs']:
        print 'unjar %s into %s' % (x.path, workdir.path)
        subprocess.check_call(['jar', 'xf', x.abspath], cwd=workdir.abspath)

def writeManifest(env, workdir, kwargs):
    manifest = kwargs.get('manifest', {})
    manifest.update(env['MANIFEST'])
    manifestFile = workdir.File('manifest').abspath
    with open(manifestFile, 'w') as f:
        for k, v in manifest.items():
            f.write('%s: %s\n' % (k, v))
    return manifestFile

def installFilesToJar(env, workdir, kwargs):
    installs = kwargs['install']
    for src, dst in installs.items():
        assert dst.startswith('@')
        dst = workdir.abspath + dst[1:]
        if not os.path.exists(dst):
            os.makedirs(dst)
        assert os.path.isdir(dst)
        assert os.path.isfile(src.abspath)
        shutil.copy(src.abspath, dst)

def jar(env, dstJar, workdir, kwargs):
    print 'jar %s into %s' % (workdir.path, dstJar.path)
    manifestFile = writeManifest(env, workdir, kwargs)
    installFilesToJar(env, workdir, kwargs)
    subprocess.check_call(['jar', 'cfm', dstJar.abspath, manifestFile, 
        '-C', workdir.abspath, '.'])
    subprocess.check_call(['jar', 'i', dstJar.abspath])

def _compileAndJar(target, source, env):
    kwargs = env['kwargs']
    workdir = kwargs['workdir']
    os.makedirs(workdir.abspath)
    dstJar = target[0]
    for srcDir in source:
        compile(env, workdir, srcDir, kwargs)
        if kwargs.get('standalone', False):
            unjar(env, workdir, kwargs)
        jar(env, dstJar, workdir, kwargs)

def expandLibs(libs):
    res = set()
    q = [env.File(x) for x in libs]
    while q:
        x = q.pop()
        res.add(x)
        for y in libDependencies.get(x, []):
            if y not in res:
                q.append(y)
    res = list(res)
    res.sort(key=lambda x:x.abspath)
    return res

def compileAndJar(env, dstJar, srcDir, **kwargs):
    srcDir = [env.Dir(x) for x in env.Flatten([srcDir])]
    for x in srcDir:
        assert x.exists()
    dstJar = env.File(dstJar)

    workdir = env.Dir(hashlib.md5(dstJar.abspath).hexdigest())
    if os.path.exists(workdir.abspath):
        shutil.rmtree(workdir.abspath)
    kwargs['workdir'] = workdir

    kwargs['libs'] = expandLibs(env.Flatten([kwargs.get('libs', [])]))
    kwargs['attempt'] = kwargs.get('attempt', 1)
    kwargs['install'] = kwargs.get('install', {})

    env = env.Clone()
    env['kwargs'] = kwargs
    env.Append(BUILDERS={'_compileAndJar': Builder(
        action=_compileAndJar,
        suffix='.jar')})
    env._compileAndJar(target=dstJar, source=srcDir)
    env.Depends(dstJar, kwargs['libs'])
    for x in srcDir:
        for f in env.walkDir(x):
            env.Depends(dstJar, f)
    installs = kwargs['install'].keys()
    installs.sort(key=lambda x:x.abspath)
    for src in installs:
        env.Depends(dstJar, src)

    return dstJar

env.AddMethod(compileAndJar)

def _formatCljMap(m):
    if isinstance(m, dict):
        return '{%s}' % (', '.join(
            '%s %s' % (_formatCljMap(k), _formatCljMap(v)) for k,v in m.iteritems()))
    elif isinstance(m, list):
        return '[%s]' % (' '.join(_formatCljMap(x) for x in m))
    elif isinstance(m, str):
        return m
    elif isinstance(m, bool):
        return 'true' if m else 'false'
    elif isinstance(m, int):
        return '%d' % m
    else:
        assert False, 'unknown type'

def _compileJs(target, source, env):
    dst = target[0]
    src = source[0]
    root = env.Dir(os.path.dirname(src.path))
    with open(dst.abspath, 'w') as fout:
        subprocess.check_call(['java', 
                '-cp', '%s' % (
                    ':'.join([root.abspath] + 
                        [x.abspath for x in env['kwargs']['libs']])),
                'clojure.main',
                env['EXTLIB'].File('cljsc.clj').abspath,
                src.abspath,
                _formatCljMap(env['kwargs']['options'])
            ],
            stdout=fout
        )

def compileJs(env, dstJs, srcDir, **kwargs):
    dstJs = env.File(dstJs)
    srcDir = env.Dir(srcDir)
    options = {':optimizations': ':advanced',
        ':output-dir': 
            '"%s"' % (env.Dir(hashlib.md5(dstJs.abspath).hexdigest()).abspath)
    }
    options.update(kwargs.get('options', {}))
    libs = expandLibs(env.Flatten(env['CLOJURE_SCRIPT'] + [kwargs.get('libs', [])]))

    env = env.Clone()
    env['kwargs'] = {'options': options,
        'libs': libs,
    }
    env.Append(BUILDERS={'_compileJs': Builder(
        action=_compileJs,
        suffix='.jar')})
    env._compileJs(target=dstJs, source=srcDir)
    for f in env.walkDir(srcDir):
        if f.abspath.endswith('.cljs') or f.abspath.endswith('.clj') or f.abspath.endswith('.js'):
            env.Depends(dstJs, f)
        else:
            env.Ignore(dstJs, f)
    for f in libs:
        env.Depends(dstJs, f)

    return dstJs

env.AddMethod(compileJs)

def install(env, targets):
    targets = env.Flatten([targets])
    installed = env.Install('$BUILD_DIR', targets)
    env.Default(installed)
    return installed

env.AddMethod(install)

env.SConscriptChdir(1)
env.SConscript('$BUILD_DIR/SConscript', exports='env')
