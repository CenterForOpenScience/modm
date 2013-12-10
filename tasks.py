# -*- coding: utf-8 -*-
import os
from invoke import task, run

docs_dir = 'docs'
build_dir = os.path.join(docs_dir, '_build')

@task
def mongo(daemon=False):
    '''Run the mongod process.
    '''
    port = os.environ.get('MONGO_PORT', 20771)
    cmd = "mongod --port {0}".format(port)
    if daemon:
        cmd += " --fork"
    run(cmd)


@task
def redis():
    port = os.environ.get("REDIS_PORT", 6379)
    cmd = "redis-server --port {0}".format(port)
    run(cmd)


@task
def test(coverage=False, browse=False):
    command = "nosetests"
    if coverage:
        command += " --with-coverage --cover-html"
    run(command, pty=True)
    if coverage and browse:
        run("open cover/index.html")

@task
def clean():
    run("rm -rf build")
    run("rm -rf dist")
    run("rm -rf modularodm.egg-info")
    clean_docs()
    print("Cleaned up.")

@task
def clean_docs():
    run("rm -rf %s" % build_dir)

@task
def browse_docs():
    run("open %s" % os.path.join(build_dir, 'index.html'))

@task
def docs(clean=False, browse=False):
    if clean:
        clean_docs()
    run("sphinx-build %s %s" % (docs_dir, build_dir), pty=True)
    if browse:
        browse_docs()

@task
def readme():
    run("rst2html.py README.rst > README.html", pty=True)
    run("open README.html")
