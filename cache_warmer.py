#!/usr/bin/python

from fabric.api import env, execute, parallel, put, run, roles
from fabric.contrib import files
#from fabric.contrib.console import confirm

sitemapurl = "http://<some url>/sitemap.xml"
sitemapfile = "/opt/sitemap.xml"
ventilator = "<some ip>"
v_port = 6557
result_manager = "<some ip>"
workers = {"<some ip>": v_port, "<some ip>": (v_port + 1)}
proc_per_worker = 8

env.roledefs = {
    'result_manager': ["<some ip>"],
    'ventilator': ["<some ip>"],
    'worker': ["<some ip>", "<some ip>"]
    # 'worker': workers.keys()
}
env.user = 'root'
env.key_filename = '/home/<some user>/.ssh/id_rsa'


@roles('ventilator', 'result_manager', 'worker')
@parallel
def installDeps():
    run("yum -y install python-lxml python-zmq")


@roles('worker')
@parallel
def startWorkers():
    if not(files.exists("/opt/cache_worker.py")):
        put("cache_worker.py", "/opt/cache_worker.py", mirror_local_mode=True)
    if not(files.exists("/opt/cache_worker.sh")):
        cmd = "/opt/cache_worker.py -w %d -r %s -v %s -p %d" % (proc_per_worker, result_manager, ventilator, workers[env.host])
        files.upload_template("detach.sh", "/opt/cache_worker.sh", {'CMD': cmd, 'LOG': '/dev/null'}, mirror_local_mode=True)
    run("/opt/cache_worker.sh", pty=False)


@roles('result_manager')
@parallel
def startResultManager():
    if not(files.exists("/opt/cache_result_manager.py")):
        put("cache_result_manager.py", "/opt/cache_result_manager.py", mirror_local_mode=True)
    if not(files.exists("/opt/cache_result_manager.sh")):
        cmd = "/opt/cache_result_manager.py -r %s -v %s" % (result_manager, ventilator)
        files.upload_template("detach.sh", "/opt/cache_result_manager.sh", {'CMD': cmd, 'LOG': '/tmp/ventilator'}, mirror_local_mode=True)
    run("/opt/cache_result_manager.sh", pty=False)


@roles('ventilator')
@parallel
def startVentilator():
    if not(files.exists("/opt/cache_ventilator.py")):
        put("cache_ventilator.py", "/opt/cache_ventilator.py", mirror_local_mode=True)
    if not(files.exists("/opt/cache_ventilator.sh")):
        cmd = "/opt/cache_ventilator.py -f %s -v %s -p %d -n %d" % (sitemapfile, ventilator, v_port, len(workers))
        files.upload_template("detach.sh", "/opt/cache_ventilator.sh", {'CMD': cmd, 'LOG': '/dev/null'}, mirror_local_mode=True)
    run("/opt/cache_ventilator.sh", pty=False)


@roles('ventilator')
@parallel
def getSiteMap():
    run("wget --quiet %s --no-cache --output-document %s" % (sitemapurl, sitemapfile))


@roles('result_manager')
@parallel
def cleanResultManager():
    run("rm /opt/{cache_result_manager.py,cache_result_manager.sh}")


@roles('ventilator')
@parallel
def cleanVentilator():
    run("rm /opt/{cache_ventilator.py,cache_ventilator.sh,} %s" % (sitemapfile))


@roles('worker')
@parallel
def cleanWorker():
    run("rm /opt/{cache_worker.py,cache_worker.pyc,cache_worker.sh}")


def clean():
    execute(cleanWorker)
    execute(cleanVentilator)
    execute(cleanResultManager)


def warmCache():
    # Install necessary python libraries
    execute(installDeps)
    # First get the latest sitemap file on the ventilator
    execute(getSiteMap)
    # Next start the services in proper order
    execute(startWorkers)
    execute(startResultManager)
    execute(startVentilator)
