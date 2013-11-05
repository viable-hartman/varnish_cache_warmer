#!/usr/bin/python

from fabric.api import env, execute, parallel, put, run, roles
from fabric.contrib import files
#from fabric.contrib.console import confirm

#from django.template import Context, Template
## Django Template Example
# f = open('templates/%s' % filename)
# f = ''.join([line for line in f])
# t = Template(f)
# c = Context({ 'servers': ips })
# rendered = t.render(c)

sitemapurl = "http://<some url/sitemap.xml"
sitemapfile = "/opt/sitemap.xml"
ventilator = "<some ip>"
rm_ports = [5558, 5559]
v_rm_port = 5560
v_port = 6557
result_manager = "<some ip>"
workers = {"<some ip>": v_port, "<some ip>": (v_port + 1)}
proc_per_worker = 8

env.roledefs = {
    'result_manager': ["<some ip>"],
    'ventilator': ["<some ip>"],
    'worker': ["<some ip>"]
    # 'worker': workers.keys()
}
env.user = 'root'
#env.user = 'viable'
env.key_filename = '/home/<some user>/.ssh/id_rsa'


@roles('ventilator', 'result_manager', 'worker')
@parallel
def installDeps():
    run("yum -y install python-lxml python-zmq")


@roles('ventilator')
@parallel
def v_iptables():
    for worker, port in workers.iteritems():
        run("iptables -s %s -I INPUT 1 -p tcp --dport %d -j ACCEPT" % (worker, port))
    run("iptables -s %s -I INPUT 1 -p tcp --dport %d -j ACCEPT" % (result_manager, v_rm_port))


@roles('result_manager')
@parallel
def rm_iptables():
    for worker in workers:
        run("iptables -s %s -I INPUT 1 -p tcp --dport %s -j ACCEPT" % (worker, ':'.join(map(str, rm_ports))))


@roles('ventilator', 'result_manager')
@parallel
def del_iptables():
    run("service iptables restart")


@roles('worker')
@parallel
def startWorkers():
    if not(files.exists("/opt/cache_worker.py")):
        put("cache_worker.py", "/opt/cache_worker.py", mirror_local_mode=True)
    if not(files.exists("/opt/cache_worker.sh")):
        idx = env.roledefs['worker'].index(env.host)
        # host = workers.keys()[idx]
        port = workers[workers.keys()[idx]]
        cmd = "/opt/cache_worker.py -w %d -r %s -v %s -p %d -c %s" % (proc_per_worker, result_manager, ventilator, port, ':'.join(map(str, rm_ports)))
        files.upload_template("detach.sh", "/opt/cache_worker.sh", {'CMD': cmd, 'LOG': '/dev/null'}, mirror_local_mode=True)
    run("/opt/cache_worker.sh", pty=False)


@roles('result_manager')
@parallel
def startResultManager():
    if not(files.exists("/opt/cache_result_manager.py")):
        put("cache_result_manager.py", "/opt/cache_result_manager.py", mirror_local_mode=True)
    if not(files.exists("/opt/cache_result_manager.sh")):
        cmd = "/opt/cache_result_manager.py -r %s -v %s -c %d -p %s" % (result_manager, ventilator, v_rm_port, ':'.join(map(str, rm_ports)))
        files.upload_template("detach.sh", "/opt/cache_result_manager.sh", {'CMD': cmd, 'LOG': '/tmp/ventilator'}, mirror_local_mode=True)
    run("/opt/cache_result_manager.sh", pty=False)


@roles('ventilator')
@parallel
def startVentilator():
    if not(files.exists("/opt/cache_ventilator.py")):
        put("cache_ventilator.py", "/opt/cache_ventilator.py", mirror_local_mode=True)
    if not(files.exists("/opt/cache_ventilator.sh")):
        cmd = "/opt/cache_ventilator.py -f %s -v %s -p %d -n %d -c %d" % (sitemapfile, ventilator, v_port, len(workers), v_rm_port)
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
    execute(v_iptables)
    execute(rm_iptables)
    # First get the latest sitemap file on the ventilator
    execute(getSiteMap)
    # Next start the services in proper order
    execute(startWorkers)
    execute(startResultManager)
    execute(startVentilator)
    print("Remember to execute del_iptables")
