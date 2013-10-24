#!/usr/bin/python

import zmq
import sys
import urllib
import urllib2
import optparse
from multiprocessing import Process

AGENTS = ["MSIE", "Chrome", "Firefox", "Safari", "Opera"]


# The "worker" functions listen on a zeromq PULL connection for "work"
# (URLs to be warmed) from the ventilator, warm those urls for all agents and
# servers, and send the results down another zeromq PUSH connection to the
# results manager.
def worker(wrk_num, result_server, ventilator_server, port):
    # Initialize a zeromq context
    context = zmq.Context()

    # Set up a channel to receive work from the ventilator
    work_receiver = context.socket(zmq.PULL)
    work_receiver.connect("tcp://%s:%d" % (ventilator_server, port))

    # Set up a channel to send result of work to the results reporter
    results_sender = context.socket(zmq.PUSH)
    results_sender.connect("tcp://%s:5558" % (result_server))

    # Set up a channel to receive control messages over
    control_receiver = context.socket(zmq.SUB)
    control_receiver.connect("tcp://%s:5559" % (result_server))
    control_receiver.setsockopt(zmq.SUBSCRIBE, "")

    # Set up a poller to multiplex the work receiver and control receiver channels
    poller = zmq.Poller()
    poller.register(work_receiver, zmq.POLLIN)
    poller.register(control_receiver, zmq.POLLIN)

    # Loop and accept messages from both channels, acting accordingly
    while True:
        socks = dict(poller.poll())

        # If the message came from work_receiver channel, visit the url for
        # each agent, and send the answer to the results reporter
        if socks.get(work_receiver) == zmq.POLLIN:
            work_message = work_receiver.recv_json()
            for user_agent in AGENTS:
                data = urllib.urlencode({})
                headers = {'User-Agent': user_agent}
                req = urllib2.Request(work_message['url'], data, headers)
                try:
                    #response = urllib2.urlopen(req, timeout=10)
                    response = urllib2.urlopen(req)
                    answer_message = {'worker': wrk_num, 'result': response.code, 'host': port, 'url': work_message['url']}  # Think about adding the URL here
                except urllib2.URLError as e:
                    answer_message = {'worker': wrk_num, 'result': e.message, 'host': port, 'url': work_message['url']}
                except urllib2.HTTPError as he:
                    answer_message = {'worker': wrk_num, 'result': he.getcode, 'host': port, 'url': work_message['url']}
                results_sender.send_json(answer_message)

        # If the message came over the control channel, shut down the worker.
        if socks.get(control_receiver) == zmq.POLLIN:
            control_message = control_receiver.recv()
            if control_message == "FINISHED":
                print("Worker %i received FINSHED, quitting!" % wrk_num)
                break


if __name__ == "__main__":
    parser = optparse.OptionParser('usage %prog [-w WORKERS] -r RESULT_SERVER -v VENTILATOR_SERVER [-p PORT]', add_help_option=True)
    parser.add_option('-w', '--workers', action="store", type="int", dest="workers", default=30, help="Number of workers to spin up.")
    parser.add_option('-r', '--result-server', action="store", type="string", dest="result_server", help="Enter the FQDN or IP of the result_server.")
    parser.add_option('-v', '--ventilator-server', action="store", type="string", dest="ventilator_server", help="Enter the FQDN or IP of the ventilator_server.")
    parser.add_option('-p', '--port', action="store", type="int", dest="port", default=6557, help="Port for ventilator push queue.")
    (options, args) = parser.parse_args()

    # Create a pool of workers to distribute work to
    workers = options.workers
    result_server = options.result_server
    ventilator_server = options.ventilator_server
    port = options.port

    if not(result_server and ventilator_server):
        print(parser.usage)
        sys.exit(1)

    worker_pool = range(workers)
    for wrk_num in range(len(worker_pool)):
        Process(target=worker, args=(wrk_num, result_server, ventilator_server, port)).start()
