#!/usr/bin/python

import time
import zmq
import sys
import optparse
# import fileinput # Was using as fast low memory line by line parser
from lxml import etree
from multiprocessing import Process

AGENTS = ["MSIE", "Chrome", "Firefox", "Safari", "Opera"]


# Fast iterative XML parsing function useful for parsing huge
# sitemap.xml files.
def fast_iter(context, func):
    elem_cnt = 0
    context.next()
    for event, elem in context:
        func(elem)
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
        elem_cnt = elem_cnt + 1
    del context
    return elem_cnt


def message(elem):
    print("%s") % (elem.text)


# The "ventilator" function parses the sitemap.xml file for urls, and
# sends those urls down a zeromq "PUSH" connection to be processed by
# listening workers, in a round robin load balanced fashion.
def ventilator(sitemap_file, ventilator_server, bport, eport, v_rm_port):
    # Initialize a zeromq context
    context = zmq.Context()

    # Set up a channel to send work
    senders = {}
    for port in range(bport, eport):
        senders[port] = context.socket(zmq.PUSH)
        senders[port].bind("tcp://%s:%d" % (ventilator_server, port))
    # ventilator_send = context.socket(zmq.PUSH)
    # ventilator_send.bind("tcp://%s:%d" % (ventilator_server, pport))

    # Set up a channel to send loc count to result_manager
    v_sender = context.socket(zmq.PUB)
    v_sender.bind("tcp://%s:%d" % (ventilator_server, v_rm_port))

    # Give everything a second to spin up and connect
    time.sleep(1)

    ## Read file line by line and send url as work messages
    #for line in fileinput.input([sitemap_file]):
    #    work_message = {'url': line}
    #    ventilator_send.send_json(work_message)

    # Parse sitemap.xml file one element at a time and send url
    # as work messages
    xmlcontext = etree.iterparse(sitemap_file, events=('end',), tag='{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
    # Iterate through each of the <loc> nodes using the fast iteration method
    loc_cnt = fast_iter(xmlcontext,
                        # For each <loc> send it as a work message
                        #message)
                        #lambda elem: ventilator_send.send_json({'url': elem.text}))
                        lambda elem: [senders[port].send_json({'url': elem.text}) for port in range(bport, eport)])

    time.sleep(1)
    # Signal result_manager record count to expect.
    loc_cnt = loc_cnt * len(AGENTS) * (eport - bport)
    print("Sending on v_sender %d") % (loc_cnt)
    v_sender.send_json({'count': loc_cnt})

    time.sleep(1)


if __name__ == "__main__":
    parser = optparse.OptionParser('usage %prog -f SITEMAP_FILE -v VENTILATO_SERVER [-p BEGINNING_PORT] [-n NUM_SERVERS]', add_help_option=True)
    parser.add_option('-f', '--sitemap-file', action="store", type="string", dest="sitemapxml", help="Path to the downloaded sitemap xml file.")
    parser.add_option('-v', '--ventilator-server', action="store", type="string", dest="ventilator_server", help="Server that queues the urls.")
    parser.add_option('-p', '--bport', action="store", type="int", dest="bport", default=6557, help="Beginning port for push queue.")
    parser.add_option('-n', '--num-servers', action="store", type="int", dest="num_servers", default=1, help="The number of worker servers that need a push queue port.")
    parser.add_option('-c', '--v-rm-port', action="store", type="int", dest="v_rm_port", default=5560, help="Result Manager communication port.")
    (options, args) = parser.parse_args()

    sitemapxml = options.sitemapxml
    ventilator_server = options.ventilator_server
    bport = options.bport
    eport = bport + options.num_servers
    v_rm_port = options.v_rm_port

    if not(sitemapxml and ventilator_server):
        print(parser.usage)
        sys.exit(1)

    # Start the ventilator!
    ventilator = Process(target=ventilator, args=(sitemapxml, ventilator_server, bport, eport, v_rm_port))
    ventilator.start()
