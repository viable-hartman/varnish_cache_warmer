#!/usr/bin/python

import sys
import zmq
import time
import signal
import optparse
from multiprocessing import Process


# The "results_manager" function receives each result from multiple workers,
# and prints those results.  When all results have been received, it signals
# the worker processes to shut down.
def result_manager(tm_between_msgs, wake_poll, result_server, ventilator_server):
    # Initialize a zeromq context
    context = zmq.Context()

    # Set up a channel to receive results
    results_receiver = context.socket(zmq.PULL)
    results_receiver.bind("tcp://%s:5558" % (result_server))

    # Set up a channel to receive ventilator messages over
    v_receiver = context.socket(zmq.SUB)
    v_receiver.connect("tcp://%s:5560" % (ventilator_server))
    v_receiver.setsockopt(zmq.SUBSCRIBE, "")

    # Set up a channel to send control commands
    control_sender = context.socket(zmq.PUB)
    control_sender.bind("tcp://%s:5559" % (result_server))

    # Set up a poller to multiplex the results_receiver and v_receiver channels
    poller = zmq.Poller()
    poller.register(results_receiver, zmq.POLLIN)
    poller.register(v_receiver, zmq.POLLIN)

    last_msg_tm = time.time()
    msg_cnt = 0
    v_message = None

    # Time check failsafe...  If we don't get a message in X seconds, we finish up.
    def wake_handler(signum, frame):
        tm_diff = time.time() - last_msg_tm
        print("Time Diff %d") % (tm_diff)
        if tm_diff >= tm_between_msgs:
            print("Result Manager timed out after %d seconds waiting for messages!") % (tm_diff)
            # Signal to all workers that we are finsihed
            control_sender.send("FINISHED")
            time.sleep(5)
            sys.exit()

    # Set the signal handler
    signal.signal(signal.SIGALRM, wake_handler)
    # Write output to log file
    #f = open('result_manager.log', 'w')
    while True:
        socks = dict(poller.poll())
        if socks.get(results_receiver) == zmq.POLLIN:
            signal.alarm(0)  # May not actually need to clear the alarm, but I do
            result_message = results_receiver.recv_json()
            msg_cnt = msg_cnt + 1
            print "Worker %s:%s answered: %s message count: %d\n    %s" % (result_message['worker'], result_message['host'], result_message['result'], msg_cnt, result_message['url'])
            #f.write("Worker %i answered: %i" % (result_message['worker'], result_message['result']))
            last_msg_tm = time.time()
            signal.alarm(wake_poll)

        # If a message came over the v_receiver channel get it.
        if socks.get(v_receiver) == zmq.POLLIN:
            signal.alarm(0)  # May not actually need to clear the alarm, but I do
            v_message = v_receiver.recv_json()
            print("Received %d count from ventilator current count is %d") % (v_message['count'], msg_cnt)
            signal.alarm(wake_poll)

        # if msg_cnt >= v_message count , break and shut down the workers.
        if v_message and msg_cnt >= v_message['count']:
            print("Result Manager received last message, quitting!")
            break

    # Signal to all workers that we are finsihed
    control_sender.send("FINISHED")
    #f.close()
    time.sleep(5)

if __name__ == "__main__":
    parser = optparse.OptionParser('usage %prog -t TIME_BETWEEN_MESSAGES -w WAKE_ALARM -r RESULT_SERVER -v VENTILATO_SERVER', add_help_option=True)
    parser.add_option('-t', '--tm-between-msgs', action="store", type="int", dest="tm_between_msgs", default=10, help="Idle time between messages before the result manager assumes its finished.")
    parser.add_option('-w', '--wake-alarm', action="store", type="int", dest="wake_poll", default=30, help="An alarm to wake the result server in case of unmatching message counts.")
    parser.add_option('-r', '--result-server', action="store", type="string", dest="result_server", help="Server where the worker results are sent.")
    parser.add_option('-v', '--ventilator-server', action="store", type="string", dest="ventilator_server", help="Server that queues the urls.")
    (options, args) = parser.parse_args()

    tm_between_msgs = options.tm_between_msgs
    wake_poll = options.wake_poll
    result_server = options.result_server
    ventilator_server = options.ventilator_server

    if not(result_server and ventilator_server):
        print(parser.usage)
        sys.exit(1)

    # Fire up our result manager...
    result_manager = Process(target=result_manager, args=(tm_between_msgs, wake_poll, result_server, ventilator_server))
    result_manager.start()
