varnish_cache_warmer
====================

Python + ZeroMQ + Fabric code that allows you to spin up a distributed varnish cache warming script, among other things.

Description of Files
--------------------

* cache_ventilator.py: This script parses the URLs out of a sitemap.xml file passed in on command line and creates
 a message queues for each worker server that will transmit the parsed URLs to.
* cache_worker.py: This script is the worker code that pulls URLs from the ventilator message queues and performs the actual request.  It binds to the result_manager queues as well to transmit its warming results.
* cache_result_manager.py: This script aggregates the results from all the worker servers and processes within the worker servers.  It reports the results on stdout and keeps a msg_count of each message it receives.  It compares that count to the expected count recieved on a separate message queue from the ventilator to know when to stop.  When it receives the last worker message it informs the workers to terminate via a control message queue.  There is a failsafe SIGALARM interrupt in place to control termination in case something goes awry.
* cache_warmer: Uses python fabric to ssh into the ventilator, result_manager, and worker servers and spin them up by first installing the necessary dependencies, then downloading the sitemap.xml file to the ventilator, next putting the necessary control code on each server, and finally launching them all in the following order workers->result_manager->ventilator.

Configuration of Firewall
-------------------------

In this setup IP Tables similar to the below would need to be configured (Of course this could be simplified, but this is a reasonable example):

```
# On cache result_manger server for receiving results
-A INPUT -s <worker 1 IP>/32 -p tcp -m tcp --dport 5558 -j ACCEPT
-A INPUT -s <worker 2 IP>/32 -p tcp -m tcp --dport 5558 -j ACCEPT
# On cache result_manger server for sending control messages 
-A INPUT -s <worker 1 IP>/32 -p tcp -m tcp --dport 5559 -j ACCEPT
-A INPUT -s <worker 2 IP>/32 -p tcp -m tcp --dport 5559 -j ACCEPT
# On ventilator server for sending url job messages
-A INPUT -s <worker 1 IP>/32 -p tcp -m tcp --dport 6557 -j ACCEPT
-A INPUT -s <worker 2 IP>/32 -p tcp -m tcp --dport 6558 -j ACCEPT
# On ventilator server for sending message count to result_manager
-A INPUT -s <result_manager IP>/32 -p tcp -m tcp --dport 5560 -j ACCEPT
``` 

Of course you could just turn iptables off.

Execution After Configuration
-----------------------------

```
# List possible tasks
fab -f cache_warmer.py list
# Run cache warmer
fab -f cache_warmer.py cacheWarmer
# Clean remote files to push changes
fab -f cache_warmer.py clean | cleanWorker | cleanResultManager | cleanVentilator
```

Basic Flow Diagram
------------------
![alt tag](https://raw.github.com/viable-hartman/varnish_cache_warmer/master/varnish_cache_warmer.png)

