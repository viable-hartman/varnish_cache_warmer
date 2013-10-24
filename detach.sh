#!/bin/bash

/usr/bin/nohup %(CMD)s >& %(LOG)s < /dev/null &
#/usr/bin/screen -S "CACHEWARM" -d -m %(CMD)s

