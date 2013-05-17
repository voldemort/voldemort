#! /bin/bash

JMXTERM_CMD="run -b voldemort.server.niosocket:type=nio-socket-server tripSocketServer"
echo $JMXTERM_CMD  | java -jar lib/jmxterm-1.0-alpha-4-uber.jar -l localhost:7777 -n
