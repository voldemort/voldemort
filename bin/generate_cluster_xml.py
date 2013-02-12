#!/usr/bin/python

import sys
import random
import argparse

# Get a random seed
rseed = int(random.randint(00000000001,99999999999))

# Setup and argument parser
parser = argparse.ArgumentParser(description='Build a voldemort cluster.xml.')
# Add supported arguments
parser.add_argument('-f', '--file', type=str, dest='file',
                    help='the file of the list of hosts(one per line)')
parser.add_argument('-N', '--name', type=str, default='voldemort', dest='name',
                    help='the name you want to give the cluster')
parser.add_argument('-n', '--nodes', type=int, default=2, dest='nodes',
                    help='the number of nodes in the cluster')
parser.add_argument('-p', '--partitions', type=int, default=300,
                    dest='partitions', help='number of partitions per node')
parser.add_argument('-s', '--socket-port', type=int, default=6666,
                    dest='sock_port', help='socket port number')
parser.add_argument('-a', '--admin-port', type=int, default=6667,
                    dest='admin_port', help='admin port number')
parser.add_argument('-H', '--http-port', type=int, default=6665,
                    dest='http_port', help='http port number')
genType = parser.add_mutually_exclusive_group()
genType.add_argument('-S', '--seed', type=int, default=rseed, dest='seed',
                    help='seed for randomizing partition distribution')
genType.add_argument('-l', '--loops', type=int, default=1000, dest='loops',
                    help='loop n times, using a different random seed every \
                          time (Note: not currently supported)')
parser.add_argument('-z', '--zones', type=int, dest='zones',
                    help='if using zones, the number of zones you will have\
                          (Note: you must add your own <zone> fields \
                          manually)')

# Parse arguments
args = parser.parse_args()

# Check args
if args.zones:
  zones = args.zones
  if (args.nodes % zones) != 0:
    print "Number of nodes must be evenly divisible by number of zones"
    sys.exit(1)

# Store arguments
if args.file:
  hostList = open(args.file).readlines()
  nodes = len(hostList)
else:
  nodes = args.nodes
partitions = args.partitions
name = args.name
http_port = args.http_port
sock_port = args.sock_port
admin_port = args.admin_port
seed = args.seed

# Generate the full list of partition IDs
part_ids = range(nodes * partitions)
# Generate full list of zone IDs
if args.zones:
  zone_ids = range(zones)
  zone_id = 0

# Shuffle up the partitions
random.seed(seed)
random.shuffle(part_ids)

# Printing cluster.xml
print "<!-- Partition distribution generated using seed [%d] -->" % seed
print "<cluster>"
print "  <name>%s</name>" % name

for i in xrange(nodes):
  node_partitions = ", ".join(str(p) for p in sorted(part_ids[i*partitions:(i+1)*partitions]))

  print "  <server>"
  print "    <id>%d</id>" % i
  if args.file:
    print "    <host>%s</host>" % hostList[i].strip()
  else:
    print "    <host>host%d</host>" % i
  print "    <http-port>%d</http-port>" % http_port
  print "    <socket-port>%d</socket-port>" % sock_port
  print "    <admin-port>%d</admin-port>" % admin_port
  print "    <partitions>%s</partitions>" % node_partitions
  # If zones are being used, assign a zone-id
  if args.zones:
    print "    <zone-id>%d</zone-id>" % zone_id
    if zone_id == (zones - 1):
      zone_id = 0
    else:
      zone_id += 1
  print "  </server>"

print "</cluster>"
