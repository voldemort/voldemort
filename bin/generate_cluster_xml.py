#!/usr/bin/python

#
#   Copyright 2013 LinkedIn, Inc
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

#  Python 2.7+ required
#  This script encapsulates the cluster.xml generation for a zoned and a non-zoned 
#  cluster. Passing --zones <num of zones> switch to the script generates a zoned cluster
#  config. A non zoned cluster is generated otherwise.
#  
#  The newly generated cluster.xml file is placed in the output dir.
#
#  Example use for a zoned cluster :
#  python generate_cluster_xml.py --file <file with host names, one host per line>
#                                 --name <name of the cluster>
#                                 --nodes <number of nodes>
#                                 --partitions <number of partitions>
#                                 --sock-port
#                                 --admin-port  
#                                 --http-port
#                                 --seed <seed value>
#                                 --zones <number of zones>
#
#  The non zoned would look similar with the exception of the absence of the --zones switch.
#  

import sys
import random
import os
import errno
try:
    import argparse
except ImportError:
    print "Python 2.7 or higher is needed"

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
parser.add_argument('-z', '--zones', type=int, dest='zones',
                    help='the number of zones you will have')
parser.add_argument('-o', '--output-dir', type=str, dest='output_dir',
                    help='output directory location')
                          
# Parse arguments
args = parser.parse_args()

# Check if the input file exists
try:
   with open(args.file): pass
except IOError:
   print 'File does not exist'

# create output-dir if it does not exist
try:
    os.makedirs(args.output_dir)
except OSError as exception:
    if exception.errno != errno.EEXIST:
        raise

# Open a new file named cluster.xml     
filepath = os.path.join(args.output_dir, 'cluster.xml')
fileHandle = open(filepath, 'w')

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
# print "<!-- Partition distribution generated using seed [%d] -->" % seed
print >> fileHandle, "<cluster>"
print >> fileHandle, "  <name>%s</name>" % name

if args.zones:
  for i in range(args.zones):
    print >> fileHandle, "  <zone>"
    print >> fileHandle, "    <zone-id>%d</zone-id>" % i
    proximityList = list()
    for j in range(1, len(zone_ids) ):
      proximityList.append(zone_ids[(i+j)%len(zone_ids)])
    print >> fileHandle, "    <proximity-list>%s</proximity-list>" % str(proximityList).strip('[]') 
    print >> fileHandle, "  </zone>" 

for i in xrange(nodes):
  node_partitions = ", ".join(str(p) for p in sorted(part_ids[i*partitions:(i+1)*partitions]))

  print >> fileHandle, "  <server>"
  print >> fileHandle, "    <id>%d</id>" % i
  if args.file:
    print >> fileHandle, "    <host>%s</host>" % hostList[i].strip()
  else:
    print >> fileHandle, "    <host>host%d</host>" % i
  print >> fileHandle, "    <http-port>%d</http-port>" % http_port
  print >> fileHandle, "    <socket-port>%d</socket-port>" % sock_port
  print >> fileHandle, "    <admin-port>%d</admin-port>" % admin_port
  print >> fileHandle, "    <partitions>%s</partitions>" % node_partitions
  # If zones are being used, assign a zone-id
  if args.zones:
    print >> fileHandle, "    <zone-id>%d</zone-id>" % zone_id
    if zone_id == (zones - 1):
      zone_id = 0
    else:
      zone_id += 1
  print >> fileHandle, "  </server>"
print >> fileHandle, "</cluster>"

fileHandle.close()