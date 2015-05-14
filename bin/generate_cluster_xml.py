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
#                                 --sock-port <port no>
#                                 --admin-port <port no> 
#                                 --http-port <port no>
#                                 --current-stores <current_stores.xml> 
#                                 --output-dir <output directory> 
#                                 --zones <number of zones>
#
#  For non zoned cluster use :
#  python generate_cluster_xml.py --file <file with host names, one host per line>
#                                 --name <name of the cluster>
#                                 --nodes <number of nodes>
#                                 --partitions <number of partitions> 
#                                 --sock-port <port no>
#                                 --admin-port <port no> 
#                                 --http-port <port no>
#                                 --current-stores <current_stores.xml> 
#                                 --output-dir <output directory>
#
# Note the absence of the --zones switch for the non zoned cluster use case.

import sys
import random
import os
import errno
import subprocess

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
parser.add_argument('-n', '--nodes', type=int, default=6, dest='nodes',
                    help='the number of nodes in the cluster')
parser.add_argument('-p', '--partitions', type=int, default=1500,
                    dest='partitions', help='number of partitions')
parser.add_argument('-sp', '--socket-port', type=int, default=6666,
                    dest='sock_port', help='socket port number')
parser.add_argument('-ap', '--admin-port', type=int, default=6667,
                    dest='admin_port', help='admin port number')
parser.add_argument('-hp', '--http-port', type=int, default=6665,
                    dest='http_port', help='http port number')
parser.add_argument('-s', '--current-stores', type=str, default= "config/tools/dummy-stores-3zoned.xml",
                    dest='current_stores',
                    help='Path to current stores xml. If you do not have info about the stores yet'
                         'use config/tools/dummy-stores.xml from the root voldemort home folder.')
parser.add_argument('-o', '--output-dir', type=str, dest='output_dir',
                    help='output directory location')
parser.add_argument('-z', '--zones', type=int, dest='zones',
                    help='For non zoned clusters do not provide this argument.' 
                         'For zoned clusters provide this argument with at least two zones.')

genType = parser.add_mutually_exclusive_group()
genType.add_argument('--seed', type=int, default=rseed, dest='seed',
                    help='seed for randomizing partition distribution')
                          
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
clusterXMLFilePath = os.path.join(os.path.abspath(args.output_dir), 'cluster.xml')
fileHandle = open(clusterXMLFilePath, 'w')

# TODO : It would be ideal to have the script accept a list of zone ids.
if args.zones:
  zones = args.zones
  if (zones == 1):
      print "For non zoned clusters do not provide this argument."
      print "For zoned clusters provide this argument with at least two zones."
      sys.exit(1)
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
vold_home = os.pardir
current_stores = os.path.join(vold_home, args.current_stores);

# Generate the full list of partition IDs
part_ids = range(partitions)
if part_ids < 1500:
  print 'Warning : The number of partitions seems to be low. Assuming max of 3 zones and 50 nodes ' \
        'per zone, a partition value of 1500 is recommended as it ensures an average of 10 ' \
        'partitions per node.'
  print 'Warning : The number of partitions seems to be low. Recommended value is 1500 or more.'
    
# Generate full list of zone IDs
if args.zones:
  zone_ids = range(zones)
  zone_id = 0

# Shuffle up the partitions
random.seed(seed)
random.shuffle(part_ids)

# Printing cluster.xml
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

# TODO : Currently, random partitions are assigned to the nodes in a round robin fashion. 
# A better approach would be to have some intelligence in the allocation such that 
# consecutive partition-ids do not land on the same node.

for i in xrange(nodes):
  j = i
  node_partitions = list()
  while j < len(part_ids):
    node_partitions.append(str(part_ids[j]))
    j += nodes;
  partitionslist = ", ".join(node_partitions);  
  
  print >> fileHandle, "  <server>"
  print >> fileHandle, "    <id>%d</id>" % i
  if args.file:
    print >> fileHandle, "    <host>%s</host>" % hostList[i].strip()
  else:
    print >> fileHandle, "    <host>host%d</host>" % i
  print >> fileHandle, "    <http-port>%d</http-port>" % http_port
  print >> fileHandle, "    <socket-port>%d</socket-port>" % sock_port
  print >> fileHandle, "    <admin-port>%d</admin-port>" % admin_port
  print >> fileHandle, "    <partitions>%s</partitions>" % partitionslist
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

# For zoned clusters call rebalance-new-cluster.sh
if args.zones:
    scriptPath = vold_home + '/bin/rebalance-new-zoned-cluster.sh'
    cmd = [scriptPath, '-v', vold_home, '-c', clusterXMLFilePath, '-s', current_stores, 
                   '-o', os.path.abspath(args.output_dir)]
    subprocess.call(cmd)
