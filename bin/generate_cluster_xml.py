import sys
import random

if len(sys.argv) != 3:
    print >> sys.stderr, "USAGE: python generate_partitions.py <nodes_file> <partitions_per_node>"
    sys.exit()

FORMAT_WIDTH = 10

nodes = 0
for line in open(sys.argv[1],'r'):
	nodes+=1

partitions = int(sys.argv[2])

ids = range(nodes * partitions)

# use known seed so this is repeatable
random.seed(92873498274)
random.shuffle(ids)

print '<cluster>'
print '<name>prodcluster</name>'
id = 0
for host in open(sys.argv[1],'r'):
    print '<server>'
    print "  <id>%d</id>" % id
    print "  <host>%s</host>" % host.strip()
    print '  <http-port>8081</http-port>'
    print '  <socket-port>6666</socket-port>'
    print '  <partitions>',
    node_ids = sorted(ids[id*partitions:(id+1)*partitions])
    for j in xrange(len(node_ids)):
        print str(node_ids[j]) + ',',
        if j % FORMAT_WIDTH == FORMAT_WIDTH - 1:
            print '    ',
    print '  </partitions>'
    print '</server>'
    id += 1
print '</cluster>'

        
