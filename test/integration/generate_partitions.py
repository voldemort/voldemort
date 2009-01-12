import sys
import random

if len(sys.argv) != 3:
    print >> sys.stderr, "USAGE: python generate_partitions.py nodes partitions_per_node"
    sys.exit()

FORMAT_WIDTH = 10

nodes = int(sys.argv[1])
partitions = int(sys.argv[2])

ids = range(nodes * partitions)

# use known seed so this is repeatable
random.seed(92873498274)
random.shuffle(ids)

for i in xrange(nodes):
    print 
    print 'node', i
    print '<partitions>'
    print '    ',
    node_ids = sorted(ids[i*partitions:(i+1)*partitions])
    for j in xrange(len(node_ids)):
        print str(node_ids[j]) + ',',
        if j % FORMAT_WIDTH == FORMAT_WIDTH - 1:
            print
            print '    ',
    print '</partitions>'

        
