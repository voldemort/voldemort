import sys
import random

if len(sys.argv) == 2:
	length = int(sys.argv[1])
else:
	length = 1000000

for i in xrange(length):
	print random.randint(0, 100*length)