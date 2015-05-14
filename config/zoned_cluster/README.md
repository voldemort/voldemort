- Zone configs contains configurations for four servers in two zones. Zone 0 (Node 0, Node 2) and Zone 1 (Node 1, Node 3)
- To start the servers 
(a) Node 0 (Zone 0) => bin/voldemort-server.sh config/zoned_cluster/node_0
(b) Node 1 (Zone 1) => bin/voldemort-server.sh config/zoned_cluster/node_1
(c) Node 2 (Zone 0) => bin/voldemort-server.sh config/zoned_cluster/node_2
(d) Node 3 (Zone 1) => bin/voldemort-server.sh config/zoned_cluster/node_3
