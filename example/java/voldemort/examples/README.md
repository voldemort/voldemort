This is a quick start for using a Store Client to talk to a Voldemort Store

```
# First, build the client and Voldemort (if not yet):
./build.sh

# Then start the server in another terminal if using local server
# Change directory to project root
bin/voldemort-server.sh config/single_node_cluster

# Then run the example program under this folder
./run.sh
```
 
