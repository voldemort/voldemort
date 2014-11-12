# Voldemort is a distributed key-value storage system #

## Overview ##

* Data is automatically replicated over multiple servers across multiple datacenters.
* Data is automatically partitioned so each server contains only a subset of the total data
* Server failure is handled transparently
* Pluggable serialization is supported to allow rich keys and values including lists and tuples with named fields, as well as to integrate with common serialization frameworks like Protocol Buffers, Thrift, and Java Serialization
* Data items are versioned to maximize data integrity in failure scenarios without compromising availability of the system
* Each node is independent of other nodes with no central point of failure or coordination
* Pluggable storage engines, to cater to different workloads
* SSD Optimized Read Write storage engine, with support for multi-tenancy
* Built in mechanism to fetch & serve batch computed data from Hadoop 
* Support for pluggable data placement strategies to support things like distribution across data centers that are geographical far apart.

It is used at LinkedIn by numerous critical services powering a large portion of the site. .

## QuickStart ##

*You can refer to http://www.project-voldemort.com for more info*

### Download Code ###

```bash
cd ~/workspace
git clone https://github.com/voldemort/voldemort.git
cd voldemort
ant release
```

### Start Server ###

```
# in one terminal
bin/voldemort-server.sh config/single_node_cluster
```

### Use Client Shell ###

Client shell gives you fast access to the store. We already have a test store defined in the "single_node_cluster", whose key and value are both String.

```bash
# in another terminal
cd ~/workspace/voldemort
bin/voldemort-shell.sh test tcp://localhost:6666/
```

Now you have the the voldemort shell running. You can try these commands in the shell

```
put "k1" "v1"
put "k2" "v2"
get "k1"
getall "k1" "k2"
delete "k1"
get "k1"
```

You can find more commands by running```help```


## Comparison to relational databases ##

Voldemort is not a relational database, it does not attempt to satisfy arbitrary relations while satisfying ACID properties. Nor is it an object database that attempts to transparently map object reference graphs. Nor does it introduce a new abstraction such as document-orientation. It is basically just a big, distributed, persistent, fault-tolerant hash table. For applications that can use an O/R mapper like ActiveRecord or Hibernate this will provide horizontal scalability and much higher availability but at great loss of convenience. For large applications under internet-type scalability pressure, a system may likely consist of a number of functionally partitioned services or apis, which may manage storage resources across multiple data centers using storage systems which may themselves be horizontally partitioned. For applications in this space, arbitrary in-database joins are already impossible since all the data is not available in any single database. A typical pattern is to introduce a caching layer which will require hashtable semantics anyway. For these applications Voldemort offers a number of advantages:

* Voldemort combines in memory caching with the storage system so that a separate caching tier is not required (instead the storage system itself is just fast).
* Unlike MySQL replication, both reads and writes scale horizontally
* Data partioning is transparent, and allows for cluster expansion without rebalancing all data
* Data replication and placement is decided by a simple API to be able to accommodate a wide range of application specific strategies
* The storage layer is completely mockable so development and unit testing can be done against a throw-away in-memory storage system without needing a real cluster (or even a real storage system) for simple testing

## Contribution ##

The source code is available under the Apache 2.0 license. We are actively looking for contributors so if you have ideas, code, bug reports, or fixes you would like to contribute please do so.

For help please see the [discussion group](http://groups.google.com/group/project-voldemort), or the IRC channel chat.us.freenode.net #voldemort. Bugs and feature requests can be filed on [Github](https://github.com/voldemort/voldemort/issues).

## Special Thanks ##

We would like to thank [JetBrains](http://www.jetbrains.com) for supporting Voldemort Project by offering open-source license of their [IntelliJ IDE](http://www.jetbrains.com/idea/) to us.
