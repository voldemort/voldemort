[Leveldb](http://code.google.com/p/leveldb/) storage engine for Voldemort.

## Why

LevelDB use snappy compression per block and attempt to limit the amount of required seek to read the data.

Bdb could do better usage of all its memory

## Usage

Just specify "leveldb" as your storage, don't use compression as possible.

### Compilation

Leveldb is a C++ product and should therefore be compiled for your platform. In this first release, we provide a dynamic library that includes snappy, leveldb and the JNI wrapper.
We did test the build on centos and maxos 10.6 only.

Requirements : 

* git 
* gcc-g++
* java

Just execute :
    
    cd contrib/leveldb
    ./make.sh # to build leveldb and leveldbjni
    . /setenv.sh # set the java library path, required before running test or voldemort server

Build voldemort : 

    ant # at the root of voldemort

The script should download snappy and leveldb for build. (note this is not very suitable for a release build as we get leveldb's trunk)

### Run the sample configuration

Make sure you ran setenv (see compilation)

    ./bin/voldemort-server.sh contrib/leveldb/config/single_node_cluster/

### Configuration

There is a bunch of configuration you can tune for leveldb, Here is a summary of those : 

    poolsize              = 4     : 0 for no pool, use a thread pool to limit concurrent access to leveldb api. 
    iterators             = true  : use iterators instead of get api to do reads
    stripes               = 100   : maximum concurrent key write allowed
    createifmissing       = true  : create the db directory if not exist
    errorifexists         = false : see leveldb doc
    paranoidchecks        = false : paranoid checks. see leveldb doc
    writebuffer.size      = 20    : size of the write buffer in MB, large buffer prevent frequent recompaction
    maxopenfile           = 1024  : maximum number of open files 
    lrublockcache         = 300   : size of the read cache in MB (space to store decompressed blocks)
    blocksize             = 4096  : size of a block in Bytes
    blockrestartinterval  = 16    : you should not change this value according to leveldb doc
    compressiontype       = 1     : use snappy (1) no compression (0) snappy is advised
    read.fillcache        = true  : fill the cache for each block read (may slow down iterations)
    read.verifychecksums  = false : Verify checksum for each read
    write.sync            = false : Sync each write to disk (slow down the db a lot)

All configuration should be prefixed either by "leveldb" 
> ie leveldb.poolsize
or by "leveldb.${your store name}"
> ie leveldb.test.poolsize
to overwrite the default value.

Priority are perstore configuration then global configuration then default.

### Configuration tips

* No need to oversize ldb caches
* Larger write buffer will make longer pause for compression
* Larger block size are better to reload the db quickly in memory but are more likely to cache miss if the db is too large, in addition, it can slow down if the db is too large

## Tests

### Test 1

For this test we used voldemort's performance tool. We mostly focussed on batch write then read only scenario but additional r/w/d scenario should idealy be performed. The test is ran localy on a single node (imac 2010 with 16GB of ram). Nor Bdb or Leveldb store is configured with compression but leveldb use snappy natively, this can have a negative impact, especially in the case when the data is randomly generated (0% compressible on the graphs).

For each test we load first the database with data of the required size then we run a series of test. We take the average throughput excluding the warm up phase.

This first test uses fairly large value of 1KB. BDB offers a pretty good throughput but the performance degrades quicker than leveldb.
![Alt text](http://github.com/btu/voldemort/raw/master/contrib/leveldb/doc/leveldb.002.jpg)

This second test focus on smaller data (80 Bytes). In that case, leveldb can handle a slightly larger volume of entries even with randomly generated data. We suspect for a while that BDB has a large overhead per entry which becomes expensive for smaller data.
![Alt text](http://github.com/btu/voldemort/raw/master/contrib/leveldb/doc/leveldb.003.jpg)

This last test focus on leveldb block size impact for a 100 000 items db. We mostly notice that the block size have a negative impact on performance when the block is larger, however it helps to load the data in memory quicker. This is only a supposition and would require deeper analysis but when the dataset is cold (freshly inserted or server restarted) the random read performance are really slow and a large amount of read IO are performed. When starting the server, performing an initial sequential scan of the DB prevent this behavior.
![Alt text](http://github.com/btu/voldemort/raw/master/contrib/leveldb/doc/leveldb.001.jpg)

We should note that there is many potential optimization and tuning that can be done on leveldb and that we don't totally master. BDB in the other hand seems to have a fairly predictable and good behavior with few tuning as long as the whole database fit in memory.

This case didn't focus much on compression but our second test do.

### Test 2

The main purpose of our testing is to compare the read throughput with bdb, as well as the db size. This test is based on our internal project, therefore, the result of writing and reading throughput is for information only. It does not represent the real db throughput due to some operation may affected by the application process. 

Environment:

* 4G memory, 2.5G available before test
* Intel(R) Core(TM) i3 CPU 550  @ 3.20GHz x4
* JVM Xmx1G
* DB size : 0.5 - 3 million entries
* Data size : about 1,7k split in 6 entries, 2 larger of 1KB and 600 Bytes and 4 small of 30-40 Bytes. All have 30-40 Bytes key.
* Data format : json (compress well)

![Alt text](http://github.com/btu/voldemort/raw/master/contrib/leveldb/doc/leveldb.004.jpg)

* BDB drop the speed while size is larger than the cache. The result shows the huge drop when data reached to 1 million entries.
* LevelDB maintain the average speed while reached to 2.7 million of entries.
* If db size is within the cache size, then performance of BDB is actually a bit better than Leveldb.
  
## Conclusion

If leveldb doesn't provide better performance at the moment compared to BDB, it is still more efficient for storing the data and offers a real benefit if the data are compressible or the value size is smaller. For the same amount of memory, you will be likely to store more records before voldemort slows down. 

## Possible improvements

* Separate thread pool for read and write.
* Use iterator and sorted table to store version in different records.
* Better configuration depending on the targeted db size / properties.
