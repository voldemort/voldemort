[Leveldb](http://code.google.com/p/leveldb/) storage engine for Voldemort.

## Why

LevelDB use snappy compression per block
levelDB attempt to limit the amount of required seek to read the data.

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

Run:

    ./make.sh # at the root of the leveldb contrib

or 

    ant # at the root of voldemort

The script should download snappy and leveldb for build. (note this is not very suitable for a release build as we get leveldb's trunk)

### Configuration

There is a bunch of configuration you can tune for leveldb, Here is a summary of those : 

* poolsize              = 4     : 0 for no pool, use a thread pool to limit concurrent access to leveldb api. 
* iterators             = true  : use iterators instead of get api to do reads
* stripes               = 100   : maximum concurrent key write allowed
* createifmissing       = true  : create the db directory if not exist
* errorifexists         = false : see leveldb doc
* paranoidchecks        = false : paranoid checks. see leveldb doc
* writebuffer.size      = 20    : size of the write buffer in MB, large buffer prevent frequent recompaction
* maxopenfile           = 1024  : maximum number of open files 
* lrublockcache         = 300   : size of the read cache in MB (space to store decompressed blocks)
* blocksize             = 4096  : size of a block in Bytes
* blockrestartinterval  = 16    : you should not change this value according to leveldb doc
* compressiontype       = 1     : use snappy (1) no compression (0) snappy is advised
* read.fillcache        = true  : fill the cache for each block read (may slow down iterations)
* read.verifychecksums  = false : Verify checksum for each read
* write.sync            = false : Sync each write to disk (slow down the db a lot)

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

The main purpose of our testing is to compare the read throughput with bdb, as well as the db size. Tests are based on our internal project, therefore, the result of writing and reading throughput is for information only. It does not represent the real db throughput due to some operation may affected by the application process. 

###Test 1

Environment:

* 2.4G memory
* Intel(R) Core(TM) i3 CPU 550  @ 3.20GHz x4
* JVM Xmx1G
* DB size : 0.5 - 3 million data sets (each data set contains 2 index keys and 1 actual data value)
* Data size : 1k
* Data format : json (compressible)

<pre><code>

   DB size :  0.5    1    1.6    2.4     3
   bdb     :  1.1   3.7   5.9     9     >10
   leveldb :  0.3   0.7   1.2    1.7    2.1


   Leveldb Sequential Read :  ~ 676 ops/sec
   Leveldb Random Read     :  ~ 557 ops/sec
   BDB Sequential Read     :  ~ 696 ops/sec
   BDB Random Read         :  ~ 235 ops/sec

</code></pre>

* BDB drop the speed while size is larger than the cache. The result shows the huge drop when data reached to 1 million data sets.
* LevelDB maintain the average speed while reached to 2.7 million of data sets.
* If db size is within the cache size, then performance of BDB is actually a bit better than Leveldb.
  
## Conclusion

If leveldb doesn't provide better performance at the moment compared to BDB, it is still more efficient for storing the data and offers a real benefit if the data are compressible. For the same amount of memory, you will be likely to store more records before voldemort slows down.

## Possible improvements

* Separate thread pool for read and write.
* Use iterator and sorted table to store version in different records.
* Better configuration depending on the targeted db size.
