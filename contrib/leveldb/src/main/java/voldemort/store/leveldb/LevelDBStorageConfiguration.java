package voldemort.store.leveldb;

import java.io.File;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;
import voldemort.utils.ReflectUtils;
import org.apache.log4j.Logger;

import voldemort.store.leveldb.leveldbjni.DbOptions;
import voldemort.store.leveldb.leveldbjni.WriteOptions;
import voldemort.store.leveldb.leveldbjni.ReadOptions;


public class LevelDBStorageConfiguration implements StorageConfiguration {
    public static final String TYPE_NAME = "leveldb";

    private static Logger logger = Logger.getLogger(LevelDBStorageConfiguration.class);
    private final Props props;
    private final String dataDirectory;
    private final int lockStripes;
    private final int poolsize;
    private final boolean iterators;
    //db options
    private final boolean createIfMissing;
    private final boolean errorIfExists;
    private final boolean paranoidChecks;
    private final long writeBufferSize;
    private final int maxOpenFile;
    private final long lRUBLockCache;
    private final int blockSize;
    private final int blockRestartInterval;
    private final int compressionType;
    //read options
    private final boolean fillCache;
    private final boolean verifyChecksums;
    //write options
    private final boolean sync;

    private final Object lock = new Object();

    public LevelDBStorageConfiguration(VoldemortConfig config) {
        props = config.getAllProps();
        File levelDir = new File(config.getDataDirectory(), this.TYPE_NAME);
        levelDir.mkdirs();
        this.poolsize = props.getInt("leveldb.poolsize", 4);
        this.iterators = props.getBoolean("leveldb.iterators", true);
        //db
        this.dataDirectory = levelDir.getAbsolutePath();
        this.lockStripes = props.getInt("leveldb.stripes", 100);
        this.createIfMissing = props.getBoolean("leveldb.createifmissing", true);
        this.errorIfExists = props.getBoolean("leveldb.errorifexists", false);
        this.paranoidChecks = props.getBoolean("leveldb.paranoidchecks", false);
        this.writeBufferSize = props.getLong("leveldb.writebuffer.size", 20l);
        this.maxOpenFile = props.getInt("leveldb.maxopenfile", 1024);
        this.lRUBLockCache = props.getLong("leveldb.lrublockcache", 100l);
        this.blockSize = props.getInt("leveldb.blocksize", 4*1024);
        this.blockRestartInterval = props.getInt("leveldb.blockrestartinterval", 16);
        this.compressionType = props.getInt("leveldb.compressiontype", 1);

        //read
        this.fillCache  = props.getBoolean("leveldb.read.fillcache", true);
        this.verifyChecksums = props.getBoolean("leveldb.read.verifychecksums", false);

        //write
        this.sync = props.getBoolean("leveldb.write.sync", false);

    }

    public void close() {}

    public StorageEngine<ByteArray, byte[], byte[]> getStore(String storeName) {
        synchronized(lock){
            int lockStripes          = props.getInt("leveldb."+storeName+".stripes", this.lockStripes);
            boolean createIfMissing  = props.getBoolean("leveldb."+storeName+".createifmissing", this.createIfMissing);
            boolean errorIfExists    = props.getBoolean("leveldb."+storeName+".errorifexists", this.errorIfExists  );
            boolean paranoidChecks   = props.getBoolean("leveldb."+storeName+".paranoidchecks",this.paranoidChecks  );
            long writeBufferSize     = props.getLong("leveldb."+storeName+".writebuffer.size", this.writeBufferSize);
            int maxOpenFile          = props.getInt("leveldb."+storeName+".maxopenfile",this.maxOpenFile);
            long lRUBLockCache       = props.getLong("leveldb."+storeName+".lrublockcache", this.lRUBLockCache );
            int blockSize            = props.getInt("leveldb."+storeName+".blocksize", this.blockSize);
            int blockRestartInterval = props.getInt("leveldb."+storeName+".blockrestartinterval", this.blockRestartInterval);
            int compressionType      = props.getInt("leveldb."+storeName+".compressiontype", this.compressionType);
            int poolsize             = props.getInt("leveldb."+storeName+".poolsize", this.poolsize);
            boolean iterators        = props.getBoolean("leveldb."+storeName+".iterators", this.iterators);
            
            //read
            boolean fillCache  = props.getBoolean("leveldb."+storeName+".read.fillcache", this.fillCache);
            boolean verifyChecksums = props.getBoolean("leveldb."+storeName+".read.verifychecksums", this.verifyChecksums);

            //write
            boolean sync = props.getBoolean("leveldb."+storeName+".write.sync", this.sync);

            DbOptions dbo = new DbOptions();
            logger.info("Config CreateIfMissing=" + createIfMissing);
            dbo.setCreateIfMissing(createIfMissing);
            dbo.setErrorIfExists(errorIfExists);
            dbo.setParanoidChecks(paranoidChecks); 
            logger.info("Config writeBufferSize=" + (writeBufferSize) + "MB");
            dbo.setWriteBufferSize(writeBufferSize * 1048576l);
            logger.info("Config maxOpenFile=" + maxOpenFile);
            dbo.setMaxOpenFile(maxOpenFile);
            logger.info("Config lRUBLockCache=" + (lRUBLockCache) + "MB");
            dbo.setLRUBlockCache(lRUBLockCache * 1048576l);
            logger.info("Config blockSize=" + blockSize + " B");
            dbo.setBlockSize(blockSize);
            dbo.setBlockRestartInterval(blockRestartInterval);

            ReadOptions ropts = new ReadOptions();
            logger.info("Config ReadCache=" + fillCache );
            ropts.setFillCache(fillCache);
            ropts.setVerifyChecksums(verifyChecksums);
            logger.info("Config SynchronousWrites=" + sync );
            WriteOptions wopts = new WriteOptions();
            wopts.setSync(sync);

            File storeDir = new File(dataDirectory, storeName);
            if(!storeDir.exists()) {
                logger.info("Creating leveldb data directory:" + storeDir.getAbsolutePath() );
                storeDir.mkdirs();
            }
            String path = storeDir.getAbsolutePath();

            return new LevelDBStorageEngine(storeName, path, lockStripes, dbo, ropts, wopts,poolsize,iterators);
        }
    }

    public String getType(){
        return TYPE_NAME;
    }

}