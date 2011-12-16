package voldemort.store.leveldb;

import java.io.File;
import java.util.List;

import voldemort.TestUtils;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import voldemort.store.leveldb.leveldbjni.DbOptions;
import voldemort.store.leveldb.leveldbjni.ReadOptions;
import voldemort.store.leveldb.leveldbjni.WriteOptions;

public class LevelDBStorageEngineTest extends AbstractStorageEngineTest {

    private StorageEngine<ByteArray, byte[], byte[]> store = null;
    String storeName = "test";
    String path = "dbtest";
    DbOptions dbo = new DbOptions();
    ReadOptions ropt = new ReadOptions();
    WriteOptions wopt = new WriteOptions();

    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        File storeDir = TestUtils.createTempDir();
        storeDir.mkdirs();
        storeDir.deleteOnExit();
        path = storeDir.getAbsolutePath();
        
        dbo.setCreateIfMissing(true);
        dbo.setErrorIfExists(false);
        dbo.setParanoidChecks(false); 
        dbo.setWriteBufferSize(20*1048576);
        dbo.setMaxOpenFile(1024);
        dbo.setLRUBlockCache(100*1048576l);
        dbo.setBlockSize(4*1024);
        dbo.setBlockRestartInterval(16);
        dbo.setCompressionType(1);
        
        ropt.setFillCache(true);
        ropt.setVerifyChecksums(false);

        wopt.setSync(true);
        
        this.store = new LevelDBStorageEngine(storeName, path, 10, dbo, ropt, wopt,4,true);

    }

    @Override
    public StorageEngine<ByteArray, byte[], byte[]> getStorageEngine() {
        return this.store;
    }

    public void testPersistence() throws Exception {
        this.store.put(new ByteArray("abc".getBytes()),
                       new Versioned<byte[]>("cdef".getBytes()),
                       null);
        this.store.close();
        this.store = new LevelDBStorageEngine(storeName, path, 10, dbo, ropt, wopt,4,true);
        List<Versioned<byte[]>> vals = store.get(new ByteArray("abc".getBytes()), null);
        assertEquals(1, vals.size());
        TestUtils.bytesEqual("cdef".getBytes(), vals.get(0).getValue());
    }

    @Override
    public void tearDown() {
        store.truncate();
    }

}
