package voldemort.store.rocksdb;

import java.util.Collections;
import java.util.List;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import voldemort.store.AbstractStorageEngine;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StoreBinaryFormat;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

/**
 * A StorageEngine that uses RocksDB for persistence
 * 
 * 
 */
public class RocksDbStorageEngine extends AbstractStorageEngine<ByteArray, byte[], byte[]> {

    private RocksDB rdb;

    // TODO Need to add stats and loggers later

    public RocksDbStorageEngine(String name, RocksDB rdbInstance) {
        super(name);
        this.rdb = rdbInstance;
    }

    public RocksDB getRdb() {
        return rdb;
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms)
            throws PersistenceFailureException {
        // TODO read locks ?
        StoreUtils.assertValidKey(key);
        List<Versioned<byte[]>> value = null;
        try {
            byte[] result = this.rdb.get(key.get());
            if(result != null) {
                value = StoreBinaryFormat.fromByteArray(result);
            } else {
                return Collections.emptyList();
            }
        } catch(RocksDBException rocksdbException) {
            throw new PersistenceFailureException(rocksdbException);
        } finally {
            // TODO log time taken
        }
        return value;
    }

}