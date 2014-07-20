package voldemort.store.rocksdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import voldemort.VoldemortException;
import voldemort.store.AbstractStorageEngine;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StoreBinaryFormat;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.StripedLock;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occurred;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A StorageEngine that uses RocksDB for persistence
 * 
 * 
 */
public class RocksDbStorageEngine extends AbstractStorageEngine<ByteArray, byte[], byte[]> {

    private static final Logger logger = Logger.getLogger(RocksDbStorageEngine.class);
    private RocksDB rocksDB;
    private final StripedLock locks;
    private static final Hex hexCodec = new Hex();

    // TODO Need to add stats and loggers later

    public RocksDbStorageEngine(String name, RocksDB rdbInstance, int lockStripes) {
        super(name);
        this.rocksDB = rdbInstance;
        this.locks = new StripedLock(lockStripes);
    }

    public RocksDB getRocksDB() {
        return rocksDB;
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms)
            throws PersistenceFailureException {
        // TODO read locks ?
        StoreUtils.assertValidKey(key);
        List<Versioned<byte[]>> value = null;
        try {
            byte[] result = getRocksDB().get(key.get());
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

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        // TODO Does RocksDB multiget supports atomicity ?
        StoreUtils.assertValidKeys(keys);
        Map<ByteArray, List<Versioned<byte[]>>> results = null;

        try {
            results = StoreUtils.getAll(this, keys, transforms);
        } catch(PersistenceFailureException pfe) {
            throw pfe;
        } finally {
            // TODO log time taken
        }

        return results;
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);

        synchronized(this.locks.lockFor(key.get())) {
            // get the value
            List<Versioned<byte[]>> currentValues = get(key, transforms);

            if(currentValues.size() > 0) {
                // compare vector clocks and throw out old ones, for updates
                Iterator<Versioned<byte[]>> iter = currentValues.iterator();
                while(iter.hasNext()) {
                    Versioned<byte[]> curr = iter.next();
                    Occurred occured = value.getVersion().compare(curr.getVersion());
                    if(occured == Occurred.BEFORE) {
                        throw new ObsoleteVersionException("Key "
                                                           + new String(hexCodec.encode(key.get()))
                                                           + " "
                                                           + value.getVersion().toString()
                                                           + " is obsolete, it is no greater than the current version of "
                                                           + curr.getVersion().toString() + ".");
                    } else if(occured == Occurred.AFTER) {
                        iter.remove();
                    }
                }
            } else {
                // if value does not exist add the value from put request to
                // existing values
                currentValues = new ArrayList<Versioned<byte[]>>(1);
            }
            currentValues.add(value);

            try {
                getRocksDB().put(key.get(), StoreBinaryFormat.toByteArray(currentValues));
            } catch(RocksDBException rocksdbException) {
                throw new PersistenceFailureException(rocksdbException);
            } finally {
                // TODO logging
            }
        }
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws PersistenceFailureException {

        StoreUtils.assertValidKey(key);

        long startTimeNs = -1;

        // if(logger.isTraceEnabled())
        startTimeNs = System.nanoTime();

        synchronized(this.locks.lockFor(key.get())) {
            try {
                byte[] value = getRocksDB().get(key.get());

                if(value == null) {
                    return false;
                }

                if(version == null) {
                    // unversioned delete. Just blow away the whole thing
                    getRocksDB().remove(key.get());
                    return true;
                } else {
                    // versioned deletes; need to determine what to delete

                    List<Versioned<byte[]>> vals = StoreBinaryFormat.fromByteArray(value);
                    Iterator<Versioned<byte[]>> iter = vals.iterator();
                    int numVersions = vals.size();
                    int numDeletedVersions = 0;

                    // go over the versions and remove everything before the
                    // supplied version
                    while(iter.hasNext()) {
                        Versioned<byte[]> curr = iter.next();
                        Version currentVersion = curr.getVersion();
                        if(currentVersion.compare(version) == Occurred.BEFORE) {
                            iter.remove();
                            numDeletedVersions++;
                        }
                    }

                    if(numDeletedVersions < numVersions) {
                        // we still have some valid versions
                        value = StoreBinaryFormat.toByteArray(vals);
                        getRocksDB().put(key.get(), value);
                    } else {
                        // we have deleted all the versions; so get rid of the
                        // entry
                        // in the database
                        getRocksDB().remove(key.get());
                    }
                    return numDeletedVersions > 0;
                }
            } catch(RocksDBException e) {
                logger.error(e);
                throw new PersistenceFailureException(e);
            } finally {
                if(logger.isTraceEnabled()) {
                    logger.trace("Completed DELETE (" + getName() + ") of key "
                                 + ByteUtils.toHexString(key.get()) + " (keyRef: "
                                 + System.identityHashCode(key) + ") in "
                                 + (System.nanoTime() - startTimeNs) + " ns at "
                                 + System.currentTimeMillis());
                }
            }
        }
    }
}