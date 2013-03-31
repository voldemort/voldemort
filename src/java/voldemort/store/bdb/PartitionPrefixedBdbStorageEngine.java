/*
 * Copyright 2008-2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.bdb;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import voldemort.routing.RoutingStrategy;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StoreBinaryFormat;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * Extends BDB Storage Engine with capabilities to perform partition range
 * scans, to speed up scan jobs, that filter on partition id
 * 
 */
public class PartitionPrefixedBdbStorageEngine extends BdbStorageEngine {

    private static final Logger logger = Logger.getLogger(PartitionPrefixedBdbStorageEngine.class);
    private final RoutingStrategy routingStrategy;

    public PartitionPrefixedBdbStorageEngine(String name,
                                             Environment environment,
                                             Database database,
                                             BdbRuntimeConfig config,
                                             RoutingStrategy strategy) {
        super(name, environment, database, config);
        this.routingStrategy = strategy;
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(int partition) {
        try {
            Cursor cursor = getBdbDatabase().openCursor(null, null);
            // evict data brought in by the cursor walk right away
            if(this.minimizeScanImpact)
                cursor.setCacheMode(CacheMode.EVICT_BIN);
            return new BdbPartitionEntriesIterator(cursor, partition, this);
        } catch(DatabaseException e) {
            this.bdbEnvironmentStats.reportException(e);
            logger.error(e);
            throw new PersistenceFailureException(e);
        }
    }

    @Override
    public ClosableIterator<ByteArray> keys(int partition) {
        try {
            Cursor cursor = getBdbDatabase().openCursor(null, null);
            // evict data brought in by the cursor walk right away
            if(this.minimizeScanImpact)
                cursor.setCacheMode(CacheMode.EVICT_BIN);
            return new BdbPartitionKeysIterator(cursor, partition, this);
        } catch(DatabaseException e) {
            this.bdbEnvironmentStats.reportException(e);
            logger.error(e);
            throw new PersistenceFailureException(e);
        }
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms)
            throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);
        int partition = routingStrategy.getMasterPartition(key.get());
        ByteArray prefixedKey = new ByteArray(StoreBinaryFormat.makePrefixedKey(key.get(),
                                                                                partition));
        return super.get(prefixedKey, transforms);
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws PersistenceFailureException {

        StoreUtils.assertValidKey(key);
        int partition = routingStrategy.getMasterPartition(key.get());
        ByteArray prefixedKey = new ByteArray(StoreBinaryFormat.makePrefixedKey(key.get(),
                                                                                partition));
        super.put(prefixedKey, value, transforms);
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws PersistenceFailureException {

        StoreUtils.assertValidKey(key);
        int partition = routingStrategy.getMasterPartition(key.get());
        ByteArray prefixedKey = new ByteArray(StoreBinaryFormat.makePrefixedKey(key.get(),
                                                                                partition));
        return super.delete(prefixedKey, version);
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    /**
     * Implements a range scan over the partition entries
     * 
     */
    private static class BdbPartitionEntriesIterator extends
            BdbIterator<Pair<ByteArray, Versioned<byte[]>>> {

        private List<Pair<ByteArray, Versioned<byte[]>>> cache;
        private int partition;
        private boolean positioned;

        public BdbPartitionEntriesIterator(Cursor cursor, int partition, BdbStorageEngine bdbEngine) {
            super(cursor, bdbEngine);
            this.partition = partition;
            this.cache = new ArrayList<Pair<ByteArray, Versioned<byte[]>>>();
            this.positioned = false;
        }

        @Override
        public boolean hasNext() {
            // we have a next element if there is at least one cached
            // element or we can make more
            return cache.size() > 0 || makeMore();
        }

        @Override
        public Pair<ByteArray, Versioned<byte[]>> next() {
            if(cache.size() == 0) {
                if(!makeMore())
                    throw new NoSuchElementException("Iterated to end.");
            }
            // must now have at least one thing in the cache
            return cache.remove(cache.size() - 1);
        }

        /**
         * Fetches the next entry from the DB, for the partition
         * 
         * @return true if some new data was fetched, false if end of data
         */
        private boolean makeMore() {
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            OperationStatus status;
            try {
                if(!positioned) {
                    positioned = true;
                    keyEntry.setData(StoreBinaryFormat.makePartitionKey(partition));
                    status = cursor.getSearchKeyRange(keyEntry,
                                                      valueEntry,
                                                      LockMode.READ_UNCOMMITTED);
                } else {
                    status = cursor.getNext(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED);
                }

                if(OperationStatus.NOTFOUND == status) {
                    // we have reached the end of the cursor
                    return false;
                }

                // check if we are still in the same partition we need
                if(StoreBinaryFormat.extractPartition(keyEntry.getData()) != partition)
                    return false;

                ByteArray key = new ByteArray(StoreBinaryFormat.extractKey(keyEntry.getData()));
                for(Versioned<byte[]> val: StoreBinaryFormat.fromByteArray(valueEntry.getData()))
                    this.cache.add(Pair.create(key, val));
                return true;
            } catch(DatabaseException e) {
                bdbEngine.bdbEnvironmentStats.reportException(e);
                logger.error(e);
                throw new PersistenceFailureException(e);
            }
        }
    }

    /**
     * Implements a range scan over the key entries belonging to the partition
     * 
     */
    private static class BdbPartitionKeysIterator extends BdbIterator<ByteArray> {

        ByteArray current = null;
        private int partition;
        private boolean positioned;

        public BdbPartitionKeysIterator(Cursor cursor, int partition, BdbStorageEngine bdbEngine) {
            super(cursor, bdbEngine);
            this.partition = partition;
            positioned = false;
        }

        @Override
        public boolean hasNext() {
            return current != null || fetchNextKey();
        }

        @Override
        public ByteArray next() {
            ByteArray result = null;
            if(current == null) {
                if(!fetchNextKey())
                    throw new NoSuchElementException("Iterated to end.");
            }
            result = current;
            current = null;
            return result;
        }

        /**
         * Fetches the next key for the partition from the DB
         * 
         * @return true if successfully fetched one more key, false if end of
         *         keys
         */
        private boolean fetchNextKey() {
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            OperationStatus status;
            valueEntry.setPartial(true);
            try {
                if(!positioned) {
                    positioned = true;
                    keyEntry.setData(StoreBinaryFormat.makePartitionKey(partition));
                    status = cursor.getSearchKeyRange(keyEntry,
                                                      valueEntry,
                                                      LockMode.READ_UNCOMMITTED);
                } else {
                    status = cursor.getNext(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED);
                }

                if(OperationStatus.NOTFOUND == status) {
                    // we have reached the end of the cursor
                    return false;
                }

                // check if we are still in the same partition we need
                if(StoreBinaryFormat.extractPartition(keyEntry.getData()) != partition)
                    return false;

                current = new ByteArray(StoreBinaryFormat.extractKey(keyEntry.getData()));
                return true;
            } catch(DatabaseException e) {
                bdbEngine.bdbEnvironmentStats.reportException(e);
                logger.error(e);
                throw new PersistenceFailureException(e);
            }
        }
    }

    @Override
    public boolean isPartitionScanSupported() {
        return true;
    }
}
