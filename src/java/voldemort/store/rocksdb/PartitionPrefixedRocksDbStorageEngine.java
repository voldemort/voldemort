package voldemort.store.rocksdb;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import voldemort.routing.RoutingStrategy;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StoreBinaryFormat;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class PartitionPrefixedRocksDbStorageEngine extends RocksDbStorageEngine {

    private final RoutingStrategy routingStrategy;

    public PartitionPrefixedRocksDbStorageEngine(String storeName,
                                                 RocksDB rdbStore,
                                                 int lockStripes,
                                                 RoutingStrategy routingStrategy,
                                                 boolean enableReadLocks) {
        super(storeName, rdbStore, lockStripes, enableReadLocks);
        this.routingStrategy = routingStrategy;
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(int partition) {
        return new RocksdbEntriesIterator(this.getRocksDB().newIterator(), partition);
    }

    @Override
    public ClosableIterator<ByteArray> keys(int partition) {
        return new RocksdbKeysIterator(this.getRocksDB().newIterator(), partition);
    }

    private ByteArray validateAndConstructKey(ByteArray key) {
        StoreUtils.assertValidKey(key);
        int partition = routingStrategy.getMasterPartition(key.get());
        ByteArray prefixedKey = new ByteArray(StoreBinaryFormat.makePrefixedKey(key.get(),
                                                                                partition));
        return prefixedKey;
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms)
            throws PersistenceFailureException {
        return super.get(validateAndConstructKey(key), transforms);
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws PersistenceFailureException {
        super.put(validateAndConstructKey(key), value, transforms);
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws PersistenceFailureException {
        return super.delete(validateAndConstructKey(key), version);
    }

    @Override
    public boolean isPartitionScanSupported() {
        return true;
    }

    @Override
    public List<Versioned<byte[]>> multiVersionPut(ByteArray key, List<Versioned<byte[]>> values) {
        return super.multiVersionPut(validateAndConstructKey(key), values);
    }

    private static class RocksdbEntriesIterator implements
            ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

        // TODO May need to identify non const methods in the inner Iterator adn
        // provide external synchronization on those if needed

        RocksIterator innerIterator;
        private List<Pair<ByteArray, Versioned<byte[]>>> cache;
        private final int partitionId;

        public RocksdbEntriesIterator(RocksIterator innerIterator, int partition) {
            this.innerIterator = innerIterator;

            this.partitionId = partition;

            // Caller of the RocksIterator should seek it before the first use.
            // seek to the prefix specified
            this.innerIterator.seek(StoreBinaryFormat.makePartitionKey(partition));

            cache = new ArrayList<Pair<ByteArray, Versioned<byte[]>>>();
        }

        @Override
        public boolean hasNext() {
            return cache.size() > 0 || makeMore();
        }

        @Override
        public Pair<ByteArray, Versioned<byte[]>> next() {
            if(cache.size() == 0) {
                if(!makeMore()) {
                    throw new NoSuchElementException("Iterated to end.");
                }
            }
            return cache.remove(cache.size() - 1);
        }

        protected boolean makeMore() {
            if(innerIterator.isValid()) {
                byte[] keyEntry = innerIterator.key();

                // Check if the next key returned starts with the expected
                // prefix
                if(StoreBinaryFormat.extractPartition(keyEntry) != partitionId) {
                    return false;
                }
                byte[] valueEntry = innerIterator.value();
                innerIterator.next();
                ByteArray key = new ByteArray(keyEntry);
                for(Versioned<byte[]> val: StoreBinaryFormat.fromByteArray(valueEntry)) {
                    cache.add(new Pair<ByteArray, Versioned<byte[]>>(key, val));
                }
                return true;
            }
            return false;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("No removal");
        }

        @Override
        public void close() {
            this.innerIterator.dispose();
        }

    }

    private static class RocksdbKeysIterator implements ClosableIterator<ByteArray> {

        // TODO May need to identify non const methods in the inner Iterator adn
        // provide external synchronization on those if needed

        RocksIterator innerIterator;
        private ByteArray cache;
        private final int partitionId;

        public RocksdbKeysIterator(RocksIterator innerIterator, int partition) {
            this.innerIterator = innerIterator;

            this.partitionId = partition;

            // Caller of the RocksIterator should seek it to the prefix before
            // the first use.
            this.innerIterator.seek(StoreBinaryFormat.makePartitionKey(partition));

            cache = null;
        }

        @Override
        public boolean hasNext() {
            return cache != null || fetchnextKey();
        }

        private boolean fetchnextKey() {
            if(this.innerIterator.isValid()) {
                byte[] keyEntry = this.innerIterator.key();

                // Check if the next key returned starts with the expected
                // prefix
                if(StoreBinaryFormat.extractPartition(keyEntry) != partitionId) {
                    return false;
                }

                this.innerIterator.next();
                cache = new ByteArray(keyEntry);
                return true;
            }
            return false;
        }

        @Override
        public ByteArray next() {
            if(cache != null) {
                if(!fetchnextKey()) {
                    throw new NoSuchElementException("Iterate to end");
                }
            }
            return cache;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("No removal");
        }

        @Override
        public void close() {
            this.innerIterator.dispose();
        }
    }
}
