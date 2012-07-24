package voldemort.store.krati;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import krati.array.DataArray;
import krati.core.segment.SegmentFactory;
import krati.store.DynamicDataStore;
import krati.util.FnvHashFunction;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.StripedLock;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class KratiStorageEngine implements StorageEngine<ByteArray, byte[], byte[]> {

    private static final Logger logger = Logger.getLogger(KratiStorageEngine.class);
    private final String name;
    private final DynamicDataStore datastore;
    private final StripedLock locks;

    public KratiStorageEngine(String name,
                              SegmentFactory segmentFactory,
                              int segmentFileSizeMB,
                              int lockStripes,
                              double hashLoadFactor,
                              int initLevel,
                              File dataDirectory) {
        this.name = Utils.notNull(name);
        try {
            this.datastore = new DynamicDataStore(dataDirectory,
                                                  initLevel,
                                                  segmentFileSizeMB,
                                                  segmentFactory,
                                                  hashLoadFactor,
                                                  new FnvHashFunction());
            this.locks = new StripedLock(lockStripes);
        } catch(Exception e) {
            throw new VoldemortException("Failure initializing store.", e);
        }

    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    public String getName() {
        return this.name;
    }

    public void close() throws VoldemortException {}

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return StoreUtils.getAll(this, keys, null);
    }

    public List<Version> getVersions(ByteArray key) {
        return StoreUtils.getVersions(get(key, null));
    }

    public void truncate() {
        try {
            datastore.clear();
        } catch(Exception e) {
            logger.error("Failed to truncate store '" + name + "': ", e);
            throw new VoldemortException("Failed to truncate store '" + name + "'.");
        }
    }

    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        try {
            return disassembleValues(datastore.get(key.get()));
        } catch(Exception e) {
            logger.error("Error reading value: ", e);
            throw new VoldemortException("Error reading value: ", e);
        }
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        List<Pair<ByteArray, Versioned<byte[]>>> returnedList = new ArrayList<Pair<ByteArray, Versioned<byte[]>>>();
        DataArray array = datastore.getDataArray();
        for(int index = 0; index < array.length(); index++) {
            byte[] returnedBytes = array.get(index);
            if(returnedBytes != null) {
                // Extract the key value pair from this
                // TODO: Move to DynamicDataStore code
                ByteBuffer bb = ByteBuffer.wrap(returnedBytes);
                int cnt = bb.getInt();
                if(cnt > 0) {
                    int keyLen = bb.getInt();
                    byte[] key = new byte[keyLen];
                    bb.get(key);

                    int valueLen = bb.getInt();
                    byte[] value = new byte[valueLen];
                    bb.get(value);

                    List<Versioned<byte[]>> versions;
                    try {
                        versions = disassembleValues(value);
                    } catch(IOException e) {
                        versions = null;
                    }

                    if(versions != null) {
                        Iterator<Versioned<byte[]>> iterVersions = versions.iterator();
                        while(iterVersions.hasNext()) {
                            Versioned<byte[]> currentVersion = iterVersions.next();
                            returnedList.add(Pair.create(new ByteArray(key), currentVersion));
                        }
                    }
                }
            }
        }
        return new KratiClosableIterator(returnedList);
    }

    public ClosableIterator<ByteArray> keys() {
        return StoreUtils.keys(entries());
    }

    public boolean delete(ByteArray key, Version maxVersion) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        Lock lock = this.locks.lockFor(key.get());
        try {
            lock.lock();
            if(maxVersion == null) {
                try {
                    return datastore.delete(key.get());
                } catch(Exception e) {
                    logger.error("Failed to delete key: ", e);
                    throw new VoldemortException("Failed to delete key: " + key, e);
                }
            }

            List<Versioned<byte[]>> returnedValuesList = this.get(key, null);

            // Case if there is nothing to delete
            if(returnedValuesList.size() == 0) {
                return false;
            }

            Iterator<Versioned<byte[]>> iter = returnedValuesList.iterator();
            while(iter.hasNext()) {
                Versioned<byte[]> currentValue = iter.next();
                Version currentVersion = currentValue.getVersion();
                if(currentVersion.compare(maxVersion) == Occurred.BEFORE) {
                    iter.remove();
                }
            }

            try {
                if(returnedValuesList.size() == 0)
                    return datastore.delete(key.get());
                else
                    return datastore.put(key.get(), assembleValues(returnedValuesList));
            } catch(Exception e) {
                String message = "Failed to delete key " + key;
                logger.error(message, e);
                throw new VoldemortException(message, e);
            }
        } finally {
            lock.unlock();
        }
    }

    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);

        Lock lock = this.locks.lockFor(key.get());
        try {
            lock.lock();
            // First get the value
            List<Versioned<byte[]>> existingValuesList = this.get(key, null);

            // If no value, add one
            if(existingValuesList.size() == 0) {
                existingValuesList = new ArrayList<Versioned<byte[]>>();
                existingValuesList.add(new Versioned<byte[]>(value.getValue(), value.getVersion()));
            } else {

                // Update the value
                List<Versioned<byte[]>> removedValueList = new ArrayList<Versioned<byte[]>>();
                for(Versioned<byte[]> versioned: existingValuesList) {
                    Occurred occurred = value.getVersion().compare(versioned.getVersion());
                    if(occurred == Occurred.BEFORE)
                        throw new ObsoleteVersionException("Obsolete version for key '" + key
                                                           + "': " + value.getVersion());
                    else if(occurred == Occurred.AFTER)
                        removedValueList.add(versioned);
                }
                existingValuesList.removeAll(removedValueList);
                existingValuesList.add(value);
            }

            try {
                datastore.put(key.get(), assembleValues(existingValuesList));
            } catch(Exception e) {
                String message = "Failed to put key " + key;
                logger.error(message, e);
                throw new VoldemortException(message, e);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Store the versioned values
     * 
     * @param values list of versioned bytes
     * @return the list of versioned values rolled into an array of bytes
     */
    private byte[] assembleValues(List<Versioned<byte[]>> values) throws IOException {

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream dataStream = new DataOutputStream(stream);

        for(Versioned<byte[]> value: values) {
            byte[] object = value.getValue();
            dataStream.writeInt(object.length);
            dataStream.write(object);

            VectorClock clock = (VectorClock) value.getVersion();
            dataStream.writeInt(clock.sizeInBytes());
            dataStream.write(clock.toBytes());
        }

        return stream.toByteArray();
    }

    /**
     * Splits up value into multiple versioned values
     * 
     * @param value
     * @return
     * @throws IOException
     */
    private List<Versioned<byte[]>> disassembleValues(byte[] values) throws IOException {

        if(values == null)
            return new ArrayList<Versioned<byte[]>>(0);

        List<Versioned<byte[]>> returnList = new ArrayList<Versioned<byte[]>>();
        ByteArrayInputStream stream = new ByteArrayInputStream(values);
        DataInputStream dataStream = new DataInputStream(stream);

        while(dataStream.available() > 0) {
            byte[] object = new byte[dataStream.readInt()];
            dataStream.read(object);

            byte[] clockBytes = new byte[dataStream.readInt()];
            dataStream.read(clockBytes);
            VectorClock clock = new VectorClock(clockBytes);

            returnList.add(new Versioned<byte[]>(object, clock));
        }

        return returnList;
    }

    private class KratiClosableIterator implements
            ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

        private Iterator<Pair<ByteArray, Versioned<byte[]>>> iter;

        public KratiClosableIterator(List<Pair<ByteArray, Versioned<byte[]>>> list) {
            iter = list.iterator();
        }

        public void close() {
            // Nothing to close here
        }

        public boolean hasNext() {
            return iter.hasNext();
        }

        public Pair<ByteArray, Versioned<byte[]>> next() {
            return iter.next();
        }

        public void remove() {
            Pair<ByteArray, Versioned<byte[]>> currentPair = next();
            delete(currentPair.getFirst(), currentPair.getSecond().getVersion());
        }

    }

    public boolean isPartitionAware() {
        return false;
    }
}
