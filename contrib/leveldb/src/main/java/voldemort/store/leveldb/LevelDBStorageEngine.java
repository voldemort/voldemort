package voldemort.store.leveldb;

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

import voldemort.store.leveldb.leveldbjni.DB;
import voldemort.store.leveldb.leveldbjni.DbOptions;
import voldemort.store.leveldb.leveldbjni.ReadOptions;
import voldemort.store.leveldb.leveldbjni.WriteOptions;
import voldemort.store.leveldb.leveldbjni.DbIterator;
import com.google.common.collect.Lists;
import voldemort.VoldemortException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.StripedLock;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;
import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.Serializer;
import voldemort.serialization.VersionedSerializer;

public class LevelDBStorageEngine implements StorageEngine<ByteArray, byte[], byte[]> {
    
    private final String name;
    private final String path;
    private DB dbstore;
    private final StripedLock locks;
    private DbOptions dbo ;
    private ReadOptions ropts;
    private WriteOptions wopts;
    private final VersionedSerializer<byte[]> versionedSerializer;
    private int poolsize = 4;
    private boolean iterators = true;

    public LevelDBStorageEngine(String name, String path, int lockStripes, 
        DbOptions dboptions, ReadOptions roptions, WriteOptions woptions,
        int poolsize, boolean iterators)
    {
        this.name = Utils.notNull(name);
        this.path = Utils.notNull(path);
        this.versionedSerializer = new VersionedSerializer<byte[]>(new IdentitySerializer());
        try{
            this.dbo = dboptions;
            this.ropts = roptions;
            this.wopts = woptions;
            this.poolsize = poolsize;
            this.iterators = iterators;
            this.dbstore = new DB(dbo, path,poolsize,iterators);
            this.locks = new StripedLock(lockStripes);
        } catch (Exception e) {
            throw new VoldemortException("Failure initializing store!", e);
        }
    }

    public String getName() {
        return this.name;
    }

    public String getPath() {
        return this.path;
    }

    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        try {
            return disassembleValues(dbstore.get(ropts, key.get()));
        } catch(Exception e) {
            throw new VoldemortException("Error reading value: ", e);
        }
    }

    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms) 
        throws VoldemortException{
        StoreUtils.assertValidKey(key);

        synchronized(this.locks.lockFor(key.get())) {
            List<Versioned<byte[]>> existingValueList = 
                this.get(key, null);

            if(existingValueList.size() == 0) {
                existingValueList = new ArrayList<Versioned<byte[]>>();
                existingValueList.add(new Versioned<byte[]>(value.getValue(), value.getVersion()));
            } else {
                List<Versioned<byte[]>> removedValueList = new ArrayList<Versioned<byte[]>>();
                for(Versioned<byte[]> versioned: existingValueList) {
                    Occurred Occurred = value.getVersion().compare(versioned.getVersion());
                    if(Occurred == Occurred.BEFORE)
                        throw new ObsoleteVersionException("Obsolete version for key:[" + key + ":" + value.getVersion()+"]");
                    else if (Occurred == Occurred.AFTER)
                        removedValueList.add(versioned);
                }
                existingValueList.removeAll(removedValueList);
                existingValueList.add(value);
            }
            
            int s = 0;
            try{
                s = dbstore.put(wopts, key.get(), assembleValues(existingValueList));
            } catch(Exception e) {
                String message = "Failed to put key" + key;
                throw new VoldemortException(message, e);
            }
            if(s<=0) throw new PersistenceFailureException("Put operation failed with status: " + s);
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

    public boolean isPartitionAware() {
        return false;
    }

    public void truncate() {
        try {
            dbstore.close();
            dbstore.destroyDB(dbo, path);
            dbstore = new DB(dbo, path,poolsize,iterators);
        } catch(Exception e) {
            throw new VoldemortException("Failed to truncate store '" + name + "'.");
        }
    }

    public ClosableIterator<ByteArray> keys() {
        DbIterator it = dbstore.newIterator(ropts);
        return new LevelDBKeysIterator(it);
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        DbIterator it = dbstore.newIterator(ropts);
        return new LevelDBEntriesIterator(it);
    }

    public List<Version> getVersions(ByteArray key) {
        return StoreUtils.getVersions(get(key, null));
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    public void close() {
        dbstore.close();
    }

    public boolean delete(ByteArray key, Version maxVersion) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        synchronized(this.locks.lockFor(key.get())) {
            if(maxVersion == null) {
                try {
                    int d = dbstore.delete(wopts,key.get());
                    return 0 < d;
                } catch(Exception e) {
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
                if(returnedValuesList.size() == 0) {
                    return 0 < dbstore.delete(wopts,key.get());
                } else
                    return 0 < dbstore.put(wopts, key.get(), assembleValues(returnedValuesList));
            } catch(Exception e) {
                throw new VoldemortException("Failed to delete key " + key, e);
            }
        }
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return StoreUtils.getAll(this, keys, null);
    }


    private class LevelDBKeysIterator extends LevelDBClosableIterator<ByteArray>
    {

        public LevelDBKeysIterator(DbIterator list) {
            super(list);
        }
        @Override
        protected ByteArray get(byte[] key, byte[] value){
            try{
                if(value!=null){
                    le = disassembleValues(value);
                    versionLeft = le.size();
                }
                versionLeft--;
                return new ByteArray(key);
            }catch(Exception e) {return null;}
        }
    }
    private class LevelDBEntriesIterator extends LevelDBClosableIterator<Pair<ByteArray, Versioned<byte[]>>>
    {

        public LevelDBEntriesIterator(DbIterator list) {
            super(list);
        }
        
        @Override
        protected Pair<ByteArray, Versioned<byte[]>> get(byte[] key, byte[] value){
            
            try{
                if(value!=null){
                    le = disassembleValues(value);
                    versionLeft = le.size();
                }
                versionLeft--;
                return Pair.create(new ByteArray(key), le.get(versionLeft));
            }catch(Exception e) {return null;}
        }
    }
    private static abstract class LevelDBClosableIterator<T> implements
            ClosableIterator<T> {

        private DbIterator iter;
        protected int versionLeft = -1;
        List<Versioned<byte[]>> le;

        public LevelDBClosableIterator(DbIterator list) {
            iter = list;
            iter.seekToFirst();
        }

        public void close() {
            iter.close();
        }

        public boolean hasNext() {
            if(versionLeft < 0)
                return iter.hasNext();
            else if(versionLeft > 0)
                return true;
            else {
                iter.next();
                boolean n = iter.hasNext();
                return n;
               }
        }
        protected abstract T get(byte[] key, byte[] value);
        public T next() {
            if(versionLeft <= 0){
                return get(iter.key(),iter.value());
            }else
                return get(iter.key(),null);
        }

        public void remove() {
            throw new UnsupportedOperationException("No removal y'all.");
        }

    }




}