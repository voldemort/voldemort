package voldemort.store.fs;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.DirectoryIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occured;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.AbstractIterator;

/**
 * A storage engine aimed at storing large values efficiently. The data layout
 * is as follows. All key-value pairs for a given key are stored in a single
 * file in the following format:
 * <ul>
 * <li>4 byte integer key length with value K</li>
 * <li>K byte key</li>
 * <li>2 byte value count with value C</li>
 * <li>C values in the following format:</li>
 * <li>
 * <ul>
 * <li>4 byte integer value length with value V</li>
 * <li>V byte value</li>
 * </ul>
 * </li>
 * </ul>
 * 
 * These files are organized into hierarchical directories. The purpose of the
 * directories is (1) to allow the files to be scattered over multiple hard
 * drives without necessetating raid and (2) to break what would be a large
 * directory of files into a hierarchy of directories each with no more than a
 * few thousand entries.
 * 
 * To locate the correct file containing the values for a given key the
 * following algorithm is used:
 * <ul>
 * <li>compute the digest of the key (e.g. sha-1)</li>
 * <li>compute the base directory containing the key by taking the first 2 bytes
 * as an unsigned integer mod the number of base directories to give an index i.
 * i is the index of the base directory in the array of all base directories in
 * alphabetical order.</li>
 * <li>compute the Nth subdirectory by taking the two bytes beginning at byte 2N
 * mod the fanOut</li>
 * <li>the actual values will be stored in a file named with the full hex
 * encoding of the digest</li>
 * </ul>
 * 
 * @author jay
 * 
 */
public class FsStorageEngine implements StorageEngine<ByteArray, byte[]> {

    private static final Logger logger = Logger.getLogger(FsStorageEngine.class);

    /* base directories in which to find the files */
    private final File[] baseDirs;

    /* The number of directories */
    private final int dirDepth;

    /* The string length of a directory name */
    private final int dirLength;

    /* The number of sub-directories within a directory */
    private final int fanOut;

    /* Striped locks for controlling access to individual keys */
    private final Object[] locks;

    /* The name of the store */
    private final String name;

    public FsStorageEngine(String name, List<File> baseDirs, int dirDepth, int fanOut, int numLocks) {
        this.name = Utils.notNull(name);
        this.dirDepth = Utils.inRange(dirDepth, 0, 3);
        this.fanOut = Utils.inRange(fanOut, 0, Integer.MAX_VALUE);
        this.dirLength = (int) Math.ceil(Math.log10(fanOut));
        this.locks = new Object[numLocks];
        for(int i = 0; i < numLocks; i++)
            this.locks[i] = new Object();
        this.baseDirs = new File[Utils.notNull(baseDirs).size()];
        Utils.inRange(baseDirs.size(), 1, Integer.MAX_VALUE);
        for(int i = 0; i < this.baseDirs.length; i++)
            this.baseDirs[i] = baseDirs.get(i);
        preallocateDirs(this.baseDirs);
    }

    /**
     * Attempt to precreate all directories. This is an optional optimization as
     * missing directories will be created on the fly
     */
    private void preallocateDirs(File[] baseDirs) {
        for(File baseDir: baseDirs) {
            File precreateDone = new File(baseDir, ".precreate-done");
            if(!precreateDone.exists()) {
                logger.info("Creating sub-directories for " + baseDir + "...");
                preallocateDirs(baseDir, this.dirDepth);
                try {
                    precreateDone.createNewFile();
                } catch(IOException e) {
                    logger.error("Error creating marker file for precreation.", e);
                }
            }
        }
    }

    private void preallocateDirs(File baseDir, int depth) {
        NumberFormat nf = NumberFormat.getPercentInstance();
        nf.setMinimumFractionDigits(1);
        double lastPercent = 0.0d;
        if(depth > 0) {
            for(int i = 0; i < fanOut; i++) {
                File dir = new File(baseDir, String.format("%0" + dirLength + "d", i));
                dir.mkdir();
                preallocateDirs(dir, depth - 1);
                double percentDone = i / (double) fanOut;
                if(depth == this.dirDepth && percentDone > lastPercent + 0.01) {
                    logger.info(nf.format(percentDone) + " done");
                    lastPercent = percentDone;
                }
            }
        }
    }

    public void close() throws VoldemortException {}

    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        FsKeyPath path = makePath(key);
        synchronized(lockFor(path)) {
            try {
                File file = new File(path.getPath());
                FsKeyAndValues kv = FsKeyAndValues.read(file);
                if(kv == null)
                    return false;

                // prune obsolete versions
                List<Versioned<byte[]>> remaining = new ArrayList<Versioned<byte[]>>(kv.getValues()
                                                                                       .size());
                for(Versioned<byte[]> v: kv.getValues())
                    if(v.getVersion().compare(version) != Occured.BEFORE)
                        remaining.add(v);

                // if everything was deleted, remove the file
                if(remaining.size() == 0) {
                    boolean deleted = file.delete();
                    if(!deleted)
                        logger.warn("Error deleting key-value file " + file.getAbsolutePath() + ".");
                    return true;
                } else {
                    // was anything deleted at all?
                    boolean deletedSome = remaining.size() < kv.getValues().size();

                    // if something, but not everything, was deleted we need to
                    // update the file
                    if(deletedSome)
                        new FsKeyAndValues(kv.getKey(), remaining).writeTo(file);

                    // return true iff anything was deleted
                    return deletedSome;
                }
            } catch(IOException e) {
                logger.error("Filesystem error in delete.", e);
                throw new PersistenceFailureException("Filesystem error while deleting values.", e);
            }
        }
    }

    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        FsKeyPath path = makePath(key);
        synchronized(lockFor(path)) {
            try {
                FsKeyAndValues kv = FsKeyAndValues.read(new File(path.getPath()));
                if(kv == null)
                    return Collections.emptyList();
                else
                    return kv.getValues();
            } catch(IOException e) {
                logger.error("Filesystem error in get.", e);
                throw new PersistenceFailureException("Filesystem error while reading values.", e);
            }
        }
    }

    /**
     * Get all the keys at once
     * 
     * @param keys The keys to get
     */
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return StoreUtils.getAll(this, keys);
    }

    public Object getCapability(StoreCapabilityType capability) {
        return null;
    }

    public String getName() {
        return name;
    }

    public List<Version> getVersions(ByteArray key) {
        StoreUtils.assertValidKey(key);
        return StoreUtils.getVersions(get(key));
    }

    public void put(ByteArray key, Versioned<byte[]> value) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        FsKeyPath path = makePath(key);
        File file = new File(path.getPath());
        synchronized(lockFor(path)) {
            try {
                // read existing values
                FsKeyAndValues found = FsKeyAndValues.read(file);

                // create a set of pruned values including the new value
                FsKeyAndValues updated;
                if(found == null) {
                    updated = new FsKeyAndValues(key, Collections.singletonList(value));
                } else {
                    // prune any values this value would make obsolete and check
                    // if the value being put is itself obsolete
                    List<Versioned<byte[]>> pruned = new ArrayList<Versioned<byte[]>>(found.getValues()
                                                                                           .size());
                    for(Versioned<byte[]> v: found.getValues()) {
                        Occured occured = value.getVersion().compare(v.getVersion());
                        if(occured == Occured.BEFORE)
                            throw new ObsoleteVersionException(value.getVersion().toString()
                                                               + " is obsolete, it is no greater than the current version of "
                                                               + v.getVersion() + ".");
                        else if(occured == Occured.CONCURRENTLY)
                            pruned.add(v);
                    }
                    pruned.add(value);
                    updated = new FsKeyAndValues(key, pruned);
                }

                // write back the updated values
                updated.writeTo(file);
            } catch(IOException e) {
                logger.error("Filesystem error in put.", e);
                throw new PersistenceFailureException("Filesystem error while reading values.", e);
            }
        }
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        return new FsStorageEngineIterator();
    }

    /**
     * An iterator over all the files in this store that reads the values in the
     * files and turns them into key/value pairs.
     * 
     */
    private class FsStorageEngineIterator extends
            AbstractIterator<Pair<ByteArray, Versioned<byte[]>>> implements
            ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

        public final Stack<Pair<ByteArray, Versioned<byte[]>>> stack = new Stack<Pair<ByteArray, Versioned<byte[]>>>();
        public final DirectoryIterator iter = new DirectoryIterator(baseDirs);

        @Override
        protected Pair<ByteArray, Versioned<byte[]>> computeNext() {
            while(true) {
                if(stack.size() > 0)
                    return stack.pop();

                if(!iter.hasNext())
                    return endOfData();

                File file = iter.next();
                try {
                    synchronized(lockFor(file.getAbsolutePath())) {
                        FsKeyAndValues kv = FsKeyAndValues.read(file);
                        if(kv != null) {
                            for(Versioned<byte[]> v: kv.getValues())
                                stack.push(Pair.create(kv.getKey(), v));
                        }
                    }
                } catch(IOException e) {
                    throw new VoldemortException("Error in iteration.", e);
                }
            }
        }

        public void close() {}
    }

    /* Get the lock for the given path object */
    private Object lockFor(FsKeyPath path) {
        return lockFor(path.getPath());
    }

    /*
     * Get the lock for the given path. Note that we do not cannonicalize the
     * path to keep this cheap. Hopefully since we always create it the same way
     * this is not necessary.
     */
    private Object lockFor(String path) {
        return this.locks[Math.abs(path.hashCode()) % this.locks.length];
    }

    /* Create the path object for the given key */
    private FsKeyPath makePath(ByteArray key) {
        return FsKeyPath.forKey(key, baseDirs, dirDepth, fanOut, dirLength);
    }

}
