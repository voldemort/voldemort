package voldemort.store.fs;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;

/**
 * Factory for making fs stores
 * 
 * @author jay
 * 
 */
public class FsStorageConfiguration implements StorageConfiguration {

    public static final String TYPE_NAME = "fs";

    private final List<File> baseDirs;
    private final VoldemortConfig config;

    public FsStorageConfiguration(VoldemortConfig config) {
        this.config = config;
        this.baseDirs = new ArrayList<File>();
        for(String dir: config.getFsStorageDirs()) {
            File d = new File(dir);
            if(!d.exists())
                d.mkdirs();
            if(!d.isDirectory() || !d.canRead() || !d.canWrite())
                throw new IllegalStateException(d.getAbsolutePath()
                                                + " is not a directory that can be read from and written to.");
            baseDirs.add(d);
        }
    }

    public void close() {}

    public StorageEngine<ByteArray, byte[]> getStore(String name) {
        // make sub-directories for the store in each base directory (if they
        // don't already exist)
        List<File> storeDirs = new ArrayList<File>();
        for(File baseDir: baseDirs) {
            File storeDir = new File(baseDir, name);
            storeDir.mkdirs();
            if(!storeDir.exists())
                throw new IllegalStateException("Failed to create subdirectory for store '" + name
                                                + "' in " + storeDir.getAbsolutePath());
            storeDirs.add(storeDir);
        }

        return new FsStorageEngine(name,
                                   storeDirs,
                                   config.getFsStorageDirDepth(),
                                   config.getFsStorageDirFanOut(),
                                   config.getFsStorageNumLockStripes());
    }

    public String getType() {
        return TYPE_NAME;
    }

}
