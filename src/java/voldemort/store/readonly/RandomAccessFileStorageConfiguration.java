package voldemort.store.readonly;

import java.io.File;

import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StorageEngineType;

public class RandomAccessFileStorageConfiguration implements StorageConfiguration {

    private final int numFileHandles;
    private final int numBackups;
    private final long fileAccessWaitTimeoutMs;
    private final File storageDir;

    public RandomAccessFileStorageConfiguration(VoldemortConfig config) {
        this.numFileHandles = config.getReadOnlyStorageFileHandles();
        this.storageDir = new File(config.getReadOnlyDataStorageDirectory());
        this.fileAccessWaitTimeoutMs = config.getReadOnlyFileWaitTimeoutMs();
        this.numBackups = config.getReadOnlyBackups();
    }

    public void close() {}

    public StorageEngine<byte[], byte[]> getStore(String name) {
        return new RandomAccessFileStore(name,
                                         storageDir,
                                         numBackups,
                                         numFileHandles,
                                         fileAccessWaitTimeoutMs);
    }

    public StorageEngineType getType() {
        return StorageEngineType.READONLY;
    }

}
