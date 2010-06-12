package voldemort.store.krati;

import java.io.File;

import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;

public class KratiStorageEngineTest extends AbstractStorageEngineTest {

    private StorageEngine<ByteArray, byte[], byte[]> store = null;

    protected void setUp() throws Exception {
        super.setUp();
        File storeDir = new File("/blah");
        storeDir.mkdirs();
        storeDir.deleteOnExit();
        this.store = new KratiStorageEngine("blah", 256, 0.75, 0, storeDir);
    }

    @Override
    public StorageEngine<ByteArray, byte[], byte[]> getStorageEngine() {
        return this.store;
    }

    @Override
    public void tearDown() {
        store.truncate();
    }

}
