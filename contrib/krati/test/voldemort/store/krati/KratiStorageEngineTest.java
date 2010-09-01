package voldemort.store.krati;

import java.io.File;

import krati.cds.impl.segment.MappedSegmentFactory;
import voldemort.TestUtils;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;

public class KratiStorageEngineTest extends AbstractStorageEngineTest {

    private StorageEngine<ByteArray, byte[], byte[]> store = null;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        File storeDir = TestUtils.createTempDir();
        storeDir.mkdirs();
        storeDir.deleteOnExit();
        this.store = new KratiStorageEngine("storeName",
                                            new MappedSegmentFactory(),
                                            10,
                                            10,
                                            0.75,
                                            0,
                                            storeDir);
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
