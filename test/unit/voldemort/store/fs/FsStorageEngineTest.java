package voldemort.store.fs;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import voldemort.TestUtils;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;

/**
 * Test suite for the fs storage engine
 * 
 * @author jay
 * 
 */
public class FsStorageEngineTest extends AbstractStorageEngineTest {

    private static final int FAN_OUT = 10;
    private static final int DEPTH = 2;
    private static final int NUM_LOCKS = 10;

    private List<File> baseDirs;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseDirs = Arrays.asList(TestUtils.createTempDir(), TestUtils.createTempDir());
        for(File f: baseDirs)
            Utils.rm(f.listFiles());
    }

    @Override
    public void tearDown() throws Exception {
        super.setUp();
        Utils.rm(this.baseDirs);
    }

    @Override
    public StorageEngine<ByteArray, byte[]> getStorageEngine() {
        return new FsStorageEngine("test", baseDirs, DEPTH, FAN_OUT, NUM_LOCKS);
    }
}
