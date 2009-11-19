package voldemort.store.fs;

import java.io.File;

import junit.framework.TestCase;
import voldemort.TestUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

public class FsKeyAndValuesTest extends TestCase {

    private File tempFile;
    private FsKeyAndValues kv1;
    private FsKeyAndValues kv2;

    @Override
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        this.tempFile = File.createTempFile("vold-test", ".kv");
        ByteArray key = new ByteArray(TestUtils.randomLetters(65).getBytes());
        Versioned<byte[]> v1 = Versioned.value(TestUtils.randomLetters(325).getBytes(),
                                               TestUtils.getClock(1, 1, 2));
        Versioned<byte[]> v2 = Versioned.value(TestUtils.randomLetters(317).getBytes(),
                                               TestUtils.getClock(1, 2, 2));
        this.kv1 = new FsKeyAndValues(key, v1, v2);
        Versioned<byte[]> v3 = Versioned.value(TestUtils.randomLetters(42).getBytes());
        this.kv2 = new FsKeyAndValues(key, v3);
    }

    @Override
    public void tearDown() {
        Utils.rm(tempFile);
    }

    public void testWriteAndRead() throws Exception {
        kv1.writeTo(tempFile);
        FsKeyAndValues found = FsKeyAndValues.read(tempFile);
        assertNotNull(found);
        checkEquals(kv1, found);
    }

    public void testOverwrite() throws Exception {
        kv1.writeTo(tempFile);
        kv2.writeTo(tempFile);
        FsKeyAndValues found = FsKeyAndValues.read(tempFile);
        checkEquals(found, kv2);
    }

    public void testReadNull() throws Exception {
        assertNull(FsKeyAndValues.read(new File("/tmp/tljslkfjsd")));
    }

    private void checkEquals(FsKeyAndValues k1, FsKeyAndValues k2) throws Exception {
        if(k1 == k2)
            return;
        assertNotNull(k1);
        assertNotNull(k2);
        assertEquals("Keys should be equal", k1.getKey(), k2.getKey());
        assertEquals("Should have the same number of values.",
                     k1.getValues().size(),
                     k2.getValues().size());
        for(int i = 0; i < k1.getValues().size(); i++) {
            Versioned<byte[]> v1 = k1.getValues().get(i);
            Versioned<byte[]> v2 = k2.getValues().get(i);
            assertTrue("Values should be equal.",
                       ByteUtils.compare(v1.getValue(), v2.getValue()) == 0);
            assertEquals("Versions should be equal.", v1.getVersion(), v2.getVersion());
        }
    }
}
