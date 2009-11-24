package voldemort.store.fs;

import java.io.File;
import java.util.Arrays;

import junit.framework.TestCase;
import voldemort.TestUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;

public class FsKeyPathTest extends TestCase {

    private static final int fanOut = 5;
    private static final int depth = 2;
    private static final ByteArray key = new ByteArray("abc".getBytes());

    private FsKeyPath path;
    private File[] dirs;

    @Override
    public void setUp() {
        dirs = new File[] { TestUtils.createTempDir(), TestUtils.createTempDir() };
        path = FsKeyPath.forKey(key, dirs, depth, fanOut, 10);
    }

    @Override
    public void tearDown() {
        Utils.rm(Arrays.asList(dirs));
    }

    public void testPathStartsWithBaseDir() {
        boolean found = false;
        for(File f: dirs)
            if(path.getPath().startsWith(f.getAbsolutePath()))
                found = true;
        assertTrue("Key should be in one of the root directories.", found);
    }

    public void testFanoutDirNameLength() {
        dirs[0].getAbsolutePath();
    }
}
