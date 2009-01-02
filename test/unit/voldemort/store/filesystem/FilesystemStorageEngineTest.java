package voldemort.store.filesystem;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileDeleteStrategy;

import voldemort.TestUtils;
import voldemort.store.BasicStoreTest;
import voldemort.store.Store;

public class FilesystemStorageEngineTest extends BasicStoreTest<String,String> {
    
    private List<File> tempDirs;
    
    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempDirs = new ArrayList<File>();
    }
    
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        for(File file: tempDirs)
            FileDeleteStrategy.FORCE.delete(file);
    }

    @Override
    public List<String> getKeys(int numKeys) {
        return getStrings(numKeys, 10);
    }

    @Override
    public Store<String, String> getStore() {
        File tempDir = TestUtils.getTempDirectory();
        tempDirs.add(tempDir);
        return new FilesystemStorageEngine("test", tempDir.getAbsolutePath());
    }

    @Override
    public List<String> getValues(int numValues) {
        return getStrings(numValues, 8);
    }

}
