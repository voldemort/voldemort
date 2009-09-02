/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.filesystem;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileDeleteStrategy;

import voldemort.TestUtils;
import voldemort.store.AbstractStoreTest;
import voldemort.store.Store;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class FilesystemStorageEngineTest extends AbstractStoreTest<String, String> {

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
        File tempDir = TestUtils.createTempDir();
        tempDirs.add(tempDir);
        return new ConfiguratinStorageEngine("test", tempDir.getAbsolutePath());
    }

    @Override
    public List<String> getValues(int numValues) {
        return getStrings(numValues, 8);
    }

    public void testEmacsTempFile() throws IOException {
        Store<String, String> store = getStore();
        assertEquals("Only one tempDir should be present", 1, tempDirs.size());
        String keyName = "testkey.xml";

        store.put(keyName, new Versioned<String>("testValue"));
        assertEquals("Only one file of name key should be present.", 1, store.get(keyName).size());

        // Now create a emacs style temp file
        new File(tempDirs.get(0), keyName + "#").createNewFile();
        new File(tempDirs.get(0), "#" + keyName + "#").createNewFile();
        new File(tempDirs.get(0), keyName + "~").createNewFile();
        new File(tempDirs.get(0), "." + keyName + "~").createNewFile();

        assertEquals("Only one file of name key should be present.", 1, store.get(keyName).size());

        // do a new put
        store.put(keyName, new Versioned<String>("testValue1"));
        assertEquals("Only one file of name key should be present.", 1, store.get(keyName).size());
        assertEquals("Value should match.", "testValue1", store.get(keyName).get(0).getValue());

        // try getAll
        Map<String, List<Versioned<String>>> map = store.getAll(Arrays.asList(keyName));
        assertEquals("Only one file of name key should be present.", 1, map.get(keyName).size());
        assertEquals("Value should match.", "testValue1", map.get(keyName).get(0).getValue());

        // try delete
        store.delete(keyName, new VectorClock());
        assertEquals("No file of name key should be present.", 0, store.get(keyName).size());
    }
}
