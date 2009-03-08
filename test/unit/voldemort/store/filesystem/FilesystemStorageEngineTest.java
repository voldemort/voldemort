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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileDeleteStrategy;

import voldemort.TestUtils;
import voldemort.store.AbstractStoreTest;
import voldemort.store.Store;

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
        File tempDir = TestUtils.getTempDirectory();
        tempDirs.add(tempDir);
        return new FilesystemStorageEngine("test", tempDir.getAbsolutePath());
    }

    @Override
    public List<String> getValues(int numValues) {
        return getStrings(numValues, 8);
    }

}
