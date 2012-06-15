/*
 * Copyright 2008-2012 LinkedIn, Inc
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

package voldemort.store.leveldb;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;

import org.apache.commons.io.FileDeleteStrategy;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import voldemort.TestUtils;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;

public class LevelDbStorageEngineTest extends AbstractStorageEngineTest {

    private File tempDir;
    private LevelDbStorageEngine store;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.tempDir = TestUtils.createTempDir();
        Options options = new Options();
        options.createIfMissing(true);
        DB db = factory.open(tempDir, options);
        store = new LevelDbStorageEngine("test", db, options, tempDir, 5);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        try {
            store.close();
        } finally {
            FileDeleteStrategy.FORCE.delete(tempDir);
        }
    }

    @Override
    public StorageEngine<ByteArray, byte[], byte[]> getStorageEngine() {
        return store;
    }

}
