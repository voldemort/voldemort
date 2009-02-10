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

package voldemort.store.bdb;

import java.io.File;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.io.FileDeleteStrategy;

import voldemort.TestUtils;
import voldemort.server.VoldemortConfig;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;

/**
 * checks that
 * 
 * @author bbansal
 * 
 */
public class BdbSplitStorageEngineTest extends TestCase {

    private File bdbMasterDir;
    private BdbStorageEngine storeA;
    private BdbStorageEngine storeB;
    private Random random;
    BdbStorageConfiguration bdbStorage;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        bdbMasterDir = TestUtils.getTempDirectory();
        FileDeleteStrategy.FORCE.delete(bdbMasterDir);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        try {
            bdbStorage.close();
        } finally {
            FileDeleteStrategy.FORCE.delete(bdbMasterDir);
        }
    }

    public void testNoMultipleFiles() {
        // lets use all the default values.
        Props props = new Props();
        props.put("node.id", 1);
        props.put("voldemort.home", "test/common/voldemort/config");
        VoldemortConfig voldemortConfig = new VoldemortConfig(props);
        voldemortConfig.setBdbCacheSize(1 * 1024 * 1024);
        voldemortConfig.setBdbDataDirectory(bdbMasterDir.toURI().getPath());

        bdbStorage = new BdbStorageConfiguration(voldemortConfig);
        storeA = (BdbStorageEngine) bdbStorage.getStore("storeA");
        storeB = (BdbStorageEngine) bdbStorage.getStore("storeB");

        storeA.put("testKey1".getBytes(), new Versioned<byte[]>("value".getBytes()));
        storeA.put("testKey2".getBytes(), new Versioned<byte[]>("value".getBytes()));
        storeA.put("testKey3".getBytes(), new Versioned<byte[]>("value".getBytes()));

        storeB.put("testKey1".getBytes(), new Versioned<byte[]>("value".getBytes()));
        storeB.put("testKey2".getBytes(), new Versioned<byte[]>("value".getBytes()));
        storeB.put("testKey3".getBytes(), new Versioned<byte[]>("value".getBytes()));

        storeA.close();
        storeB.close();

        assertEquals("common BDB file should exists.", true, (bdbMasterDir.exists()));

        assertNotSame("StoreA BDB file should not exists.", true, (new File(bdbMasterDir + "/"
                                                                            + "storeA").exists()));
        assertNotSame("StoreB BDB file should not exists.", true, (new File(bdbMasterDir + "/"
                                                                            + "storeB").exists()));
    }

    public void testMultipleFiles() {
        // lets use all the default values.
        Props props = new Props();
        props.put("node.id", 1);
        props.put("voldemort.home", "test/common/voldemort/config");
        VoldemortConfig voldemortConfig = new VoldemortConfig(props);
        voldemortConfig.setBdbCacheSize(1 * 1024 * 1024);
        voldemortConfig.setBdbFilePerStore(true);
        voldemortConfig.setBdbDataDirectory(bdbMasterDir.toURI().getPath());

        bdbStorage = new BdbStorageConfiguration(voldemortConfig);
        storeA = (BdbStorageEngine) bdbStorage.getStore("storeA");
        storeB = (BdbStorageEngine) bdbStorage.getStore("storeB");

        storeA.put("testKey1".getBytes(), new Versioned<byte[]>("value".getBytes()));
        storeA.put("testKey2".getBytes(), new Versioned<byte[]>("value".getBytes()));
        storeA.put("testKey3".getBytes(), new Versioned<byte[]>("value".getBytes()));

        storeB.put("testKey1".getBytes(), new Versioned<byte[]>("value".getBytes()));
        storeB.put("testKey2".getBytes(), new Versioned<byte[]>("value".getBytes()));
        storeB.put("testKey3".getBytes(), new Versioned<byte[]>("value".getBytes()));

        storeA.close();
        storeB.close();

        assertEquals("StoreA BDB file should exists.", true, (new File(bdbMasterDir + "/"
                                                                       + "storeA").exists()));
        assertEquals("StoreB BDB file should  exists.", true, (new File(bdbMasterDir + "/"
                                                                        + "storeB").exists()));
    }
}
