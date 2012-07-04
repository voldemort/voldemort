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

package voldemort.store.bdb;

import java.io.File;

import junit.framework.TestCase;

import org.apache.commons.io.FileDeleteStrategy;

import voldemort.TestUtils;
import voldemort.server.VoldemortConfig;
import voldemort.store.StoreDefinition;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;

/**
 * checks that BDB cache partitioning works and caches stay within limits
 * 
 */
public class BdbCachePartitioningTest extends TestCase {

    private File bdbMasterDir;
    private BdbStorageConfiguration bdbStorage;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        bdbMasterDir = TestUtils.createTempDir();
        FileDeleteStrategy.FORCE.delete(bdbMasterDir);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        try {
            if(bdbStorage != null)
                bdbStorage.close();
        } finally {
            FileDeleteStrategy.FORCE.delete(bdbMasterDir);
        }
    }

    /**
     * Tests that, given no data completely fits in memory (realistic prod
     * conditions), stores will stay within their limits, no matter how much
     * disproportinate traffic you throw at it
     */
    public void testStaticPrivateCaches() {

        int totalCache = 20 * 1024 * 1024; // total cache size
        int shareA = 10 * 1024 * 1024;// A reserves 10MB
        int shareB = 5 * 1024 * 1024;// B reserves 5MB
        int shareC = totalCache - shareA - shareB; // the rest, 5 MB
        int numRecords = 40;

        // lets use all the default values.
        Props props = new Props();
        props.put("node.id", 1);
        props.put("voldemort.home", "test/common/voldemort/config");
        VoldemortConfig voldemortConfig = new VoldemortConfig(props);
        voldemortConfig.setBdbCacheSize(totalCache);
        voldemortConfig.setBdbOneEnvPerStore(true);
        voldemortConfig.setBdbDataDirectory(bdbMasterDir.toURI().getPath());

        bdbStorage = new BdbStorageConfiguration(voldemortConfig);
        StoreDefinition defA = TestUtils.makeStoreDefinition("storeA", shareA / (1024 * 1024));
        BdbStorageEngine storeA = (BdbStorageEngine) bdbStorage.getStore(defA);

        StoreDefinition defB = TestUtils.makeStoreDefinition("storeB", shareB / (1024 * 1024));
        BdbStorageEngine storeB = (BdbStorageEngine) bdbStorage.getStore(defB);

        StoreDefinition defC = TestUtils.makeStoreDefinition("storeC");
        BdbStorageEngine storeC = (BdbStorageEngine) bdbStorage.getStore(defC);

        // load data into the stores; each store is guaranteed to be ~ 40MB.
        // Data won't fit in memory
        byte[] value = new byte[1024 * 1024];
        for(int i = 0; i < numRecords; i++) {
            storeA.put(TestUtils.toByteArray("testKey" + i), new Versioned<byte[]>(value), null);
            storeB.put(TestUtils.toByteArray("testKey" + i), new Versioned<byte[]>(value), null);
            storeC.put(TestUtils.toByteArray("testKey" + i), new Versioned<byte[]>(value), null);
        }

        // we will bring all of that data into the cache, by doing a keywalk.
        // This should expand the cache as much as possible
        for(int i = 0; i < numRecords; i++) {
            storeA.get(TestUtils.toByteArray("testKey" + i), null);
            storeB.get(TestUtils.toByteArray("testKey" + i), null);
            storeC.get(TestUtils.toByteArray("testKey" + i), null);
        }

        long cacheSizeA = bdbStorage.getEnvironment(defA).getConfig().getCacheSize();
        long cacheSizeB = bdbStorage.getEnvironment(defB).getConfig().getCacheSize();
        long cacheSizeC = bdbStorage.getEnvironment(defC).getConfig().getCacheSize();

        // check that they are certainly equal to expected limits. This should
        // be true since the cache would definitely expand enough
        assertTrue(cacheSizeA >= shareA);
        assertTrue(cacheSizeB >= shareB);
        assertTrue(cacheSizeC >= shareC);

        // check that they are not exceedingly high than their limits. Small
        // overflows are expected. But should not be more than a 1MB
        assertTrue((cacheSizeA - (shareA)) <= (1024 * 1024));
        assertTrue((cacheSizeB - (shareB)) <= (1024 * 1024));
        assertTrue((cacheSizeC - (shareC)) <= (1024 * 1024));

        // try doing reads on store C alone, for which we have no reservations.
        // The other stores should not shrink. This simulates a spike on one
        // store
        for(int cycle = 0; cycle < 2; cycle++) {
            for(int i = 0; i < numRecords; i++) {
                storeC.get(TestUtils.toByteArray("testKey" + i), null);
            }
        }

        long cacheSizeANow = bdbStorage.getEnvironment(defA).getConfig().getCacheSize();
        long cacheSizeBNow = bdbStorage.getEnvironment(defB).getConfig().getCacheSize();

        assertTrue(cacheSizeA == cacheSizeANow);
        assertTrue(cacheSizeB == cacheSizeBNow);

        storeA.close();
        storeB.close();
        storeC.close();
    }
}
