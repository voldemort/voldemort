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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.io.FileDeleteStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.TestUtils;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageInitializationException;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteUtils;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;

/**
 * checks that BDB cache partitioning works and caches stay within limits
 * 
 */
@RunWith(Parameterized.class)
public class BdbCachePartitioningTest {

    private File bdbMasterDir;
    private BdbStorageConfiguration bdbStorage;
    private boolean prefixPartitionId;

    public BdbCachePartitioningTest(boolean prefixPartitionId) {
        this.prefixPartitionId = prefixPartitionId;
    }

    @Parameters
    public static Collection<Object[]> modes() {
        Object[][] data = new Object[][] { { true }, { false } };
        return Arrays.asList(data);
    }

    @Before
    public void setUp() throws Exception {
        bdbMasterDir = TestUtils.createTempDir();
        FileDeleteStrategy.FORCE.delete(bdbMasterDir);
    }

    @After
    public void tearDown() throws Exception {
        FileDeleteStrategy.FORCE.delete(bdbMasterDir);
    }

    private EnvironmentStats getStats(Environment environment) {
        StatsConfig config = new StatsConfig();
        config.setFast(true);
        return environment.getStats(config);
    }

    private long getAndCheckCacheSize(BdbStorageEngine engine, StoreDefinition storeDef, String key) {
        engine.get(TestUtils.toByteArray(key), null);
        return getStats(bdbStorage.getEnvironment(storeDef)).getCacheTotalBytes();
    }

    private long getCacheSize(StoreDefinition storeDef) {
        return getStats(bdbStorage.getEnvironment(storeDef)).getCacheTotalBytes();
    }

    /**
     * Tests that, given no data completely fits in memory (realistic prod
     * conditions), stores will stay within their limits, no matter how much
     * disproportinate traffic you throw at it
     */
    @Test
    public void testStaticPrivateCaches() {

        int totalCache = 20 * ByteUtils.BYTES_PER_MB; // total cache size
        int shareA = 10 * ByteUtils.BYTES_PER_MB;// A reserves 10MB
        int shareB = 5 * ByteUtils.BYTES_PER_MB;// B reserves 5MB
        int shareC = totalCache - shareA - shareB; // the rest, 5 MB
        int numRecords = 40;

        BdbStorageEngine storeA = null, storeB = null, storeC = null;
        try {
            // lets use all the default values.
            Props props = new Props();
            props.put("node.id", 1);
            props.put("voldemort.home", "test/common/voldemort/config");
            VoldemortConfig voldemortConfig = new VoldemortConfig(props);
            voldemortConfig.setBdbCacheSize(totalCache);
            voldemortConfig.setBdbOneEnvPerStore(true);
            voldemortConfig.setBdbDataDirectory(bdbMasterDir.toURI().getPath());
            voldemortConfig.setBdbPrefixKeysWithPartitionId(prefixPartitionId);

            bdbStorage = new BdbStorageConfiguration(voldemortConfig);
            StoreDefinition defA = TestUtils.makeStoreDefinition("storeA",
                                                                 shareA / (ByteUtils.BYTES_PER_MB));
            storeA = (BdbStorageEngine) bdbStorage.getStore(defA,
                                                            TestUtils.makeSingleNodeRoutingStrategy());

            StoreDefinition defB = TestUtils.makeStoreDefinition("storeB",
                                                                 shareB / (ByteUtils.BYTES_PER_MB));
            storeB = (BdbStorageEngine) bdbStorage.getStore(defB,
                                                            TestUtils.makeSingleNodeRoutingStrategy());

            StoreDefinition defC = TestUtils.makeStoreDefinition("storeC");
            storeC = (BdbStorageEngine) bdbStorage.getStore(defC,
                                                            TestUtils.makeSingleNodeRoutingStrategy());

            // before any traffic, the cache will not have grown
            assertTrue(Math.abs(shareA - getCacheSize(defA)) > ByteUtils.BYTES_PER_MB);
            assertTrue(Math.abs(shareB - getCacheSize(defB)) > ByteUtils.BYTES_PER_MB);

            // sharedCacheSize reading 0 confirms that the store has a private
            // cache
            assertEquals(0, getStats(bdbStorage.getEnvironment(defA)).getSharedCacheTotalBytes());
            assertEquals(0, getStats(bdbStorage.getEnvironment(defB)).getSharedCacheTotalBytes());

            // load data into the stores; each store is guaranteed to be ~ 40MB.
            // Data won't fit in memory
            byte[] value = new byte[ByteUtils.BYTES_PER_MB];
            for(int i = 0; i < numRecords; i++) {
                storeA.put(TestUtils.toByteArray("testKey" + i), new Versioned<byte[]>(value), null);
                storeB.put(TestUtils.toByteArray("testKey" + i), new Versioned<byte[]>(value), null);
                storeC.put(TestUtils.toByteArray("testKey" + i), new Versioned<byte[]>(value), null);
            }

            // we will bring all of that data into the cache, by doing a
            // keywalk.
            // This should expand the cache as much as possible
            long cacheSizeA = Long.MIN_VALUE;
            long cacheSizeB = Long.MIN_VALUE;
            long cacheSizeC = Long.MIN_VALUE;

            for(int cycle = 0; cycle < 10; cycle++) {
                for(int i = 0; i < numRecords; i++) {
                    long cycleCacheSizeA = getAndCheckCacheSize(storeA, defA, "testKey" + i);
                    long cycleCacheSizeB = getAndCheckCacheSize(storeB, defB, "testKey" + i);
                    long cycleCacheSizeC = getAndCheckCacheSize(storeC, defC, "testKey" + i);
                    // record the maximum cache size, each store every grew to
                    cacheSizeA = (cycleCacheSizeA > cacheSizeA) ? cycleCacheSizeA : cacheSizeA;
                    cacheSizeB = (cycleCacheSizeB > cacheSizeB) ? cycleCacheSizeB : cacheSizeB;
                    cacheSizeC = (cycleCacheSizeC > cacheSizeC) ? cycleCacheSizeC : cacheSizeC;
                }
            }

            // check that they are certainly less than expected limits.
            assertTrue(cacheSizeA <= shareA);
            assertTrue(cacheSizeB <= shareB);
            assertTrue(cacheSizeC <= shareC);

            // check that they are not exceedingly high than their limits. Small
            // overflows are okay. But should not be more than a 1MB
            assertTrue(Math.abs(cacheSizeA - shareA) <= ByteUtils.BYTES_PER_MB);
            assertTrue(Math.abs(cacheSizeB - shareB) <= ByteUtils.BYTES_PER_MB);
            assertTrue(Math.abs(cacheSizeC - shareC) <= ByteUtils.BYTES_PER_MB);

            // try doing reads on store C alone, for which we have no
            // reservations.
            // This simulates a spike on one store
            long cacheSizeCNow = Long.MIN_VALUE;
            for(int cycle = 0; cycle < 10; cycle++) {
                for(int i = 0; i < numRecords; i++) {
                    long cycleCacheSizeCNow = getAndCheckCacheSize(storeC, defC, "testkey" + i);
                    // record the maximum cache size, each store grew to
                    cacheSizeCNow = (cycleCacheSizeCNow > cacheSizeCNow) ? cycleCacheSizeCNow
                                                                        : cacheSizeCNow;
                }
            }

            assertTrue(cacheSizeCNow <= shareC);
        } finally {
            if(storeA != null)
                storeA.close();
            if(storeB != null)
                storeB.close();
            if(storeC != null)
                storeC.close();
            bdbStorage.close();
        }
    }

    /**
     * Tests that any reservation that will not violate minimum shared cache
     * will fail, during server startup and dynamic updation
     */
    @Test
    public void testMinimumSharedCache() {
        int totalCache = 20 * ByteUtils.BYTES_PER_MB; // total cache size
        int shareA = 10 * ByteUtils.BYTES_PER_MB;// A reserves 10MB

        // lets use all the default values.
        Props props = new Props();
        props.put("node.id", 1);
        props.put("voldemort.home", "test/common/voldemort/config");
        VoldemortConfig voldemortConfig = new VoldemortConfig(props);
        voldemortConfig.setBdbCacheSize(totalCache);
        voldemortConfig.setBdbOneEnvPerStore(true);
        voldemortConfig.setBdbDataDirectory(bdbMasterDir.toURI().getPath());
        voldemortConfig.setBdbMinimumSharedCache(15 * ByteUtils.BYTES_PER_MB);
        voldemortConfig.setBdbPrefixKeysWithPartitionId(prefixPartitionId);

        BdbStorageEngine storeA = null;
        bdbStorage = new BdbStorageConfiguration(voldemortConfig);
        assertEquals(0, bdbStorage.getReservedCacheSize());

        try {
            StoreDefinition defA = TestUtils.makeStoreDefinition("storeA", shareA
                                                                           / ByteUtils.BYTES_PER_MB);
            storeA = (BdbStorageEngine) bdbStorage.getStore(defA,
                                                            TestUtils.makeSingleNodeRoutingStrategy());
            fail("Should have thrown exception since minSharedCache will be violated");
        } catch(StorageInitializationException sie) {
            // should come here.
        }
        // failing operations should not alter reserved cache size
        assertEquals(0, bdbStorage.getReservedCacheSize());

        voldemortConfig.setBdbMinimumSharedCache(10 * ByteUtils.BYTES_PER_MB);
        bdbStorage = new BdbStorageConfiguration(voldemortConfig);
        try {
            StoreDefinition defA = TestUtils.makeStoreDefinition("storeA", shareA
                                                                           / ByteUtils.BYTES_PER_MB);
            storeA = (BdbStorageEngine) bdbStorage.getStore(defA,
                                                            TestUtils.makeSingleNodeRoutingStrategy());
        } catch(StorageInitializationException sie) {
            // should not come here.
            fail("minSharedCache should n't have been violated");
        }
        assertEquals(shareA, bdbStorage.getReservedCacheSize());

        long reserveCacheSize = bdbStorage.getReservedCacheSize();
        // now, try increasing the reservation dynamically and it should fail
        try {
            StoreDefinition defA = TestUtils.makeStoreDefinition("storeA", 15);
            bdbStorage.update(defA);
            fail("Should have thrown exception since minSharedCache will be violated");
        } catch(StorageInitializationException sie) {
            // should come here.
        }
        // this failure cannot alter the reservedCacheSize
        assertEquals(reserveCacheSize, bdbStorage.getReservedCacheSize());

        if(storeA != null)
            storeA.close();
        bdbStorage.close();
    }

    @Test
    public void testDynamicReservations() {
        int totalCache = 20 * ByteUtils.BYTES_PER_MB; // total cache size
        int shareA = 10 * ByteUtils.BYTES_PER_MB;// A reserves 10MB
        int shareB = totalCache - shareA;
        int numRecords = 40;

        // lets use all the default values.
        Props props = new Props();
        props.put("node.id", 1);
        props.put("voldemort.home", "test/common/voldemort/config");
        VoldemortConfig voldemortConfig = new VoldemortConfig(props);
        voldemortConfig.setBdbCacheSize(totalCache);
        voldemortConfig.setBdbOneEnvPerStore(true);
        voldemortConfig.setBdbDataDirectory(bdbMasterDir.toURI().getPath());
        voldemortConfig.setBdbMinimumSharedCache(5 * ByteUtils.BYTES_PER_MB);
        voldemortConfig.setBdbPrefixKeysWithPartitionId(prefixPartitionId);

        bdbStorage = new BdbStorageConfiguration(voldemortConfig);
        StoreDefinition defA = TestUtils.makeStoreDefinition("storeA", shareA / (1024 * 1024));
        BdbStorageEngine storeA = (BdbStorageEngine) bdbStorage.getStore(defA,
                                                                         TestUtils.makeSingleNodeRoutingStrategy());

        StoreDefinition defB = TestUtils.makeStoreDefinition("storeB");
        BdbStorageEngine storeB = (BdbStorageEngine) bdbStorage.getStore(defB,
                                                                         TestUtils.makeSingleNodeRoutingStrategy());

        // load data into the stores; each store is guaranteed to be ~ 40MB.
        // Data won't fit in memory
        byte[] value = new byte[ByteUtils.BYTES_PER_MB];
        for(int i = 0; i < numRecords; i++) {
            storeA.put(TestUtils.toByteArray("testKey" + i), new Versioned<byte[]>(value), null);
            storeB.put(TestUtils.toByteArray("testKey" + i), new Versioned<byte[]>(value), null);
        }

        // 1. start with 10MB reserved cache for A and the rest 10MB for B
        long cacheSizeA = Long.MIN_VALUE;
        long cacheSizeB = Long.MIN_VALUE;

        for(int cycle = 0; cycle < 10; cycle++) {
            for(int i = 0; i < numRecords; i++) {
                long cycleCacheSizeA = getAndCheckCacheSize(storeA, defA, "testKey" + i);
                long cycleCacheSizeB = getAndCheckCacheSize(storeB, defB, "testKey" + i);
                // record the maximum cache size, each store every grew to
                cacheSizeA = (cycleCacheSizeA > cacheSizeA) ? cycleCacheSizeA : cacheSizeA;
                cacheSizeB = (cycleCacheSizeB > cacheSizeB) ? cycleCacheSizeB : cacheSizeB;
            }
        }

        assertTrue(Math.abs(cacheSizeA - shareA) <= ByteUtils.BYTES_PER_MB);
        assertTrue(Math.abs(cacheSizeB - shareB) <= ByteUtils.BYTES_PER_MB);

        // 2. dynamically grow the cache to 15MB and watch B shrink.
        shareA = 15 * ByteUtils.BYTES_PER_MB;
        shareB = totalCache - shareA;
        defA = TestUtils.makeStoreDefinition("storeA", shareA / (1024 * 1024));
        bdbStorage.update(defA);

        cacheSizeA = Long.MIN_VALUE;
        cacheSizeB = Long.MIN_VALUE;

        for(int cycle = 0; cycle < 10; cycle++) {
            for(int i = 0; i < numRecords; i++) {
                long cycleCacheSizeA = getAndCheckCacheSize(storeA, defA, "testKey" + i);
                long cycleCacheSizeB = getAndCheckCacheSize(storeB, defB, "testKey" + i);
                // record the maximum cache size, each store every grew to
                cacheSizeA = (cycleCacheSizeA > cacheSizeA) ? cycleCacheSizeA : cacheSizeA;
                cacheSizeB = (cycleCacheSizeB > cacheSizeB) ? cycleCacheSizeB : cacheSizeB;
            }
        }

        assertTrue(Math.abs(cacheSizeA - shareA) <= ByteUtils.BYTES_PER_MB);
        assertTrue(Math.abs(cacheSizeB - shareB) <= ByteUtils.BYTES_PER_MB);

        // 3. dynamically shrink it back to 10MB and watch B expand again.
        shareA = 10 * ByteUtils.BYTES_PER_MB;
        shareB = totalCache - shareA;
        defA = TestUtils.makeStoreDefinition("storeA", shareA / (1024 * 1024));
        bdbStorage.update(defA);

        cacheSizeA = Long.MIN_VALUE;
        cacheSizeB = Long.MIN_VALUE;

        for(int cycle = 0; cycle < 10; cycle++) {
            for(int i = 0; i < numRecords; i++) {
                long cycleCacheSizeA = getAndCheckCacheSize(storeA, defA, "testKey" + i);
                long cycleCacheSizeB = getAndCheckCacheSize(storeB, defB, "testKey" + i);
                // record the maximum cache size, each store every grew to
                cacheSizeA = (cycleCacheSizeA > cacheSizeA) ? cycleCacheSizeA : cacheSizeA;
                cacheSizeB = (cycleCacheSizeB > cacheSizeB) ? cycleCacheSizeB : cacheSizeB;
            }
        }

        // check that they are not exceedingly high than their limits. Small
        // overflows are expected. But should not be more than a 1MB
        assertTrue(Math.abs(cacheSizeA - shareA) <= ByteUtils.BYTES_PER_MB);
        assertTrue(Math.abs(cacheSizeB - shareB) <= ByteUtils.BYTES_PER_MB);

        storeA.close();
        storeB.close();
        bdbStorage.close();
    }

}
