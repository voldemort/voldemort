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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.TestUtils;
import voldemort.server.VoldemortConfig;
import voldemort.utils.ByteUtils;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.StatsConfig;

/**
 * checks BDB runtime behavior relating to operating multiple environments
 * 
 * 
 */
@RunWith(Parameterized.class)
public class BdbSplitStorageEngineTest {

    private File bdbMasterDir;
    private BdbStorageConfiguration bdbStorage;

    private static long CACHE_SIZE = (long) Math.min(Runtime.getRuntime().maxMemory() * 0.30,
                                                     32 * 1000 * 1000);
    private boolean prefixPartitionId;

    public BdbSplitStorageEngineTest(boolean prefixPartitionId) {
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
    }

    @After
    public void tearDown() throws Exception {
        if(bdbStorage != null)
            bdbStorage.close();
    }

    @Test
    public void testNoMultipleEnvironment() {
        // lets use all the default values.
        Props props = new Props();
        props.put("node.id", 1);
        props.put("voldemort.home", "test/common/voldemort/config");
        VoldemortConfig voldemortConfig = new VoldemortConfig(props);
        voldemortConfig.setBdbCacheSize(1 * 1024 * 1024);
        voldemortConfig.setBdbDataDirectory(bdbMasterDir.toURI().getPath());
        voldemortConfig.setBdbOneEnvPerStore(false);
        voldemortConfig.setBdbPrefixKeysWithPartitionId(prefixPartitionId);

        bdbStorage = new BdbStorageConfiguration(voldemortConfig);
        BdbStorageEngine storeA = (BdbStorageEngine) bdbStorage.getStore(TestUtils.makeStoreDefinition("storeA"),
                                                                         TestUtils.makeSingleNodeRoutingStrategy());
        BdbStorageEngine storeB = (BdbStorageEngine) bdbStorage.getStore(TestUtils.makeStoreDefinition("storeB"),
                                                                         TestUtils.makeSingleNodeRoutingStrategy());

        storeA.put(TestUtils.toByteArray("testKey1"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);
        storeA.put(TestUtils.toByteArray("testKey2"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);
        storeA.put(TestUtils.toByteArray("testKey3"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);

        storeB.put(TestUtils.toByteArray("testKey1"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);
        storeB.put(TestUtils.toByteArray("testKey2"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);
        storeB.put(TestUtils.toByteArray("testKey3"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);

        storeA.close();
        storeB.close();

        Assert.assertEquals("common BDB file should exists.", true, (bdbMasterDir.exists()));

        assertNotSame("StoreA BDB file should not exists.", true, (new File(bdbMasterDir + "/"
                                                                            + "storeA").exists()));
        assertNotSame("StoreB BDB file should not exists.", true, (new File(bdbMasterDir + "/"
                                                                            + "storeB").exists()));
    }

    @Test
    public void testMultipleEnvironment() {
        // lets use all the default values.
        Props props = new Props();
        props.put("node.id", 1);
        props.put("voldemort.home", "test/common/voldemort/config");
        VoldemortConfig voldemortConfig = new VoldemortConfig(props);
        voldemortConfig.setBdbCacheSize(1 * 1024 * 1024);
        voldemortConfig.setBdbOneEnvPerStore(true);
        voldemortConfig.setBdbDataDirectory(bdbMasterDir.toURI().getPath());
        voldemortConfig.setBdbPrefixKeysWithPartitionId(prefixPartitionId);

        bdbStorage = new BdbStorageConfiguration(voldemortConfig);
        BdbStorageEngine storeA = (BdbStorageEngine) bdbStorage.getStore(TestUtils.makeStoreDefinition("storeA"),
                                                                         TestUtils.makeSingleNodeRoutingStrategy());
        BdbStorageEngine storeB = (BdbStorageEngine) bdbStorage.getStore(TestUtils.makeStoreDefinition("storeB"),
                                                                         TestUtils.makeSingleNodeRoutingStrategy());

        storeA.put(TestUtils.toByteArray("testKey1"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);
        storeA.put(TestUtils.toByteArray("testKey2"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);
        storeA.put(TestUtils.toByteArray("testKey3"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);

        storeB.put(TestUtils.toByteArray("testKey1"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);
        storeB.put(TestUtils.toByteArray("testKey2"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);
        storeB.put(TestUtils.toByteArray("testKey3"),
                   new Versioned<byte[]>("value".getBytes()),
                   null);

        storeA.close();
        storeB.close();

        assertEquals("StoreA BDB file should exists.", true, (new File(bdbMasterDir + "/"
                                                                       + "storeA").exists()));
        assertEquals("StoreB BDB file should  exists.", true, (new File(bdbMasterDir + "/"
                                                                        + "storeB").exists()));
    }

    @Test
    public void testUnsharedCache() throws DatabaseException {
        EnvironmentConfig environmentConfig = new EnvironmentConfig();
        environmentConfig.setDurability(Durability.COMMIT_NO_SYNC);
        environmentConfig.setAllowCreate(true);
        environmentConfig.setTransactional(true);
        environmentConfig.setSharedCache(false);
        environmentConfig.setCacheSize(CACHE_SIZE);

        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setTransactional(true);
        databaseConfig.setSortedDuplicates(true);

        long maxCacheSize = getMaxCacheUsage(environmentConfig, databaseConfig);

        assertEquals("MaxCacheSize > CACHE_SIZE", true, maxCacheSize > CACHE_SIZE);
        assertEquals("MaxCacheSize < 2 * CACHE_SIZE", true, maxCacheSize < 2 * CACHE_SIZE);
    }

    @Test
    public void testSharedCache() throws DatabaseException {
        EnvironmentConfig environmentConfig = new EnvironmentConfig();
        environmentConfig.setDurability(Durability.COMMIT_NO_SYNC);
        environmentConfig.setAllowCreate(true);
        environmentConfig.setTransactional(true);
        environmentConfig.setSharedCache(true);
        environmentConfig.setCacheSize(CACHE_SIZE);

        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setTransactional(true);
        databaseConfig.setSortedDuplicates(true);

        long maxCacheSize = getMaxCacheUsage(environmentConfig, databaseConfig);
        // Include a buffer of 1mb.. since the actual cache usage can be a few
        // bytes more
        assertEquals("MaxCacheSize" + maxCacheSize + " <= CACHE_SIZE:" + CACHE_SIZE,
                     true,
                     maxCacheSize <= (CACHE_SIZE + ByteUtils.BYTES_PER_MB));
    }

    private long getMaxCacheUsage(EnvironmentConfig environmentConfig, DatabaseConfig databaseConfig)
            throws DatabaseException {
        File dirA = new File(bdbMasterDir + "/" + "storeA");
        if(!dirA.exists()) {
            dirA.mkdirs();
        }
        Environment environmentA = new Environment(dirA, environmentConfig);
        Database databaseA = environmentA.openDatabase(null, "storeA", databaseConfig);
        BdbStorageEngine storeA = BdbStorageEngineTest.makeBdbStorageEngine("storeA",
                                                                            environmentA,
                                                                            databaseA,
                                                                            new BdbRuntimeConfig(),
                                                                            this.prefixPartitionId);

        File dirB = new File(bdbMasterDir + "/" + "storeB");
        if(!dirB.exists()) {
            dirB.mkdirs();
        }
        Environment environmentB = new Environment(dirB, environmentConfig);
        Database databaseB = environmentB.openDatabase(null, "storeB", databaseConfig);
        BdbStorageEngine storeB = BdbStorageEngineTest.makeBdbStorageEngine("storeB",
                                                                            environmentB,
                                                                            databaseB,
                                                                            new BdbRuntimeConfig(),
                                                                            this.prefixPartitionId);

        long maxCacheUsage = 0;
        for(int i = 0; i <= 10000; i++) {

            byte[] value = new byte[(int) (CACHE_SIZE / 10000)];
            // try to push values in cache
            storeA.put(TestUtils.toByteArray(i + "A"), new Versioned<byte[]>(value), null);
            storeA.get(TestUtils.toByteArray(i + "A"), null);

            storeB.put(TestUtils.toByteArray(i + "B"), new Versioned<byte[]>(value), null);
            storeB.get(TestUtils.toByteArray(i + "B"), null);

            EnvironmentStats statsA = environmentA.getStats(new StatsConfig());
            EnvironmentStats statsB = environmentB.getStats(new StatsConfig());

            long totalCacheSize = statsA.getCacheTotalBytes() + statsB.getCacheTotalBytes();
            System.out.println("A.size:" + statsA.getCacheTotalBytes() + " B.size:"
                               + statsB.getCacheTotalBytes() + " total:" + totalCacheSize + " max:"
                               + maxCacheUsage + " cacheMax:"
                               + environmentA.getConfig().getCacheSize());
            System.out.println("Shared.A:" + statsA.getSharedCacheTotalBytes() + " nSharedEnv:"
                               + statsA.getNSharedCacheEnvironments());
            maxCacheUsage = Math.max(maxCacheUsage, totalCacheSize);
        }

        return maxCacheUsage;
    }
}
