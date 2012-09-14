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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileDeleteStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.TestUtils;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;

@RunWith(Parameterized.class)
public class BdbStorageEngineTest extends AbstractStorageEngineTest {

    private static final LockMode LOCK_MODE = LockMode.DEFAULT;

    private Environment environment;
    private EnvironmentConfig envConfig;
    private Database database;
    private File tempDir;
    private BdbStorageEngine store;
    private DatabaseConfig databaseConfig;
    private BdbRuntimeConfig runtimeConfig;
    private boolean prefixPartitionId;

    public BdbStorageEngineTest(boolean prefixPartitionId) {
        this.prefixPartitionId = prefixPartitionId;
    }

    @Parameters
    public static Collection<Object[]> modes() {
        Object[][] data = new Object[][] { { true }, { false } };
        return Arrays.asList(data);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.envConfig = new EnvironmentConfig();
        this.envConfig.setDurability(Durability.COMMIT_NO_SYNC);
        this.envConfig.setAllowCreate(true);
        this.envConfig.setTransactional(true);
        this.tempDir = TestUtils.createTempDir();
        this.environment = new Environment(this.tempDir, envConfig);
        this.databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setTransactional(true);
        databaseConfig.setSortedDuplicates(false);
        this.database = environment.openDatabase(null, "test", databaseConfig);
        this.runtimeConfig = new BdbRuntimeConfig();
        runtimeConfig.setLockMode(LOCK_MODE);
        this.store = makeBdbStorageEngine("test",
                                          this.environment,
                                          this.database,
                                          runtimeConfig,
                                          this.prefixPartitionId);
    }

    protected static BdbStorageEngine makeBdbStorageEngine(String name,
                                                           Environment environment,
                                                           Database database,
                                                           BdbRuntimeConfig config,
                                                           boolean prefixPartitionId) {
        if(prefixPartitionId) {
            return new PartitionPrefixedBdbStorageEngine(name,
                                                         environment,
                                                         database,
                                                         config,
                                                         TestUtils.makeSingleNodeRoutingStrategy());
        } else {
            return new BdbStorageEngine(name, environment, database, config);
        }
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        try {
            store.close();
            environment.close();
        } finally {
            FileDeleteStrategy.FORCE.delete(tempDir);
        }
    }

    @Override
    public StorageEngine<ByteArray, byte[], byte[]> getStorageEngine() {
        return store;
    }

    @Test
    public void testPersistence() throws Exception {
        this.store.put(new ByteArray("abc".getBytes()),
                       new Versioned<byte[]>("cdef".getBytes()),
                       null);
        this.store.close();
        this.environment.close();
        this.environment = new Environment(this.tempDir, envConfig);
        this.database = environment.openDatabase(null, "test", databaseConfig);
        this.store = makeBdbStorageEngine("test",
                                          this.environment,
                                          this.database,
                                          runtimeConfig,
                                          this.prefixPartitionId);
        List<Versioned<byte[]>> vals = store.get(new ByteArray("abc".getBytes()), null);
        assertEquals(1, vals.size());
        TestUtils.bytesEqual("cdef".getBytes(), vals.get(0).getValue());
    }

    @Test
    public void testEquals() {
        String name = "someName";
        assertEquals(makeBdbStorageEngine(name,
                                          environment,
                                          database,
                                          runtimeConfig,
                                          this.prefixPartitionId),
                     makeBdbStorageEngine(name,
                                          environment,
                                          database,
                                          runtimeConfig,
                                          this.prefixPartitionId));
    }

    @Test
    public void testNullConstructorParameters() {
        try {
            makeBdbStorageEngine(null, environment, database, runtimeConfig, this.prefixPartitionId);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("No exception thrown for null name.");
        try {
            makeBdbStorageEngine("name", null, database, runtimeConfig, this.prefixPartitionId);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("No exception thrown for null environment.");
        try {
            makeBdbStorageEngine("name", environment, null, runtimeConfig, this.prefixPartitionId);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("No exception thrown for null database.");
    }

    @Test
    public void testConcurrentReadAndPut() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(10);
        final CountDownLatch latch = new CountDownLatch(10);
        final AtomicBoolean returnedEmpty = new AtomicBoolean(false);
        final byte[] keyBytes = "foo".getBytes();
        final byte[] valueBytes = "bar".getBytes();
        store.put(new ByteArray(keyBytes), new Versioned<byte[]>(valueBytes), null);

        for(int i = 0; i < 10; i++) {
            executor.submit(new Runnable() {

                public void run() {
                    try {
                        for(int j = 0; j < 1000 && !returnedEmpty.get(); j++) {
                            List<Versioned<byte[]>> vals = store.get(new ByteArray(keyBytes), null);
                            if(vals.size() == 0 && j > 1)
                                returnedEmpty.set(true);
                            else {
                                VectorClock v = (VectorClock) vals.get(0).getVersion();
                                v.incrementVersion(0, System.currentTimeMillis());
                                try {
                                    store.put(new ByteArray(keyBytes),
                                              new Versioned<byte[]>(valueBytes, v),
                                              null);
                                } catch(ObsoleteVersionException e) {
                                    // Ignore these
                                }
                            }
                        }
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        latch.await();
        assertFalse("Should not have seen any empty results", returnedEmpty.get());
    }

    @Test
    public void testSimultaneousIterationAndModification() throws Exception {
        // start a thread to do modifications
        ExecutorService executor = Executors.newFixedThreadPool(2);
        final Random rand = new Random();
        final AtomicInteger count = new AtomicInteger(0);
        final AtomicBoolean keepRunning = new AtomicBoolean(true);
        executor.execute(new Runnable() {

            public void run() {
                while(keepRunning.get()) {
                    byte[] bytes = Integer.toString(count.getAndIncrement()).getBytes();
                    store.put(new ByteArray(bytes), Versioned.value(bytes), null);
                    count.incrementAndGet();
                }
            }
        });
        executor.execute(new Runnable() {

            public void run() {
                while(keepRunning.get()) {
                    byte[] bytes = Integer.toString(rand.nextInt(count.get())).getBytes();
                    store.delete(new ByteArray(bytes), new VectorClock());
                    count.incrementAndGet();
                }
            }
        });

        // wait a bit
        while(count.get() < 300)
            continue;

        // now simultaneously do iteration
        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iter = this.store.entries();
        while(iter.hasNext())
            iter.next();
        iter.close();
        keepRunning.set(false);
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    public void testNativeBackup() throws Exception {
        File backupToDir = File.createTempFile("bdb-storage", "bkp");
        backupToDir.delete();
        backupToDir.mkdir();
        try {
            store.nativeBackup(backupToDir, false, false, new AsyncOperationStatus(0, "dummy"));
            // Check that one file was copied
            assertArrayEquals(backupToDir.list(), new String[] { "00000000.jdb" });
            long backupFileModified = backupToDir.listFiles()[0].lastModified();

            store.nativeBackup(backupToDir, false, false, new AsyncOperationStatus(0, "dummy"));
            // Check that there are now two files, and the first one hasn't
            // changed
            String[] backedUp = backupToDir.list();
            Arrays.sort(backedUp);
            assertArrayEquals(backedUp, new String[] { "00000000.jdb", "00000001.jdb" });
            assertEquals(backupFileModified, backupToDir.listFiles()[0].lastModified());
        } finally {
            deleteDir(backupToDir);
        }
    }

    private static void assertArrayEquals(Object[] expected, Object[] actual) {
        String error = Arrays.toString(expected) + " does not equal " + Arrays.toString(actual);
        assertEquals(error, expected.length, actual.length);
        for(int i = 0; i < expected.length; i++) {
            assertEquals(error, expected[i], actual[i]);
        }
    }

    private boolean deleteDir(File dir) {
        if(dir.isDirectory()) {
            for(File file: dir.listFiles()) {
                deleteDir(file);
            }
        }
        return dir.delete();
    }
}
