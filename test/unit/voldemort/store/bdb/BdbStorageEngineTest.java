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

import com.sleepycat.je.*;
import org.apache.commons.io.FileDeleteStrategy;
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

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BdbStorageEngineTest extends AbstractStorageEngineTest {

    private static final LockMode LOCK_MODE = LockMode.DEFAULT;

    private Environment environment;
    private EnvironmentConfig envConfig;
    private Database database;
    private File tempDir;
    private BdbStorageEngine store;
    private DatabaseConfig databaseConfig;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.envConfig = new EnvironmentConfig();
        this.envConfig.setTxnNoSync(true);
        this.envConfig.setAllowCreate(true);
        this.envConfig.setTransactional(true);
        this.tempDir = TestUtils.createTempDir();
        this.environment = new Environment(this.tempDir, envConfig);
        this.databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate(true);
        databaseConfig.setTransactional(true);
        databaseConfig.setSortedDuplicates(true);
        this.database = environment.openDatabase(null, "test", databaseConfig);
        this.store = new BdbStorageEngine("test", this.environment, this.database, LOCK_MODE);
    }

    @Override
    protected void tearDown() throws Exception {
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

    public void testPersistence() throws Exception {
        this.store.put(new ByteArray("abc".getBytes()),
                       new Versioned<byte[]>("cdef".getBytes()),
                       null);
        this.store.close();
        this.environment.close();
        this.environment = new Environment(this.tempDir, envConfig);
        this.database = environment.openDatabase(null, "test", databaseConfig);
        this.store = new BdbStorageEngine("test", this.environment, this.database, LOCK_MODE);
        List<Versioned<byte[]>> vals = store.get(new ByteArray("abc".getBytes()), null);
        assertEquals(1, vals.size());
        TestUtils.bytesEqual("cdef".getBytes(), vals.get(0).getValue());
    }

    public void testEquals() {
        String name = "someName";
        assertEquals(new BdbStorageEngine(name, environment, database, LOCK_MODE),
                     new BdbStorageEngine(name, environment, database, LOCK_MODE));
    }

    public void testNullConstructorParameters() {
        try {
            new BdbStorageEngine(null, environment, database, LOCK_MODE);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("No exception thrown for null name.");
        try {
            new BdbStorageEngine("name", null, database, LOCK_MODE);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("No exception thrown for null environment.");
        try {
            new BdbStorageEngine("name", environment, null, LOCK_MODE);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("No exception thrown for null database.");
    }

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
                                    store.put(new ByteArray(keyBytes), new Versioned<byte[]>(valueBytes, v), null);
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

    public void testNativeBackup() throws Exception {
        File backupToDir = File.createTempFile("bdb-storage", "bkp");
        backupToDir.delete();
        backupToDir.mkdir();
        try {
            store.nativeBackup(backupToDir, new AsyncOperationStatus(0, "dummy"));
            // Check that one file was copied
            assertArrayEquals(backupToDir.list(), new String[]{"00000000.jdb"});
            File backupFile = backupToDir.listFiles()[0];

            store.nativeBackup(backupToDir, new AsyncOperationStatus(0, "dummy"));
            // Check that there are now two files, and the first one hasn't changed
            assertArrayEquals(backupToDir.list(), new String[]{"00000000.jdb", "00000001.jdb"});
            assertEquals(backupFile.lastModified(), backupToDir.listFiles()[0].lastModified());
        } finally {
            deleteDir(backupToDir);
        }

    }

    private static void assertArrayEquals(Object[] expected, Object[] actual) {
        String error = Arrays.toString(expected) + " does not equal " + Arrays.toString(actual);
        assertEquals(error, expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(error, expected[0], actual[0]);
        }
    }

    private boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            for (File file : dir.listFiles()) {
                deleteDir(file);
            }
        }
        return dir.delete();
    }
}
