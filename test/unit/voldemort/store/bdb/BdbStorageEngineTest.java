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
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileDeleteStrategy;

import voldemort.TestUtils;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class BdbStorageEngineTest extends AbstractStorageEngineTest {

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
        this.store = new BdbStorageEngine("test", this.environment, this.database);
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
        this.store = new BdbStorageEngine("test", this.environment, this.database);
        List<Versioned<byte[]>> vals = store.get(new ByteArray("abc".getBytes()), null);
        assertEquals(1, vals.size());
        TestUtils.bytesEqual("cdef".getBytes(), vals.get(0).getValue());
    }

    public void testEquals() {
        String name = "someName";
        assertEquals(new BdbStorageEngine(name, environment, database),
                     new BdbStorageEngine(name, environment, database));
    }

    public void testNullConstructorParameters() {
        try {
            new BdbStorageEngine(null, environment, database);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("No exception thrown for null name.");
        try {
            new BdbStorageEngine("name", null, database);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("No exception thrown for null environment.");
        try {
            new BdbStorageEngine("name", environment, null);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("No exception thrown for null database.");
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
}
