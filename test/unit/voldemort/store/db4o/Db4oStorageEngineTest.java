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

package voldemort.store.db4o;

import java.io.File;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

import com.db4o.config.QueryEvaluationMode;
import com.db4o.cs.Db4oClientServer;
import com.db4o.cs.config.ServerConfiguration;

public class Db4oStorageEngineTest extends AbstractStorageEngineTest {

    private File tempDir;
    private Db4oByteArrayStorageEngine store;
    private ServerConfiguration dbConfig;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.tempDir = TestUtils.createTempDir();
        this.dbConfig = newDb4oConfig();
        this.store = new Db4oByteArrayStorageEngine(this.tempDir + "/" + "test", dbConfig);
    }

    private ServerConfiguration newDb4oConfig() {
        return getDb4oConfig(Db4oStorageConfiguration.KEY_VALUE_PAIR_CLASS,
                             Db4oStorageConfiguration.KEY_FIELD_NAME);
    }

    @SuppressWarnings("unchecked")
    private ServerConfiguration getDb4oConfig(Class keyValuePairClass, String keyFieldName) {
        ServerConfiguration config = Db4oClientServer.newServerConfiguration();
        // Use lazy mode
        config.common().queries().evaluationMode(QueryEvaluationMode.LAZY);
        // Set activation depth to 5
        config.common().activationDepth(5);
        // Set index by Key
        config.common().objectClass(keyValuePairClass).objectField(keyFieldName).indexed(true);
        // Cascade on delete
        config.common().objectClass(keyValuePairClass).cascadeOnDelete(true);
        return config;
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
    public StorageEngine<ByteArray, byte[]> getStorageEngine() {
        return store;
    }

    public void testPersistence() throws Exception {
        this.store.put(new ByteArray("abc".getBytes()), new Versioned<byte[]>("cdef".getBytes()));
        this.store.close();
        this.store = new Db4oByteArrayStorageEngine(this.tempDir + "/" + "test", newDb4oConfig());
        List<Versioned<byte[]>> vals = store.get(new ByteArray("abc".getBytes()));
        assertEquals(1, vals.size());
        TestUtils.bytesEqual("cdef".getBytes(), vals.get(0).getValue());
    }

    public void testEquals() {
        String name = "someName";
        assertEquals(new Db4oByteArrayStorageEngine(this.tempDir + "/" + name, newDb4oConfig()),
                     new Db4oByteArrayStorageEngine(this.tempDir + "/" + name, newDb4oConfig()));
    }

    public void testNullConstructorParameters() {
        try {
            new Db4oByteArrayStorageEngine(null, dbConfig);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("No exception thrown for null path name.");
        try {
            new Db4oByteArrayStorageEngine("name", null);
        } catch(IllegalArgumentException e) {
            return;
        }
        fail("No exception thrown for null configuration.");
    }

    public void testSimultaneousIterationAndModification() throws Exception {
        // start a thread to do modifications
        ExecutorService executor = Executors.newFixedThreadPool(2);
        final Random rand = new Random();
        final AtomicInteger count = new AtomicInteger(0);
        executor.execute(new Runnable() {

            public void run() {
                while(!Thread.interrupted()) {
                    byte[] bytes = Integer.toString(count.getAndIncrement()).getBytes();
                    store.put(new ByteArray(bytes), Versioned.value(bytes));
                    count.incrementAndGet();
                }
            }
        });
        executor.execute(new Runnable() {

            public void run() {
                while(!Thread.interrupted()) {
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
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS));
    }
}
