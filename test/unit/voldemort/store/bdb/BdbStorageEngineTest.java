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

import org.apache.commons.io.FileDeleteStrategy;

import voldemort.TestUtils;
import voldemort.store.StorageEngine;
import voldemort.store.StorageEngineTest;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class BdbStorageEngineTest extends StorageEngineTest {

    private Environment environment;
    private Database database;
    private File tempDir;
    private BdbStorageEngine store;
    private DatabaseConfig databaseConfig;
    private Random random;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.random = new Random();
        EnvironmentConfig environmentConfig = new EnvironmentConfig();
        environmentConfig = new EnvironmentConfig();
        environmentConfig.setTxnNoSync(true);
        environmentConfig.setAllowCreate(true);
        environmentConfig.setTransactional(true);
        this.tempDir = TestUtils.getTempDirectory();
        this.environment = new Environment(this.tempDir, environmentConfig);
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
            database.close();
            environment.close();
        } finally {
            FileDeleteStrategy.FORCE.delete(tempDir);
        }
    }

    @Override
    public StorageEngine<byte[], byte[]> getStorageEngine() {
        return store;
    }

    public void testMakeKey(byte[] key, VectorClock clock) {
        byte[] keyBytes = BdbStorageEngine.makeKey(key, clock);
        assertTrue("Invalid key returned",
                   TestUtils.bytesEqual(key, BdbStorageEngine.getObjKey(keyBytes)));
        assertEquals("Invalid clock returned", clock, BdbStorageEngine.getVersion(keyBytes));
    }

    public void testMakeKey() {
        testMakeKey("".getBytes(), TestUtils.getClock());
        testMakeKey("abc".getBytes(), null);
        testMakeKey("hello-there".getBytes(), TestUtils.getClock(1, 1, 2, 3, 4));
    }

    public void testPersistence() throws Exception {
        StorageEngine<byte[], byte[]> eng = getStorageEngine();
        eng.put("abc".getBytes(), new Versioned<byte[]>("cdef".getBytes()));
        eng.close();
        this.database = environment.openDatabase(null, "test", databaseConfig);
        eng = new BdbStorageEngine("test", this.environment, this.database);
        List<Versioned<byte[]>> vals = eng.get("abc".getBytes());
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

}
