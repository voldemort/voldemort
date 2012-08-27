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

package voldemort.scheduled;

import java.io.File;
import java.util.Date;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.io.FileDeleteStrategy;

import voldemort.MockTime;
import voldemort.TestUtils;
import voldemort.server.VoldemortConfig;
import voldemort.server.scheduler.DataCleanupJob;
import voldemort.server.scheduler.SchedulerService;
import voldemort.server.storage.ScanPermitWrapper;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.utils.ByteArray;
import voldemort.utils.EventThrottler;
import voldemort.utils.Props;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class DataCleanupJobTest extends TestCase {

    private MockTime time;
    private StorageEngine<ByteArray, byte[], byte[]> engine;
    private File storeDir;
    private BdbStorageConfiguration bdbStorage;

    @Override
    public void setUp() throws Exception {
        time = new MockTime();
        storeDir = TestUtils.createTempDir();
        FileDeleteStrategy.FORCE.delete(storeDir);

        // lets use all the default values.
        Props props = new Props();
        props.put("node.id", 1);
        props.put("voldemort.home", "test/common/voldemort/config");
        VoldemortConfig voldemortConfig = new VoldemortConfig(props);
        voldemortConfig.setBdbCacheSize(1024 * 1024);
        voldemortConfig.setBdbOneEnvPerStore(true);
        voldemortConfig.setBdbDataDirectory(storeDir.toURI().getPath());

        bdbStorage = new BdbStorageConfiguration(voldemortConfig);
        StoreDefinition defA = TestUtils.makeStoreDefinition("cleanupTestStore");
        engine = bdbStorage.getStore(defA);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        try {
            if(engine != null)
                engine.close();
            if(bdbStorage != null)
                bdbStorage.close();
        } finally {
            FileDeleteStrategy.FORCE.delete(storeDir);
        }
    }

    public void testCleanupFrequency() {

        SchedulerService scheduler = new SchedulerService(1, time);

        try {
            Date now = new Date();

            // clean up will purge everything older than last 2 seconds
            Runnable cleanupJob = new DataCleanupJob<ByteArray, byte[], byte[]>(engine,
                                                                                new ScanPermitWrapper(1),
                                                                                2 * Time.MS_PER_SECOND,
                                                                                SystemTime.INSTANCE,
                                                                                new EventThrottler(1));

            // and will run every 5 seconds starting now
            scheduler.schedule("cleanup-freq-test", cleanupJob, now, 5 * Time.MS_PER_SECOND);

            // load some data
            for(int i = 0; i < 10; i++) {
                ByteArray b = new ByteArray(Integer.toString(i).getBytes());
                engine.put(b, new Versioned<byte[]>(b.get()), null);
            }
            // sleep for 2 seconds
            Thread.sleep(2 * Time.MS_PER_SECOND);

            // None of the keys should have been deleted, i.e data cleanup
            // should n't have run.
            for(int i = 0; i < 10; i++) {
                ByteArray b = new ByteArray(Integer.toString(i).getBytes());
                List<Versioned<byte[]>> found = engine.get(b, null);
                assertTrue("Did not find key '" + i + "' in store!", found.size() > 0);
            }

            // wait till 4 seconds from start
            Thread.sleep(System.currentTimeMillis() - (now.getTime() + 4 * Time.MS_PER_SECOND));
            // load some more data
            for(int i = 10; i < 20; i++) {
                ByteArray b = new ByteArray(Integer.toString(i).getBytes());
                engine.put(b, new Versioned<byte[]>(b.get()), null);
            }

            // give time for data cleanup to finally run
            Thread.sleep(System.currentTimeMillis() - (now.getTime() + 6 * Time.MS_PER_SECOND));

            // first batch of writes should have been deleted
            for(int i = 0; i < 10; i++) {
                ByteArray b = new ByteArray(Integer.toString(i).getBytes());
                List<Versioned<byte[]>> found = engine.get(b, null);
                assertTrue("Expected key '" + i + "' to be deleted!", found.size() == 0);
            }
            // and later ones retained.
            for(int i = 10; i < 20; i++) {
                ByteArray b = new ByteArray(Integer.toString(i).getBytes());
                List<Versioned<byte[]>> found = engine.get(b, null);
                assertTrue("Expected key '" + i + "' to be retained!", found.size() > 0);
            }

        } catch(Exception e) {

        } finally {
            scheduler.stop();
        }
    }

    public void testCleanupCleansUp() {
        time.setTime(123);
        put("a", "b", "c");
        time.setTime(123 + Time.MS_PER_DAY + 1);
        put("d", "e", "f");
        assertContains("a", "b", "c", "d", "e", "f");

        // update a single item to bump its vector clock time
        put("a");

        // now run cleanup
        new DataCleanupJob<ByteArray, byte[], byte[]>(engine,
                                                      new ScanPermitWrapper(1),
                                                      Time.MS_PER_DAY,
                                                      time,
                                                      new EventThrottler(1)).run();

        // Check that all the later keys are there AND the key updated later
        assertContains("a", "d", "e", "f");
    }

    private void put(String... items) {
        for(String item: items) {
            VectorClock clock = null;
            List<Versioned<byte[]>> found = engine.get(new ByteArray(item.getBytes()), null);
            if(found.size() == 0) {
                clock = new VectorClock(time.getMilliseconds());
            } else if(found.size() == 1) {
                VectorClock oldClock = (VectorClock) found.get(0).getVersion();
                clock = oldClock.incremented(0, time.getMilliseconds());
            } else {
                fail("Found multiple versions.");
            }
            engine.put(new ByteArray(item.getBytes()),
                       new Versioned<byte[]>(item.getBytes(), clock),
                       null);
        }
    }

    private void assertContains(String... keys) {
        for(String key: keys) {
            List<Versioned<byte[]>> found = engine.get(new ByteArray(key.getBytes()), null);
            assertTrue("Did not find key '" + key + "' in store!", found.size() > 0);
        }
    }

}
