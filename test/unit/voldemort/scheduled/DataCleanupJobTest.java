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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.io.File;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.MockTime;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.common.service.SchedulerService;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.scheduler.DataCleanupJob;
import voldemort.server.storage.ScanPermitWrapper;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.retention.RetentionEnforcingStore;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Props;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class DataCleanupJobTest {

    private static final Logger logger = Logger.getLogger(DataCleanupJobTest.class);

    private MockTime time;
    private StorageEngine<ByteArray, byte[], byte[]> engine;
    private File storeDir;
    private BdbStorageConfiguration bdbStorage;
    private MetadataStore metadataStore;
    private boolean prefixPartitionId;
    private final static String STORE_NAME = "cleanupTestStore";

    private final static int START_RETENTION = 7;
    private final static int REDUCED_RETENTION = 2;

    public DataCleanupJobTest(boolean prefixPartitionId) {
        this.prefixPartitionId = prefixPartitionId;
    }

    @Parameters
    public static Collection<Object[]> modes() {
        Object[][] data = new Object[][] { { true }, { false } };
        return Arrays.asList(data);
    }

    private StoreDefinition getStoreDef(int retentionDays) {
        SerializerDefinition serializerDef = new SerializerDefinition("string");
        return new StoreDefinitionBuilder().setName(STORE_NAME)
                                           .setType(BdbStorageConfiguration.TYPE_NAME)
                                           .setKeySerializer(serializerDef)
                                           .setValueSerializer(serializerDef)
                                           .setRetentionPeriodDays(retentionDays)
                                           .setRoutingPolicy(RoutingTier.CLIENT)
                                           .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                           .setReplicationFactor(1)
                                           .setPreferredReads(1)
                                           .setRequiredReads(1)
                                           .setPreferredWrites(1)
                                           .setRequiredWrites(1)
                                           .build();
    }

    private void updateStoreDef(int retentionDays) {
        StoreDefinition storeDef = getStoreDef(retentionDays);
        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
        String storeStr = mapper.writeStoreList(Arrays.asList(storeDef));
        VectorClock clock = new VectorClock(System.currentTimeMillis());
        clock.incrementVersion(0, System.currentTimeMillis());
        Versioned<byte[]> storeSerialized = new Versioned<byte[]>(ByteUtils.getBytes(storeStr, "UTF-8"), clock);
        metadataStore.updateStoreDefinitions(storeSerialized);
    }

    @Before
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
        voldemortConfig.setBdbPrefixKeysWithPartitionId(prefixPartitionId);

        bdbStorage = new BdbStorageConfiguration(voldemortConfig);
        StoreDefinition storeDef = getStoreDef(START_RETENTION);
        engine = bdbStorage.getStore(storeDef, TestUtils.makeSingleNodeRoutingStrategy());

        List<Node> nodes = Lists.newArrayList();
        nodes.add(new Node(0, "test-host", 1234, 1235, 1236, Arrays.asList(0)));
        Cluster cluster = new Cluster("cluster", nodes);

        StoreRepository repo = new StoreRepository();
        repo.setSlopStore(new SlopStorageEngine(new InMemoryStorageEngine<ByteArray, byte[], byte[]>("slop"), cluster));
        repo.addNodeStore(0, engine);
        metadataStore = ServerTestUtils.createMetadataStore(cluster, Arrays.asList(storeDef));
    }

    @After
    public void tearDown() throws Exception {

        try {
            if(engine != null)
                engine.close();
            if(bdbStorage != null)
                bdbStorage.close();
        } finally {
            FileDeleteStrategy.FORCE.delete(storeDir);
        }
    }

    private void putWithTimeStamp(int start, int end, long milliseconds) {
        for (int i = start; i < end; i++) {
            ByteArray b = new ByteArray(Integer.toString(i).getBytes());
            VectorClock clock = new VectorClock(milliseconds);
            Versioned<byte[]> value = new Versioned<byte[]>(b.get(), clock);
            engine.put(b, value, null);
        }
    }

    private void assertPresence(int start, int end) {
        for (int i = start; i < end; i++) {
            ByteArray b = new ByteArray(Integer.toString(i).getBytes());
            List<Versioned<byte[]>> found = engine.get(b, null);
            assertTrue("Did not find key '" + i + "' in store!", found.size() > 0);
        }
    }

    private void assertAbsence(int start, int end) {
        for (int i = start; i < end; i++) {
            ByteArray b = new ByteArray(Integer.toString(i).getBytes());
            List<Versioned<byte[]>> found = engine.get(b, null);
            assertTrue("Expected key '" + i + "' to be deleted!", found.size() == 0);
        }
    }
    
    @Test
    public void testStoreDeletion() throws InterruptedException {
        SchedulerService scheduler = new SchedulerService(1, time);
        String cleanUpJobName = "cleanup-freq-test";
        
        try {
            MockTime mockTime = new MockTime(System.currentTimeMillis());
            ScanPermitWrapper scanWrapper = new ScanPermitWrapper(1);
            // clean up will purge everything older than last 2 seconds
            DataCleanupJob cleanupJob = new DataCleanupJob<ByteArray, byte[], byte[]>(engine,
                                                                                new ScanPermitWrapper(1),
                                                                                STORE_NAME,
                                                                                mockTime,
                                                                                metadataStore);

            // and will run every 3 seconds starting now
            scheduler.schedule(cleanUpJobName, cleanupJob, new Date(), 1 * Time.MS_PER_SECOND);

            // Insert records that should be deleted when the DataCleanUp Job
            // runs
            putWithTimeStamp(0, 10, System.currentTimeMillis() - 8 * Time.MS_PER_DAY);

            Thread.sleep(3 * Time.MS_PER_SECOND);
            // All of them should be deleted.
            assertAbsence(0, 10);

            // Delete the store.
            metadataStore.deleteStoreDefinition(STORE_NAME);

            // Wait 2 seconds to give the Scheduler job, come out of the
            // previous scan if any
            Thread.sleep(2 * Time.MS_PER_SECOND);
            
            // Check the same value 1000 times as the deleted store should
            // never acquire a lock. It should silently return before acquiring the lock.
            // Intermittent failure, means problem with the code which needs to be fixed.
            for(int i = 0; i < 1000; i ++) {
                assertEquals("Deleted store should never acquire a scan permit", 1, scanWrapper.availablePermits());
            }

        } finally {
            scheduler.terminate(cleanUpJobName);
            scheduler.stop();
        }
    }

    @Test
    public void testCleanupFrequency() throws InterruptedException {

        SchedulerService scheduler = new SchedulerService(1, time);
        String cleanUpJobName = "cleanup-freq-test";

        try {
            MockTime mockTime = new MockTime(System.currentTimeMillis());
            // clean up will purge everything older than last 2 seconds
            DataCleanupJob cleanupJob = new DataCleanupJob<ByteArray, byte[], byte[]>(engine,
                                                                                new ScanPermitWrapper(1),
                                                                                STORE_NAME,
                                                                                mockTime,
                                                                                metadataStore);

            // and will run every 3 seconds starting now
            scheduler.schedule(cleanUpJobName, cleanupJob, new Date(), 1 * Time.MS_PER_SECOND);

            // load some data
            putWithTimeStamp(0, 10, System.currentTimeMillis());

            Thread.sleep(3 * Time.MS_PER_SECOND);

            // None of the keys should have been deleted
            assertPresence(0, 10);
            assertEquals("No entries must be deleted now", 0, cleanupJob.getEntriesDeleted());
            assertTrue("Entries are scanned, but not deleted", cleanupJob.getEntriesScanned() > 0);

            // Increase the mockTime by a day and a millisecond.
            mockTime.setTime(System.currentTimeMillis() + START_RETENTION * Time.MS_PER_DAY + 1);
            // load some more data
            putWithTimeStamp(10, 20, mockTime.getMilliseconds());

            // give time for data cleanup to run
            Thread.sleep(3 * Time.MS_PER_SECOND);

            // first batch of writes should have been deleted
            assertAbsence(0, 10);
            // and later ones retained.
            assertPresence(10, 20);
            assertEquals("10 entries should be deleted", 10, cleanupJob.getEntriesDeleted());

            // Insert 10 keys that are separated by a day and millisecond
            long currentTime = mockTime.getMilliseconds();
            for (int i = 20; i < 30; i++) {
                currentTime += Time.MS_PER_DAY + 1;
                putWithTimeStamp(i, i + 1, currentTime);
            }

            mockTime.setTime(currentTime);
            updateStoreDef(REDUCED_RETENTION);

            Thread.sleep(3 * Time.MS_PER_SECOND);

            assertAbsence(10, 28);
            assertEquals("28 entries should be deleted", 28, cleanupJob.getEntriesDeleted());
            // Only last 2 keys should be present, third key is old by 2 days
            // and 2 milliseconds
            assertPresence(28, 30);
        } finally {
            scheduler.terminate(cleanUpJobName);
            scheduler.stop();
        }
    }

    @Test
    public void testCleanupCleansUp() {
        time.setTime(123);
        put("a", "b", "c");
        time.setTime(123 + START_RETENTION * Time.MS_PER_DAY + 1);
        put("d", "e", "f");
        assertContains("a", "b", "c", "d", "e", "f");

        // update a single item to bump its vector clock time
        put("a");

        // now run cleanup
        new DataCleanupJob<ByteArray, byte[], byte[]>(engine,
                                                      new ScanPermitWrapper(1),
                                                      STORE_NAME,
                                                      time,
                                                      metadataStore).run();

        // Check that all the later keys are there AND the key updated later
        assertContains("a", "d", "e", "f");
    }

    public void testCleanupStartTime() {
        // Make sure the default is always the next day.
        GregorianCalendar cal = new GregorianCalendar();
        assertEquals("Default is not tomorrow",
                     Utils.getDayOfTheWeekFromNow(1),
                     (cal.get(Calendar.DAY_OF_WEEK) + 1) % 7);

        // When starting the server any day in the week from SUN to FRI and
        // targeting a saturday, should always start on the next saturday
        GregorianCalendar expectedStart = TestUtils.getCalendar(2012,
                                                                Calendar.SEPTEMBER,
                                                                29,
                                                                0,
                                                                0,
                                                                0);
        Random rand = new Random();
        for(int day = Calendar.SUNDAY; day <= Calendar.FRIDAY; day++) {
            GregorianCalendar serverStartTime = TestUtils.getCalendar(2012,
                                                                      Calendar.SEPTEMBER,
                                                                      22 + day,
                                                                      rand.nextInt(24),
                                                                      rand.nextInt(60),
                                                                      rand.nextInt(60));
            GregorianCalendar computedStart = Utils.getCalendarForNextRun(serverStartTime,
                                                                          Calendar.SATURDAY,
                                                                          0);
            assertEquals("Expected :" + expectedStart.getTimeInMillis() + " Computed: "
                                 + computedStart.getTimeInMillis(),
                         expectedStart.getTimeInMillis(),
                         computedStart.getTimeInMillis());
        }

        // Targeting saturday, 00:00 and starting on a friday 23:59:59 should
        // start the next saturday
        GregorianCalendar serverStartTime = TestUtils.getCalendar(2012,
                                                                  Calendar.SEPTEMBER,
                                                                  28,
                                                                  23,
                                                                  59,
                                                                  59);
        GregorianCalendar computedStart = Utils.getCalendarForNextRun(serverStartTime,
                                                                      Calendar.SATURDAY,
                                                                      0);
        assertEquals("Expected :" + expectedStart.getTimeInMillis() + " Computed: "
                             + computedStart.getTimeInMillis(),
                     expectedStart.getTimeInMillis(),
                     computedStart.getTimeInMillis());

        // If we start past the start hour on the target day, it should start
        // the next week
        serverStartTime = TestUtils.getCalendar(2012, Calendar.SEPTEMBER, 29, 1, 0, 1);
        computedStart = Utils.getCalendarForNextRun(serverStartTime, Calendar.SATURDAY, 0);
        assertEquals(Calendar.SATURDAY, computedStart.get(Calendar.DAY_OF_WEEK));
        assertEquals(serverStartTime.get(Calendar.DAY_OF_YEAR) + 7,
                     computedStart.get(Calendar.DAY_OF_YEAR));
    }

    private void runRetentionEnforcingStoreTest(boolean onlineDeletes) throws InterruptedException {

        time.setTime(System.currentTimeMillis());
        StoreDefinition retentionStoreDef = new StoreDefinitionsMapper().readStoreList(new StringReader(VoldemortTestConstants.getStoreDefinitionsWithRetentionXml()))
                                                                        .get(0);
        RetentionEnforcingStore store = new RetentionEnforcingStore(engine,
                                                                    retentionStoreDef,
                                                                    onlineDeletes,
                                                                    time);
        // do a bunch of puts
        store.put(new ByteArray("k1".getBytes()), new Versioned<byte[]>("v1".getBytes()), null);
        store.put(new ByteArray("k2".getBytes()), new Versioned<byte[]>("v2".getBytes()), null);
        long writeMs = System.currentTimeMillis();

        // wait for a bit and then do more puts
        Thread.sleep(2000);

        store.put(new ByteArray("k3".getBytes()), new Versioned<byte[]>("v3".getBytes()), null);
        store.put(new ByteArray("k4".getBytes()), new Versioned<byte[]>("v4".getBytes()), null);

        // move time forward just enough such that some keys will have expired.
        time.setTime(writeMs + retentionStoreDef.getRetentionDays() * Time.MS_PER_DAY + 1);
        assertEquals("k1 should have expired", 0, store.get(new ByteArray("k1".getBytes()), null)
                                                       .size());
        assertEquals("k2 should have expired", 0, store.get(new ByteArray("k2".getBytes()), null)
                                                       .size());

        assertTrue("k3 should not have expired", store.get(new ByteArray("k3".getBytes()), null)
                                                      .size() > 0);
        assertTrue("k4 should not have expired", store.get(new ByteArray("k4".getBytes()), null)
                                                      .size() > 0);
        // get all with k1, k4 should return a map with k4 alone
        Map<ByteArray, List<Versioned<byte[]>>> getAllResult = store.getAll(Arrays.asList(new ByteArray("k1".getBytes()),
                                                                                          new ByteArray("k4".getBytes())),
                                                                            null);
        assertEquals("map should contain one element only", 1, getAllResult.size());
        assertEquals("k1 should not be present",
                     false,
                     getAllResult.containsKey(new ByteArray("k1".getBytes())));
        assertEquals("k4 should be present",
                     true,
                     getAllResult.containsKey(new ByteArray("k4".getBytes())));

        // if online deletes are not configured, we should see the deleted keys
        // in the base bdb store, so the datacleanup job can go and delete them
        assertEquals("k1 should be present",
                     !onlineDeletes,
                     engine.get(new ByteArray("k1".getBytes()), null).size() > 0);
        assertEquals("k2 should be present",
                     !onlineDeletes,
                     engine.get(new ByteArray("k2".getBytes()), null).size() > 0);

        // delete everything for next run
        engine.truncate();
    }

    public void testRetentionEnforcingStore() throws InterruptedException {
        runRetentionEnforcingStoreTest(false);
    }

    public void testRetentionEnforcingStoreOnlineDeletes() throws InterruptedException {
        runRetentionEnforcingStoreTest(true);
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
