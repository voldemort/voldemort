/*
 * Copyright 2012 LinkedIn, Inc
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.cluster.Cluster;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.ServerStoreVerifier;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.server.scheduler.slop.StreamingSlopPusherJob;
import voldemort.server.storage.ScanPermitWrapper;
import voldemort.store.StorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

@SuppressWarnings("unchecked")
public class StreamingSlopPusherTest {

    private VoldemortServer[] servers;
    private Cluster cluster;
    private static final int NUM_SERVERS = 3;
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  100000,
                                                                                  100000,
                                                                                  64 * 1024);
    private MetadataStore metadataStore;
    private Properties props;
    private VoldemortConfig[] configs;

    @Before
    public void setUp() throws Exception {
        cluster = ServerTestUtils.getLocalCluster(NUM_SERVERS);
        servers = new VoldemortServer[NUM_SERVERS];
        props = new Properties();
        metadataStore = ServerTestUtils.createMetadataStore(cluster,
                                                            new StoreDefinitionsMapper().readStoreList(new File(storesXmlfile)));
        props.put("pusher.type", "streaming");
        props.put("slop.frequency.ms", "1000000");

        configs = new VoldemortConfig[NUM_SERVERS];
        for(int i = 0; i < NUM_SERVERS; i++) {
            configs[i] = ServerTestUtils.createServerConfig(true,
                                                            i,
                                                            TestUtils.createTempDir()
                                                                     .getAbsolutePath(),
                                                            null,
                                                            storesXmlfile,
                                                            props);
        }
    }

    // This method may be susceptible to BindException issues due to TOCTOU
    // problem with getLocalCluster.
    private void startServers(int... nodeIds) {
        for(int nodeId: nodeIds) {
            if(nodeId < NUM_SERVERS) {
                servers[nodeId] = ServerTestUtils.startVoldemortServerInMannerThatMayResultInBindException(socketStoreFactory,
                                                                       configs[nodeId],
                                                                       cluster);
            }
        }
    }

    private void stopServers(int... nodeIds) throws IOException {
        for(int nodeId: nodeIds) {
            if(nodeId < NUM_SERVERS)
                ServerTestUtils.stopVoldemortServer(servers[nodeId]);
        }
    }

    private VoldemortServer getVoldemortServer(int nodeId) {
        return servers[nodeId];
    }

    @After
    public void tearDown() {
        socketStoreFactory.close();
    }

    @Test
    public void testFailedServer() throws IOException, InterruptedException {
        startServers(0, 2);

        // Put into slop store 0
        StorageEngine<ByteArray, Slop, byte[]> slopStoreNode0 = getVoldemortServer(0).getStoreRepository()
                                                                                     .getSlopStore()
                                                                                     .asSlopStore();

        // Generate slops for 1
        final List<Versioned<Slop>> entrySet1 = ServerTestUtils.createRandomSlops(1,
                                                                                  50,
                                                                                  "test-replication-memory",
                                                                                  "users",
                                                                                  "test-replication-persistent",
                                                                                  "test-readrepair-memory",
                                                                                  "test-consistent",
                                                                                  "test-consistent-with-pref-list");
        // Generate slops for 2
        final List<Versioned<Slop>> entrySet2 = ServerTestUtils.createRandomSlops(2,
                                                                                  50,
                                                                                  "test-replication-memory",
                                                                                  "users",
                                                                                  "test-replication-persistent",
                                                                                  "test-readrepair-memory",
                                                                                  "test-consistent",
                                                                                  "test-consistent-with-pref-list");

        populateSlops(0, slopStoreNode0, entrySet1, entrySet2);

        StreamingSlopPusherJob pusher = new StreamingSlopPusherJob(getVoldemortServer(0).getStoreRepository(),
                                                                   getVoldemortServer(0).getMetadataStore(),
                                                                   new BannagePeriodFailureDetector(new FailureDetectorConfig().setCluster(cluster)
                                                                                                                               .setStoreVerifier(new ServerStoreVerifier(socketStoreFactory,
                                                                                                                                                                         metadataStore,
                                                                                                                                                                         configs[0]))),
                                                                   configs[0],
                                                                   new ScanPermitWrapper(1));

        pusher.run();

        // Give some time for the slops to go over
        Thread.sleep(2000);

        // Now check if the slops went through and also got deleted
        Iterator<Versioned<Slop>> entryIterator = entrySet2.listIterator();
        while(entryIterator.hasNext()) {
            Versioned<Slop> versionedSlop = entryIterator.next();
            Slop nextSlop = versionedSlop.getValue();
            StorageEngine<ByteArray, byte[], byte[]> store = getVoldemortServer(2).getStoreRepository()
                                                                                  .getStorageEngine(nextSlop.getStoreName());
            if(nextSlop.getOperation().equals(Slop.Operation.PUT)) {
                assertNotSame("entry should be present at store",
                              0,
                              store.get(nextSlop.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(nextSlop.getValue()),
                             new String(store.get(nextSlop.getKey(), null).get(0).getValue()));
            } else if(nextSlop.getOperation().equals(Slop.Operation.DELETE)) {
                assertEquals("entry value should match", 0, store.get(nextSlop.getKey(), null)
                                                                 .size());
            }

            // did it get deleted correctly
            assertEquals("slop should have gone", 0, slopStoreNode0.get(nextSlop.makeKey(), null)
                                                                   .size());
        }

        entryIterator = entrySet1.listIterator();
        while(entryIterator.hasNext()) {
            Versioned<Slop> versionedSlop = entryIterator.next();
            Slop nextSlop = versionedSlop.getValue();
            // did it get deleted correctly
            assertNotSame("slop should be there", 0, slopStoreNode0.get(nextSlop.makeKey(), null)
                                                                   .size());
        }

        // Check counts
        SlopStorageEngine slopEngine = getVoldemortServer(0).getStoreRepository().getSlopStore();
        assertEquals(slopEngine.getOutstandingTotal(), 50);
        assertEquals(slopEngine.getOutstandingByNode().get(1), new Long(50));
        assertEquals(slopEngine.getOutstandingByNode().get(2), new Long(0));

        stopServers(0, 2);
    }

    /**
     * Tests that everything works even if the slops get replayed out of order
     */
    @Test
    @Ignore
    public void testOutOfOrder() throws InterruptedException, IOException {
        startServers(0, 1);

        // Put into slop store 0
        StorageEngine<ByteArray, Slop, byte[]> slopStoreNode0 = getVoldemortServer(0).getStoreRepository()
                                                                                     .getSlopStore()
                                                                                     .asSlopStore();

        // 5 slops for 3 stores => ( key1 [put(value1), delete, put(value2)] AND
        // key2 [put(value2), delete] )
        long keyInt1 = (long) (Math.random() * 1000000000L), keyInt2 = (long) (Math.random() * 10000000L);
        ByteArray key1 = new ByteArray(ByteUtils.getBytes("" + keyInt1, "UTF-8")), key2 = new ByteArray(ByteUtils.getBytes(""
                                                                                                                                   + keyInt2,
                                                                                                                           "UTF-8"));
        byte[] value1 = ByteUtils.getBytes("value-" + new String(key1.get()), "UTF-8"), value2 = ByteUtils.getBytes("value-"
                                                                                                                            + new String(key2.get()),
                                                                                                                    "UTF-8");

        VectorClock vectorClock1 = new VectorClock(), vectorClock2 = new VectorClock();

        List<Versioned<Slop>> entrySet = Lists.newArrayList();
        for(String storeName: Lists.newArrayList("test-replication-memory",
                                                 "users",
                                                 "test-replication-persistent")) {
            entrySet.add(Versioned.value(new Slop(storeName,
                                                  Slop.Operation.PUT,
                                                  key1,
                                                  value1,
                                                  null,
                                                  1,
                                                  new Date()), vectorClock1));
            vectorClock1 = vectorClock1.incremented(0, System.currentTimeMillis());
            entrySet.add(Versioned.value(new Slop(storeName,
                                                  Slop.Operation.DELETE,
                                                  key1,
                                                  null,
                                                  null,
                                                  1,
                                                  new Date()), vectorClock1));
            vectorClock1 = vectorClock1.incremented(0, System.currentTimeMillis());
            entrySet.add(Versioned.value(new Slop(storeName,
                                                  Slop.Operation.PUT,
                                                  key1,
                                                  value2,
                                                  null,
                                                  1,
                                                  new Date()), vectorClock1));

            vectorClock2 = vectorClock2.incremented(0, System.currentTimeMillis());
            entrySet.add(Versioned.value(new Slop(storeName,
                                                  Slop.Operation.PUT,
                                                  key2,
                                                  value2,
                                                  null,
                                                  1,
                                                  new Date()), vectorClock2));
            vectorClock2 = vectorClock2.incremented(0, System.currentTimeMillis());
            entrySet.add(Versioned.value(new Slop(storeName,
                                                  Slop.Operation.DELETE,
                                                  key2,
                                                  null,
                                                  null,
                                                  1,
                                                  new Date()), vectorClock2));

        }

        Collections.shuffle(entrySet);

        // Generate two sub lists to send in batches
        List<Versioned<Slop>> firstSet = entrySet.subList(0, 7), secondSet = entrySet.subList(7, 15);
        populateSlops(0, slopStoreNode0, firstSet);

        StreamingSlopPusherJob pusher = new StreamingSlopPusherJob(getVoldemortServer(0).getStoreRepository(),
                                                                   getVoldemortServer(0).getMetadataStore(),
                                                                   new BannagePeriodFailureDetector(new FailureDetectorConfig().setCluster(cluster)
                                                                                                                               .setStoreVerifier(new ServerStoreVerifier(socketStoreFactory,
                                                                                                                                                                         metadataStore,
                                                                                                                                                                         configs[0]))),
                                                                   configs[0],
                                                                   new ScanPermitWrapper(1));

        pusher.run();

        populateSlops(0, slopStoreNode0, secondSet);

        pusher.run();

        // Give some time for the slops to go over
        Thread.sleep(2000);

        for(String storeName: Lists.newArrayList("test-replication-memory",
                                                 "users",
                                                 "test-replication-persistent")) {
            StorageEngine<ByteArray, byte[], byte[]> store = getVoldemortServer(1).getStoreRepository()
                                                                                  .getStorageEngine(storeName);
            assertEquals(store.get(key1, null).size(), 1);
            assertEquals(ByteUtils.compare(store.get(key1, null).get(0).getValue(), value2), 0);
            assertEquals(store.get(key2, null).size(), 0);
        }
        stopServers(0, 1);
    }

    @Test
    public void testNormalPush() throws InterruptedException, IOException {
        startServers(0, 1);

        // Put into slop store 0
        StorageEngine<ByteArray, Slop, byte[]> slopStoreNode0 = getVoldemortServer(0).getStoreRepository()
                                                                                     .getSlopStore()
                                                                                     .asSlopStore();

        // Generate slops for 1
        final List<Versioned<Slop>> entrySet = ServerTestUtils.createRandomSlops(1,
                                                                                 100,
                                                                                 "test-replication-memory",
                                                                                 "users",
                                                                                 "test-replication-persistent",
                                                                                 "test-readrepair-memory",
                                                                                 "test-consistent",
                                                                                 "test-consistent-with-pref-list");

        populateSlops(0, slopStoreNode0, entrySet);

        StreamingSlopPusherJob pusher = new StreamingSlopPusherJob(getVoldemortServer(0).getStoreRepository(),
                                                                   getVoldemortServer(0).getMetadataStore(),
                                                                   new BannagePeriodFailureDetector(new FailureDetectorConfig().setCluster(cluster)
                                                                                                                               .setStoreVerifier(new ServerStoreVerifier(socketStoreFactory,
                                                                                                                                                                         metadataStore,
                                                                                                                                                                         configs[0]))),
                                                                   configs[0],
                                                                   new ScanPermitWrapper(1));

        pusher.run();

        // Give some time for the slops to go over
        Thread.sleep(2000);

        // Now check if the slops went through and also got deleted
        Iterator<Versioned<Slop>> entryIterator = entrySet.listIterator();
        while(entryIterator.hasNext()) {
            Versioned<Slop> versionedSlop = entryIterator.next();
            Slop nextSlop = versionedSlop.getValue();
            StorageEngine<ByteArray, byte[], byte[]> store = getVoldemortServer(1).getStoreRepository()
                                                                                  .getStorageEngine(nextSlop.getStoreName());
            if(nextSlop.getOperation().equals(Slop.Operation.PUT)) {
                assertNotSame("entry should be present at store",
                              0,
                              store.get(nextSlop.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(nextSlop.getValue()),
                             new String(store.get(nextSlop.getKey(), null).get(0).getValue()));
            } else if(nextSlop.getOperation().equals(Slop.Operation.DELETE)) {
                assertEquals("entry value should match", 0, store.get(nextSlop.getKey(), null)
                                                                 .size());
            }

            // did it get deleted correctly
            assertEquals("slop should have gone", 0, slopStoreNode0.get(nextSlop.makeKey(), null)
                                                                   .size());
        }

        // Check counts
        SlopStorageEngine slopEngine = getVoldemortServer(0).getStoreRepository().getSlopStore();
        assertEquals(slopEngine.getOutstandingTotal(), 0);
        assertEquals(slopEngine.getOutstandingByNode().get(1), new Long(0));
        assertEquals(slopEngine.getOutstandingByNode().get(2), new Long(0));

        stopServers(0, 1);
    }

    /**
     * Given a list of multiple slops and a slop store populate it and then
     * return an iterator over the slops
     * 
     * @param entrySet Multiple list of slops (for different stores) - Important
     *        [assumes all sizes are same]
     * @param slopStore The slop store
     * @return Iterator over slops
     */
    private void populateSlops(int nodeId,
                               StorageEngine<ByteArray, Slop, byte[]> slopStore,
                               List<Versioned<Slop>>... entrySet) {
        int size = entrySet[0].size();
        Iterator entryIterators[] = new Iterator[entrySet.length];
        for(int i = 0; i < entrySet.length; i++) {
            entryIterators[i] = entrySet[i].iterator();
        }
        for(int i = 0; i < size; i++) {
            for(int j = 0; j < entrySet.length; j++) {
                Versioned<Slop> versioned = (Versioned<Slop>) entryIterators[j].next();
                // For all the 'DELETES' first put
                if(versioned.getValue().getOperation() == Slop.Operation.DELETE) {
                    try {
                        StorageEngine<ByteArray, byte[], byte[]> storageEngine = getVoldemortServer(nodeId).getStoreRepository()
                                                                                                           .getStorageEngine(versioned.getValue()
                                                                                                                                      .getStoreName());
                        storageEngine.put(versioned.getValue().getKey(),
                                          Versioned.value(versioned.getValue().getValue(),
                                                          versioned.getVersion()),
                                          null);

                    } catch(ObsoleteVersionException e) {}
                }

                try {
                    slopStore.put(versioned.getValue().makeKey(), versioned, null);
                } catch(ObsoleteVersionException e) {}
            }
        }
    }

    @Test
    public void testNormalPushBothWays() throws InterruptedException, IOException {
        startServers(0, 1);

        // Get both slop stores
        StorageEngine<ByteArray, Slop, byte[]> slopStoreNode0 = getVoldemortServer(0).getStoreRepository()
                                                                                     .getSlopStore()
                                                                                     .asSlopStore();
        StorageEngine<ByteArray, Slop, byte[]> slopStoreNode1 = getVoldemortServer(1).getStoreRepository()
                                                                                     .getSlopStore()
                                                                                     .asSlopStore();

        // Generate slops for 0
        final List<Versioned<Slop>> entrySetNode0 = ServerTestUtils.createRandomSlops(1,
                                                                                      100,
                                                                                      "test-readrepair-memory",
                                                                                      "test-consistent",
                                                                                      "test-consistent-with-pref-list");
        final List<Versioned<Slop>> entrySetNode1 = ServerTestUtils.createRandomSlops(0,
                                                                                      100,
                                                                                      "test-replication-memory",
                                                                                      "users",
                                                                                      "test-replication-persistent");

        // Populated the slop stores
        populateSlops(0, slopStoreNode0, entrySetNode0);
        populateSlops(1, slopStoreNode1, entrySetNode1);

        StreamingSlopPusherJob pusher0 = new StreamingSlopPusherJob(getVoldemortServer(0).getStoreRepository(),
                                                                    getVoldemortServer(0).getMetadataStore(),
                                                                    new BannagePeriodFailureDetector(new FailureDetectorConfig().setCluster(cluster)
                                                                                                                                .setStoreVerifier(new ServerStoreVerifier(socketStoreFactory,
                                                                                                                                                                          metadataStore,
                                                                                                                                                                          configs[0]))),
                                                                    configs[0],
                                                                    new ScanPermitWrapper(1)), pusher1 = new StreamingSlopPusherJob(getVoldemortServer(1).getStoreRepository(),
                                                                                                                                    getVoldemortServer(1).getMetadataStore(),
                                                                                                                                    new BannagePeriodFailureDetector(new FailureDetectorConfig().setCluster(cluster)
                                                                                                                                                                                                .setStoreVerifier(new ServerStoreVerifier(socketStoreFactory,
                                                                                                                                                                                                                                          metadataStore,
                                                                                                                                                                                                                                          configs[1]))),
                                                                                                                                    configs[1],
                                                                                                                                    new ScanPermitWrapper(1));

        pusher0.run();
        pusher1.run();

        // Give some time for the slops to go over
        Thread.sleep(2000);

        // Now check if the slops worked
        Iterator<Versioned<Slop>> entryIterator0 = entrySetNode0.listIterator();
        while(entryIterator0.hasNext()) {
            Versioned<Slop> versionedSlop = entryIterator0.next();
            Slop nextSlop = versionedSlop.getValue();
            StorageEngine<ByteArray, byte[], byte[]> store = getVoldemortServer(1).getStoreRepository()
                                                                                  .getStorageEngine(nextSlop.getStoreName());
            if(nextSlop.getOperation().equals(Slop.Operation.PUT)) {
                assertNotSame("entry should be present at store",
                              0,
                              store.get(nextSlop.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(nextSlop.getValue()),
                             new String(store.get(nextSlop.getKey(), null).get(0).getValue()));
            } else if(nextSlop.getOperation().equals(Slop.Operation.DELETE)) {
                assertEquals("entry value should match", 0, store.get(nextSlop.getKey(), null)
                                                                 .size());
            }
            // did it get deleted correctly
            assertEquals("slop should have gone", 0, slopStoreNode0.get(nextSlop.makeKey(), null)
                                                                   .size());
        }

        Iterator<Versioned<Slop>> entryIterator1 = entrySetNode1.listIterator();
        while(entryIterator1.hasNext()) {
            Versioned<Slop> versionedSlop = entryIterator1.next();
            Slop nextSlop = versionedSlop.getValue();
            StorageEngine<ByteArray, byte[], byte[]> store = getVoldemortServer(0).getStoreRepository()
                                                                                  .getStorageEngine(nextSlop.getStoreName());
            if(nextSlop.getOperation().equals(Slop.Operation.PUT)) {
                assertNotSame("entry should be present at store",
                              0,
                              store.get(nextSlop.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(nextSlop.getValue()),
                             new String(store.get(nextSlop.getKey(), null).get(0).getValue()));
            } else if(nextSlop.getOperation().equals(Slop.Operation.DELETE)) {
                assertEquals("entry value should match", 0, store.get(nextSlop.getKey(), null)
                                                                 .size());
            }
            // did it get deleted correctly
            assertEquals("slop should have gone", 0, slopStoreNode1.get(nextSlop.makeKey(), null)
                                                                   .size());
        }

        // Check counts
        SlopStorageEngine slopEngine = getVoldemortServer(0).getStoreRepository().getSlopStore();
        assertEquals(slopEngine.getOutstandingTotal(), 0);
        assertEquals(slopEngine.getOutstandingByNode().get(1), new Long(0));

        slopEngine = getVoldemortServer(1).getStoreRepository().getSlopStore();
        assertEquals(slopEngine.getOutstandingTotal(), 0);
        assertEquals(slopEngine.getOutstandingByNode().get(0), new Long(0));

        stopServers(0, 1);
    }

    @Test
    public void testServerReplacementWithoutBounce() throws IOException, InterruptedException {
        startServers(0, 2);

        // Put into slop store 0
        StorageEngine<ByteArray, Slop, byte[]> slopStoreNode0 = getVoldemortServer(0).getStoreRepository()
                                                                                     .getSlopStore()
                                                                                     .asSlopStore();

        // Generate slops for 1
        final List<Versioned<Slop>> entrySet1 = ServerTestUtils.createRandomSlops(1,
                                                                                  50,
                                                                                  "test-replication-memory",
                                                                                  "users",
                                                                                  "test-replication-persistent",
                                                                                  "test-readrepair-memory",
                                                                                  "test-consistent",
                                                                                  "test-consistent-with-pref-list");
        // Generate slops for 2
        final List<Versioned<Slop>> entrySet2 = ServerTestUtils.createRandomSlops(2,
                                                                                  50,
                                                                                  "test-replication-memory",
                                                                                  "users",
                                                                                  "test-replication-persistent",
                                                                                  "test-readrepair-memory",
                                                                                  "test-consistent",
                                                                                  "test-consistent-with-pref-list");

        populateSlops(0, slopStoreNode0, entrySet1, entrySet2);

        StreamingSlopPusherJob pusher = new StreamingSlopPusherJob(getVoldemortServer(0).getStoreRepository(),
                                                                   getVoldemortServer(0).getMetadataStore(),
                                                                   new BannagePeriodFailureDetector(new FailureDetectorConfig().setCluster(cluster)
                                                                                                                               .setStoreVerifier(new ServerStoreVerifier(socketStoreFactory,
                                                                                                                                                                         metadataStore,
                                                                                                                                                                         configs[0]))),
                                                                   configs[0],
                                                                   new ScanPermitWrapper(1));

        pusher.run();

        // Give some time for the slops to go over
        Thread.sleep(10000);

        // Now check if the slops went through and also got deleted
        Iterator<Versioned<Slop>> entryIterator = entrySet2.listIterator();
        while(entryIterator.hasNext()) {
            Versioned<Slop> versionedSlop = entryIterator.next();
            Slop nextSlop = versionedSlop.getValue();
            StorageEngine<ByteArray, byte[], byte[]> store = getVoldemortServer(2).getStoreRepository()
                                                                                  .getStorageEngine(nextSlop.getStoreName());
            if(nextSlop.getOperation().equals(Slop.Operation.PUT)) {
                assertNotSame("entry should be present at store",
                              0,
                              store.get(nextSlop.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(nextSlop.getValue()),
                             new String(store.get(nextSlop.getKey(), null).get(0).getValue()));
            } else if(nextSlop.getOperation().equals(Slop.Operation.DELETE)) {
                assertEquals("entry value should match", 0, store.get(nextSlop.getKey(), null)
                                                                 .size());
            }

            // did it get deleted correctly
            assertEquals("slop should have gone", 0, slopStoreNode0.get(nextSlop.makeKey(), null)
                                                                   .size());
        }

        entryIterator = entrySet1.listIterator();
        while(entryIterator.hasNext()) {
            Versioned<Slop> versionedSlop = entryIterator.next();
            Slop nextSlop = versionedSlop.getValue();
            // did it get deleted correctly
            assertNotSame("slop should be there", 0, slopStoreNode0.get(nextSlop.makeKey(), null)
                                                                   .size());
        }

        // Check counts
        SlopStorageEngine slopEngine = getVoldemortServer(0).getStoreRepository().getSlopStore();
        assertEquals(slopEngine.getOutstandingTotal(), 50);
        assertEquals(slopEngine.getOutstandingByNode().get(1), new Long(50));
        assertEquals(slopEngine.getOutstandingByNode().get(2), new Long(0));

        // now replace server 1 with a new host and start it
        cluster = ServerTestUtils.updateClusterWithNewHost(cluster, 1);
        startServers(1);

        // update the meatadata store with the new cluster on node 0 and 2 (the
        // two servers that are running)
        servers[0].getMetadataStore().put(MetadataStore.CLUSTER_KEY, cluster);
        servers[2].getMetadataStore().put(MetadataStore.CLUSTER_KEY, cluster);

        // Give some time for the pusher job to figure out that server1 is up
        Thread.sleep(35000);

        // start the pusher job again
        pusher.run();

        // Give some time for the slops to go over
        Thread.sleep(10000);

        // make sure the slot for server 1 is pushed to the new host
        // Now check if the slops went through and also got deleted
        entryIterator = entrySet1.listIterator();
        while(entryIterator.hasNext()) {
            Versioned<Slop> versionedSlop = entryIterator.next();
            Slop nextSlop = versionedSlop.getValue();
            StorageEngine<ByteArray, byte[], byte[]> store = getVoldemortServer(1).getStoreRepository()
                                                                                  .getStorageEngine(nextSlop.getStoreName());
            if(nextSlop.getOperation().equals(Slop.Operation.PUT)) {
                assertNotSame("entry should be present at store",
                              0,
                              store.get(nextSlop.getKey(), null).size());
                assertEquals("entry value should match",
                             new String(nextSlop.getValue()),
                             new String(store.get(nextSlop.getKey(), null).get(0).getValue()));
            } else if(nextSlop.getOperation().equals(Slop.Operation.DELETE)) {
                assertEquals("entry value should match", 0, store.get(nextSlop.getKey(), null)
                                                                 .size());
            }

            // did it get deleted correctly
            assertEquals("slop should have gone", 0, slopStoreNode0.get(nextSlop.makeKey(), null)
                                                                   .size());
        }

    }
}