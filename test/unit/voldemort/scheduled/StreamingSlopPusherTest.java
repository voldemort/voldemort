package voldemort.scheduled;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.junit.After;
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
import voldemort.store.StorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

@SuppressWarnings("unchecked")
public class StreamingSlopPusherTest extends TestCase {

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

    @Override
    protected void setUp() throws Exception {
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

    private void startServers(int... nodeIds) {
        for(int nodeId: nodeIds) {
            if(nodeId < NUM_SERVERS) {
                servers[nodeId] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
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

    @Override
    @After
    public void tearDown() throws IOException, InterruptedException {
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
        final List<Slop> entrySet1 = ServerTestUtils.createRandomSlops(1,
                                                                       50,
                                                                       "test-replication-memory",
                                                                       "users",
                                                                       "test-replication-persistent",
                                                                       "test-readrepair-memory",
                                                                       "test-consistent",
                                                                       "test-consistent-with-pref-list");
        // Generate slops for 2
        final List<Slop> entrySet2 = ServerTestUtils.createRandomSlops(2,
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
                                                                   new BannagePeriodFailureDetector(new FailureDetectorConfig().setNodes(cluster.getNodes())
                                                                                                                               .setStoreVerifier(new ServerStoreVerifier(socketStoreFactory,
                                                                                                                                                                         metadataStore,
                                                                                                                                                                         configs[0]))),
                                                                   configs[0]);

        pusher.run();

        // Give some time for the slops to go over
        Thread.sleep(2000);

        // Now check if the slops went through and also got deleted
        Iterator<Slop> entryIterator = entrySet2.listIterator();
        while(entryIterator.hasNext()) {
            Slop nextSlop = entryIterator.next();
            StorageEngine<ByteArray, byte[], byte[]> store = getVoldemortServer(2).getStoreRepository()
                                                                                  .getStorageEngine(nextSlop.getStoreName());
            if(nextSlop.getOperation().equals(Slop.Operation.PUT)) {
                assertNotSame("entry should be present at store", 0, store.get(nextSlop.getKey(),
                                                                               null).size());
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
            Slop nextSlop = entryIterator.next();
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

    @Test
    public void testNormalPush() throws InterruptedException, IOException {
        startServers(0, 1);

        // Put into slop store 0
        StorageEngine<ByteArray, Slop, byte[]> slopStoreNode0 = getVoldemortServer(0).getStoreRepository()
                                                                                     .getSlopStore()
                                                                                     .asSlopStore();

        // Generate slops for 1
        final List<Slop> entrySet = ServerTestUtils.createRandomSlops(1,
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
                                                                   new BannagePeriodFailureDetector(new FailureDetectorConfig().setNodes(cluster.getNodes())
                                                                                                                               .setStoreVerifier(new ServerStoreVerifier(socketStoreFactory,
                                                                                                                                                                         metadataStore,
                                                                                                                                                                         configs[0]))),
                                                                   configs[0]);

        pusher.run();

        // Give some time for the slops to go over
        Thread.sleep(2000);

        // Now check if the slops went through and also got deleted
        Iterator<Slop> entryIterator = entrySet.listIterator();
        while(entryIterator.hasNext()) {
            Slop nextSlop = entryIterator.next();
            StorageEngine<ByteArray, byte[], byte[]> store = getVoldemortServer(1).getStoreRepository()
                                                                                  .getStorageEngine(nextSlop.getStoreName());
            if(nextSlop.getOperation().equals(Slop.Operation.PUT)) {
                assertNotSame("entry should be present at store", 0, store.get(nextSlop.getKey(),
                                                                               null).size());
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
                               List<Slop>... entrySet) {
        int size = entrySet[0].size();
        Iterator entryIterators[] = new Iterator[entrySet.length];
        for(int i = 0; i < entrySet.length; i++) {
            entryIterators[i] = entrySet[i].iterator();
        }
        for(int i = 0; i < size; i++) {
            for(int j = 0; j < entrySet.length; j++) {
                Slop slop = (Slop) entryIterators[j].next();
                // For all the 'DELETES' first put
                if(slop.getOperation() == Slop.Operation.DELETE) {
                    try {
                        StorageEngine<ByteArray, byte[], byte[]> storageEngine = getVoldemortServer(nodeId).getStoreRepository()
                                                                                                           .getStorageEngine(slop.getStoreName());
                        storageEngine.put(slop.getKey(), Versioned.value(slop.getValue()), null);
                    } catch(ObsoleteVersionException e) {}
                }

                try {
                    slopStore.put(slop.makeKey(), Versioned.value(slop), null);
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
        final List<Slop> entrySetNode0 = ServerTestUtils.createRandomSlops(1,
                                                                           100,
                                                                           "test-readrepair-memory",
                                                                           "test-consistent",
                                                                           "test-consistent-with-pref-list");
        final List<Slop> entrySetNode1 = ServerTestUtils.createRandomSlops(0,
                                                                           100,
                                                                           "test-replication-memory",
                                                                           "users",
                                                                           "test-replication-persistent");

        // Populated the slop stores
        populateSlops(0, slopStoreNode0, entrySetNode0);
        populateSlops(1, slopStoreNode1, entrySetNode1);

        StreamingSlopPusherJob pusher0 = new StreamingSlopPusherJob(getVoldemortServer(0).getStoreRepository(),
                                                                    getVoldemortServer(0).getMetadataStore(),
                                                                    new BannagePeriodFailureDetector(new FailureDetectorConfig().setNodes(cluster.getNodes())
                                                                                                                                .setStoreVerifier(new ServerStoreVerifier(socketStoreFactory,
                                                                                                                                                                          metadataStore,
                                                                                                                                                                          configs[0]))),
                                                                    configs[0]), pusher1 = new StreamingSlopPusherJob(getVoldemortServer(1).getStoreRepository(),
                                                                                                                      getVoldemortServer(1).getMetadataStore(),
                                                                                                                      new BannagePeriodFailureDetector(new FailureDetectorConfig().setNodes(cluster.getNodes())
                                                                                                                                                                                  .setStoreVerifier(new ServerStoreVerifier(socketStoreFactory,
                                                                                                                                                                                                                            metadataStore,
                                                                                                                                                                                                                            configs[1]))),
                                                                                                                      configs[1]);

        pusher0.run();
        pusher1.run();

        // Give some time for the slops to go over
        Thread.sleep(2000);

        // Now check if the slops worked
        Iterator<Slop> entryIterator0 = entrySetNode0.listIterator();
        while(entryIterator0.hasNext()) {
            Slop nextSlop = entryIterator0.next();
            StorageEngine<ByteArray, byte[], byte[]> store = getVoldemortServer(1).getStoreRepository()
                                                                                  .getStorageEngine(nextSlop.getStoreName());
            if(nextSlop.getOperation().equals(Slop.Operation.PUT)) {
                assertNotSame("entry should be present at store", 0, store.get(nextSlop.getKey(),
                                                                               null).size());
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

        Iterator<Slop> entryIterator1 = entrySetNode1.listIterator();
        while(entryIterator1.hasNext()) {
            Slop nextSlop = entryIterator1.next();
            StorageEngine<ByteArray, byte[], byte[]> store = getVoldemortServer(0).getStoreRepository()
                                                                                  .getStorageEngine(nextSlop.getStoreName());
            if(nextSlop.getOperation().equals(Slop.Operation.PUT)) {
                assertNotSame("entry should be present at store", 0, store.get(nextSlop.getKey(),
                                                                               null).size());
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
}