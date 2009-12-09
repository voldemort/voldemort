package voldemort.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.SocketRequestHandlerFactory;
import voldemort.server.socket.AdminService;
import voldemort.server.socket.SocketService;
import voldemort.store.RandomlyFailingDelegatingStore;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.UnreachableStoreException;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

import com.google.common.collect.AbstractIterator;

/**
 * Test to check streaming behavior under failure.
 * 
 * @author bbansal
 * 
 */
public class AdminServiceFailureTest extends TestCase {

    private static int TEST_KEYS = 10000;
    private static double FAIL_PROBABILITY = 0.60;

    private AdminClient adminClient;
    private Cluster cluster;
    private SocketService adminServer;
    StorageEngine<ByteArray, byte[]> failingStorageEngine;
    private Thread thread;

    private enum StreamOperations {
        FETCH_ENTRIES,
        FETCH_KEYS,
        DELETE_PARTITIONS,
        UPDATE_ENTRIES
    }

    @Override
    public void setUp() throws IOException {
        cluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 2, 3 }, { 4, 5, 6, 7 } });
        List<StoreDefinition> storeDefs = ServerTestUtils.getStoreDefs(1);

        failingStorageEngine = new RandomlyFailingDelegatingStore<ByteArray, byte[]>(new InMemoryStorageEngine<ByteArray, byte[]>(storeDefs.get(0)
                                                                                                                                           .getName()));
        adminServer = getAdminServer(cluster.getNodeById(0),
                                     cluster,
                                     storeDefs,
                                     failingStorageEngine);
        adminClient = ServerTestUtils.getAdminClient(cluster);
        adminServer.start();
    }

    private AdminService getAdminServer(Node node,
                                        Cluster cluster,
                                        List<StoreDefinition> storeDefs,
                                        StorageEngine<ByteArray, byte[]> storageEngine)
            throws IOException {
        StoreRepository storeRepository = new StoreRepository();
        storeRepository.addStorageEngine(storageEngine);
        storeRepository.addLocalStore(storageEngine);

        return new AdminService(new SocketRequestHandlerFactory(storeRepository,
                                                                ServerTestUtils.createMetadataStore(cluster,
                                                                                                    storeDefs),
                                                                ServerTestUtils.createServerConfig(0,
                                                                                                   TestUtils.createTempDir()
                                                                                                            .getAbsolutePath(),
                                                                                                   null,
                                                                                                   null),
                                                                null),
                                node.getAdminPort(),
                                2,
                                2,
                                10000,
                                "test-admin-service",
                                false);
    }

    @Override
    public void tearDown() throws IOException {
        try {
            adminServer.stop();
            adminClient.stop();
        } catch(Exception e) {
            // ignore
        }
    }

    protected AdminClient getAdminClient() {
        return adminClient;
    }

    public void testWithStartFailure() {
        // put some entries in store
        for(StreamOperations operation: StreamOperations.values()) {
            adminServer.stop();
            try {
                doOperation(operation, 0, failingStorageEngine.getName(), Arrays.asList(0, 1));
                fail();
            } catch(UnreachableStoreException e) {
                // ignore
            }
        }
    }

    // client side should get exceptions from servers
    public void testFailures() {

        for(StreamOperations operation: StreamOperations.values()) {
            try {
                doOperation(operation, 0, failingStorageEngine.getName(), Arrays.asList(0, 1));
                fail("Unit test should fail here !!");
            } catch(Exception e) {
                // ignore
            }
        }
    }

    private void doOperation(StreamOperations e,
                             int nodeId,
                             String storeName,
                             List<Integer> partitionList) {
        switch(e) {
            case DELETE_PARTITIONS:
                putAlltoStore();
                getAdminClient().deletePartitions(nodeId, storeName, partitionList, null);
                return;
            case FETCH_ENTRIES:
                putAlltoStore();
                consumeIterator(getAdminClient().fetchPartitionEntries(nodeId,
                                                                       storeName,
                                                                       partitionList,
                                                                       null));
                return;
            case FETCH_KEYS:
                putAlltoStore();
                consumeIterator(getAdminClient().fetchPartitionKeys(nodeId,
                                                                    storeName,
                                                                    partitionList,
                                                                    null));
                return;
            case UPDATE_ENTRIES:
                getAdminClient().updateEntries(nodeId,
                                               storeName,
                                               getRandomlyFailingIterator(ServerTestUtils.createRandomKeyValuePairs(TEST_KEYS)),
                                               null);
                return;
            default:
                throw new RuntimeException("Unknown operation");
        }
    }

    private <K> void consumeIterator(Iterator<K> iterator) {
        while(iterator.hasNext())
            iterator.next();
    }

    private void putAlltoStore() {
        for(Entry<ByteArray, byte[]> entry: ServerTestUtils.createRandomKeyValuePairs(TEST_KEYS)
                                                           .entrySet()) {
            try {
                failingStorageEngine.put(entry.getKey(), new Versioned<byte[]>(entry.getValue()));
            } catch(Exception e) {
                // ignore
            }
        }
    }

    private Iterator<Pair<ByteArray, Versioned<byte[]>>> getRandomlyFailingIterator(final Map<ByteArray, byte[]> entryMap) {
        return new AbstractIterator<Pair<ByteArray, Versioned<byte[]>>>() {

            private final Iterator<Entry<ByteArray, byte[]>> innerIterator = entryMap.entrySet()
                                                                                     .iterator();

            @Override
            protected Pair<ByteArray, Versioned<byte[]>> computeNext() {
                if(Math.random() > FAIL_PROBABILITY) {
                    throw new VoldemortException("Failing Iterator.");
                }

                if(innerIterator.hasNext())
                    return endOfData();

                Entry<ByteArray, byte[]> entry = innerIterator.next();
                return new Pair<ByteArray, Versioned<byte[]>>(entry.getKey(),
                                                              new Versioned<byte[]>(entry.getValue()));
            }
        };
    }
}
