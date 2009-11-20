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
import voldemort.server.socket.SocketService;
import voldemort.store.FailingDelegatingStore;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
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

    private enum StreamOperations {
        FETCH_ENTRIES,
        FETCH_KEYS,
        DELETE_PARTITIONS,
        UPDATE_ENTRIES
    };

    @Override
    public void setUp() throws IOException {
        cluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 2, 3 }, { 4, 5, 6, 7 } });
        List<StoreDefinition> storeDefs = ServerTestUtils.getStoreDefs(1);

        failingStorageEngine = new FailingDelegatingStore<ByteArray, byte[]>(new InMemoryStorageEngine<ByteArray, byte[]>(storeDefs.get(0)
                                                                                                                                   .getName()));
        adminServer = getAdminServer(cluster.getNodeById(0),
                                     cluster,
                                     storeDefs,
                                     failingStorageEngine);
        adminServer.start();
        adminClient = ServerTestUtils.getAdminClient(cluster);
    }

    private SocketService getAdminServer(Node node,
                                         Cluster cluster,
                                         List<StoreDefinition> storeDefs,
                                         Store<ByteArray, byte[]> storageEngine) throws IOException {
        StoreRepository storeRepository = new StoreRepository();
        storeRepository.addLocalStore(storageEngine);

        return new SocketService(new SocketRequestHandlerFactory(storeRepository,
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

    public void testFetchWithFailingStore() {
        try {
            doOperation(StreamOperations.FETCH_ENTRIES,
                        0,
                        failingStorageEngine.getName(),
                        Arrays.asList(0, 1));
            fail();
        } catch(UnreachableStoreException e) {
            // ignore
        }
    }

    private void doOperation(StreamOperations e, int nodeId, String storeName, List<Integer> partitionList) {
        switch(e) {
            case DELETE_PARTITIONS:
                putAlltoStore(nodeId, storeName);
                getAdminClient().deletePartitions(nodeId, storeName, partitionList, null);
                return;
            case FETCH_ENTRIES:
                putAlltoStore(nodeId, storeName);
                getAdminClient().fetchPartitionEntries(nodeId, storeName, partitionList, null);
                return;
            case FETCH_KEYS:
                putAlltoStore(nodeId, storeName);
                getAdminClient().fetchPartitionKeys(nodeId, storeName, partitionList, null);
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

    private void putAlltoStore(int nodeId, String storeName) {
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
