package voldemort.store.slop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import voldemort.TestUtils;
import voldemort.client.StoreClient;
import voldemort.cluster.Node;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryPutAssertionStorageEngine;
import voldemort.utils.ByteArray;

@RunWith(Parameterized.class)
public class LongHintedHandoffTest {

    private final Logger logger = Logger.getLogger(LongHintedHandoffTest.class);
    private static final Long MAX_TOTAL_TIME_MS = 1000L * 60 * 10;
    private static final Integer KEY_LENGTH = 16;
    private static final Integer VALUE_LENGTH = 32;
    private HintedHandoffTestEnvironment testEnv;
    private final Integer replicationFactor;
    private final Integer requiredWrites;
    private final Integer preferredWrites;

    public LongHintedHandoffTest(int replicationFactor, int requiredWrites, int preferredWrites) {
        this.replicationFactor = replicationFactor;
        this.requiredWrites = requiredWrites;
        this.preferredWrites = preferredWrites;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { 3, 1, 1 }, { 3, 1, 2 }, { 3, 1, 3 }, { 3, 2, 2 },
                { 3, 2, 3 }, { 3, 3, 3 }, { 2, 1, 1 }, { 2, 1, 2 }, { 2, 2, 2 } });
    }

    @Before
    public void setUp() throws InterruptedException {
        testEnv = new HintedHandoffTestEnvironment();
        testEnv.setReplicationFactor(replicationFactor)
               .setPreferredWrite(preferredWrites)
               .setRequiredWrite(requiredWrites);
        testEnv.start();
    }

    @After
    public void tearDown() {
        testEnv.stop();
        logger.info("Stopped all servers");
    }

    @Test
    public void testHintedHandoff() throws InterruptedException {
        Set<Integer> nodeIds = new HashSet<Integer>();
        long startMs = System.currentTimeMillis();
        long endMs = startMs + MAX_TOTAL_TIME_MS;
        long totalPuts = 0;
        long numRejectedPuts = 0;
        long numAssertPuts = 0;

        StoreClient<byte[], byte[]> client = testEnv.makeClient();
        while(true) {
            if(System.currentTimeMillis() > endMs) {
                break;
            }
            // generate key
            ByteArray key = new ByteArray(TestUtils.randomBytes(KEY_LENGTH));
            byte[] value = TestUtils.randomBytes(VALUE_LENGTH);
            // put to nodes
            try {
                client.put(key.get(), value);
                // if put does not throw exception
                List<Node> routes = testEnv.routeRequest(key.get());
                for(Node node: routes) {
                    numAssertPuts++;
                    nodeIds.add(node.getId());
                    Store<ByteArray, byte[], byte[]> realStore = testEnv.getRealStore(node.getId());
                    if(realStore instanceof InMemoryPutAssertionStorageEngine) {
                        ((InMemoryPutAssertionStorageEngine<ByteArray, byte[], byte[]>) realStore).assertPut(key);
                    } else {
                        fail("realStore is not InMemoryPutAssertionStorageEngine. Test setup is wrong");
                    }
                }
            } catch(InsufficientOperationalNodesException e) {
                numRejectedPuts++;
                if(logger.isDebugEnabled()) {
                    logger.debug("Key " + key + " is rejected for InsufficientOperationalNodes");
                }
            } finally {
                totalPuts++;
            }
        }

        // bring all servers up
        testEnv.waitForWrapUp();

        // check
        long numFailedAssertions = 0;
        for(Integer nodeId: nodeIds) {
            Store<ByteArray, byte[], byte[]> realStore = testEnv.getRealStore(nodeId);
            if(realStore instanceof InMemoryPutAssertionStorageEngine) {
                Set<ByteArray> keys = ((InMemoryPutAssertionStorageEngine<ByteArray, byte[], byte[]>) realStore).getFailedAssertions();
                for(ByteArray key: keys) {
                    logger.error("key [" + key + "] is asserted but not recorded on node ["
                                 + nodeId + "]");
                }
                numFailedAssertions += keys.size();
            } else {
                fail("realStore is not InMemoryPutAssertionStorageEngine");
            }
        }

        logger.info("Total Client Puts Rejected (InsufficientOperationalNodes): " + numRejectedPuts);
        logger.info("Total Client Put Operations: " + totalPuts);
        logger.info("Total Server Put Assertions: " + numAssertPuts);
        logger.info("Total Server Put Lost: " + numFailedAssertions);

        assertEquals(numFailedAssertions + " server puts are lost: " + numFailedAssertions,
                     0L,
                     numFailedAssertions);
    }
}
