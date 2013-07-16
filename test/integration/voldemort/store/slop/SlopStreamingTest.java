package voldemort.store.slop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.protocol.admin.StreamingClient;
import voldemort.cluster.Node;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryPutAssertionStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

/**
 * Integration tests for the slop wrapping streaming client
 * 
 */
public class SlopStreamingTest {

    private final Logger logger = Logger.getLogger(LongHintedHandoffTest.class);
    private static final Long MAX_TOTAL_TIME_MS = 1000L * 15;
    private static final Integer KEY_LENGTH = 16;
    private static final Integer VALUE_LENGTH = 32;
    private SlopStreamingTestEnvironment testEnv;
    StreamingClient streamer;

    @After
    public void tearDown() {
        try {
            testEnv.warpUp();
        } catch(InterruptedException e1) {
            logger.warn(e1);
        }
        testEnv.stop();
        logger.info("Stopped all servers");

        try {
            streamer.closeStreamingSessions();
        } catch(Exception e) {
            logger.warn(e);
        }
    }

    /*
     * Does streaming puts with one node marked down. Then we drain all slops
     * and ensure all keys end up on the right nodes
     */
    @Test
    public void testSlopStreaming() throws InterruptedException {

        testEnv = new SlopStreamingTestEnvironment(1, false);
        // 3-2-2 setup
        testEnv.setPreferredWrite(2).setRequiredWrite(2);

        testEnv.start();

        Set<Integer> nodeIds = new HashSet<Integer>();
        long startMs = System.currentTimeMillis();
        long endMs = startMs + MAX_TOTAL_TIME_MS;
        long totalPuts = 0;
        long numRejectedPuts = 0;
        long numAssertPuts = 0;

        streamer = testEnv.makeSlopStreamingClient();

        while(true) {
            if(System.currentTimeMillis() > endMs) {
                break;
            }
            // generate key
            ByteArray key = new ByteArray(TestUtils.randomBytes(KEY_LENGTH));
            byte[] value = TestUtils.randomBytes(VALUE_LENGTH);
            Versioned<byte[]> outputValue = Versioned.value(value);
            // put to nodes
            try {
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
                streamer.streamingPut(key, outputValue, testEnv.STORE_NAME);
            } catch(InsufficientOperationalNodesException e) {
                numRejectedPuts++;
                if(logger.isDebugEnabled()) {
                    logger.debug("Key " + key + " is rejected for InsufficientOperationalNodes");
                }
            } finally {
                totalPuts++;
            }
        }

        streamer.commitToVoldemort();

        streamer.closeStreamingSessions();

        // bring all servers up
        testEnv.warpUp();

        try {
            streamer.commitToVoldemort();
            streamer.closeStreamingSessions();
        } catch(Exception e) {
            logger.warn(e);
        }
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

    /*
     * Check if we throw exception when we have more than 1 node down
     */
    @Test(expected = InsufficientOperationalNodesException.class)
    public void testSlopStreamingMultipleNodesUnavailable() throws InterruptedException {

        testEnv = new SlopStreamingTestEnvironment(2, false);
        // 3-2-2 setup
        testEnv.setPreferredWrite(2).setRequiredWrite(2);

        testEnv.start();

        streamer = testEnv.makeSlopStreamingClient();

    }

    /*
     * Check if we throw an exception when a node is down and we did not supply
     * it
     */
    @Test(expected = VoldemortException.class)
    public void testSlopStreamingInitializeWithFaultyNode() throws InterruptedException {

        testEnv = new SlopStreamingTestEnvironment(0, true);
        // 3-2-2 setup
        testEnv.setPreferredWrite(2).setRequiredWrite(2);

        testEnv.start();

        streamer = testEnv.makeSlopStreamingClient();

    }

    /*
     * Check if we detect the number of faulty nodes correctly
     */
    @Test
    public void testNumFaultyNode() throws InterruptedException {

        testEnv = new SlopStreamingTestEnvironment(0, true);
        // 3-2-2 setup
        testEnv.setPreferredWrite(2).setRequiredWrite(2);

        testEnv.start();

        streamer = testEnv.makeSlopStreamingClient(true);

        org.junit.Assert.assertTrue(streamer.getFaultyNodes().size() > 0);

    }
}
