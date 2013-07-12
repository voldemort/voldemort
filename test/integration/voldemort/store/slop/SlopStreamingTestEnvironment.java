package voldemort.store.slop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import voldemort.VoldemortTestConstants;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.protocol.admin.SlopWrappingStreamingClient;
import voldemort.client.protocol.admin.StreamingClientConfig;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryPutAssertionStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;

public class SlopStreamingTestEnvironment extends HintedHandoffTestEnvironment {

    private final Logger logger = Logger.getLogger(SlopStreamingTestEnvironment.class);

    String bootstrapUrl;

    int numFailedNodes;
    boolean throwIntermittentException = false;

    protected final CountDownLatch waitForFaultyNodeLatch = new CountDownLatch(1);

    public SlopStreamingTestEnvironment(int numFailedNodes, boolean throwIntermittentException) {
        super();
        this.numFailedNodes = numFailedNodes;
        this.throwIntermittentException = throwIntermittentException;
    }

    public SlopWrappingStreamingClient makeSlopStreamingClient() throws InterruptedException {
        return makeSlopStreamingClient(false);
    }

    public SlopWrappingStreamingClient makeSlopStreamingClient(boolean wrapException)
            throws InterruptedException {

        startFinishLatch.await();
        if(throwIntermittentException)
            waitForFaultyNodeLatch.await();
        Props property = new Props();
        property.put("streaming.platform.bootstrapURL", bootstrapUrl);
        StreamingClientConfig config = new StreamingClientConfig(property);
        SlopWrappingStreamingClient streamer = new SlopWrappingStreamingClient(config);

        Callable<Integer> cpCallable = new CheckpointCallable();
        Callable<Integer> rbCallable = new RollbackCallable();
        List<String> stores = new ArrayList();

        stores.add(STORE_NAME);
        List<Integer> failedNodes = new ArrayList<Integer>();

        for(int i = 0; i < numFailedNodes; i++)
            failedNodes.add(i + 1);
        if(!wrapException) {
            streamer.initStreamingSessions(stores, cpCallable, rbCallable, true, failedNodes);
        }

        else {
            try {
                streamer.initStreamingSessions(stores, cpCallable, rbCallable, true, failedNodes);
            } catch(Exception e) {
                logger.error(e);
            }
        }
        return streamer;

    }

    @Override
    public void run() {
        Random random = new Random(System.currentTimeMillis());
        cluster = VoldemortTestConstants.getEightNodeClusterWithZones();
        bootstrapUrl = cluster.getNodeById(0).getSocketUrl().toString();
        storeDef = storeDefBuilder.build();
        // setup store engines
        for(Integer nodeId = 0; nodeId < NUM_NODES_TOTAL; nodeId++) {
            createInnerStore(nodeId); // do only once
        }

        for(Integer nodeId = 0; nodeId < NUM_NODES_TOTAL; nodeId++) {
            try {
                startServer(nodeId);
            } catch(IOException e) {
                logger.error("Server " + nodeId + "failed to start", e);
            }
        }

        factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));

        // wait for start of servers
        startFinishLatch.countDown();

        try {

            Set<Integer> nodesToFail = new HashSet<Integer>();
            nodesToFail.add(1);
            boolean wrapUpSignal = false;

            if(logger.isInfoEnabled()) {
                if(wrapUpSignal) {
                    logger.info("Wake Up and wrap up. Make all servers NORMAL");
                } else {
                    logger.info("Wake Up and decide new failure statuses");
                }
                for(Map.Entry<Integer, Store<ByteArray, byte[], byte[]>> entry: realStores.entrySet()) {
                    InMemoryPutAssertionStorageEngine<ByteArray, byte[], byte[]> engine = (InMemoryPutAssertionStorageEngine<ByteArray, byte[], byte[]>) entry.getValue();
                    logger.info("Outstanding Put Assertions of node [" + entry.getKey() + "]: "
                                + engine.getFailedAssertions().size());
                }
            }

            if(logger.isInfoEnabled()) {
                logger.info("Setting nodes to Fail: " + nodesToFail.toString());
            }
            for(Integer nodeId = 0; nodeId < NUM_NODES_TOTAL; nodeId++) {
                makeNodeNormal(nodeId);
            }

            for(Integer nodeId = 0; nodeId < NUM_NODES_TOTAL; nodeId++) {
                if(nodesToFail.contains(nodeId)) {
                    // fail a node if it's normal
                    if(nodesStatus.get(nodeId) == NodeStatus.NORMAL) {

                        makeNodeDown(nodeId);

                    }

                } else {
                    // make node normal if not normal
                    if(nodesStatus.get(nodeId) != NodeStatus.NORMAL) {
                        makeNodeNormal(nodeId);
                    }

                }
            }
            if(throwIntermittentException)
                waitForFaultyNodeLatch.countDown();

            if(!throwIntermittentException) {
                for(Integer nodeId = 0; nodeId < NUM_NODES_TOTAL; nodeId++) {
                    makeNodeNormal(nodeId);
                }
            }

        } catch(Exception e) {} finally {
            wrapUpFinishLatch.countDown();
        }
    }

    class CheckpointCallable implements Callable {

        public Integer call() {
            logger.debug("checkpoint!");
            return 1;
        }
    }

    class RollbackCallable implements Callable {

        public Integer call() {
            logger.debug("rollback!");
            return 1;
        }
    }
}
