package voldemort.client.protocol.admin;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.cluster.Node;
import voldemort.serialization.SlopSerializer;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.slop.Slop;
import voldemort.store.slop.strategy.HandoffToAnyStrategy;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

/**
 * 
 * This class is a wrapper on the StreamingClient It serves the purpose of
 * making streaming resilient to single node failures and making it highly
 * available.
 * 
 * The StreamingClient stops streaming when a voldemort node is down
 * 
 * The streaming client interface would expect the data producer(eg: faust) to
 * provide the failed list of nodes upfront. On a node failure the streaming
 * client also provides the list of faulty nodes which can be used by the data
 * producer to reinitialize the streaming session.
 * 
 * Why did we choose this design: ==> The streaming session does not buffer the
 * entries, it just keeps them on a network buffer, if a node fails during this
 * time, the following flush() call will yield and generate an exception Only
 * the data producer can resend those events, thereby re-initializing the
 * session
 * 
 */
public class StreamingClient extends BaseStreamingClient {

    private static final Logger logger = Logger.getLogger(StreamingClient.class);

    private int MAX_FAULTY_NODES = 1;

    private static final String SLOP_STORE = "slop";

    // list of stores to be streamed
    List<String> stores;

    @SuppressWarnings("rawtypes")
    private Callable stubCheckpointCallback;
    @SuppressWarnings("rawtypes")
    private Callable stubRecoveryCallback;

    // delta push or swap for read-only like store
    private boolean allowMerge;

    // SLOPS have a special serializer
    private SlopSerializer slopSerializer = new SlopSerializer();

    // used for callbacks
    private ExecutorService streamingSlopResults;

    private Random generator = new Random();

    public StreamingClient(StreamingClientConfig config) {
        super(config);
        MAX_FAULTY_NODES = config.getFailedNodesTolerated();
        logger.info("StreamingClient constructed with MAX_FAULTY_NODES set to : " + MAX_FAULTY_NODES);
        streamingSlopResults = Executors.newFixedThreadPool(1);

    }

    @Override
    @SuppressWarnings("rawtypes")
    public synchronized void initStreamingSessions(List<String> stores,
                                                   final Callable checkpointCallback,
                                                   final Callable recoveryCallback,
                                                   boolean allowMerge,
                                                   List<Integer> blackListedNodes) {

        this.stores = stores;
        stubCheckpointCallback = checkpointCallback;
        stubRecoveryCallback = recoveryCallback;
        this.blackListedNodes = blackListedNodes;

        initializeWithFailedNodes();
    }

    @Override
    public synchronized void streamingPut(ByteArray key, Versioned<byte[]> value, String storeName) {

        if(!storeName.equals(SLOP_STORE)) {
            try {
                super.streamingPut(key, value, storeName);
            } catch(Exception e) {
                // call failed node detection & reinit super
                synchronousInvokeCallback(stubRecoveryCallback);
            }

            List<Node> nodeList = storeToRoutingStrategy.get(storeName).routeRequest(key.get());

            // If we have faulty nodes stream slops
            if(blackListedNodes != null && blackListedNodes.size() > 0) {
                for(Node node: nodeList) {
                    if(blackListedNodes.contains(node.getId())) {
                        try {
                            streamingSlopPut(key, value, storeName, node.getId());
                        } catch(Exception e) {
                            synchronousInvokeCallback(stubRecoveryCallback);
                        }

                    }
                }

            }
        }

    }

    /**
     * This is a method to stream slops to "slop" store when a node is detected
     * faulty in a streaming session
     * 
     * @param key -- original key
     * @param value -- original value
     * @param storeName -- the store for which we are registering the slop
     * @param failedNodeId -- the faulty node ID for which we register a slop
     * @throws IOException
     */

    protected synchronized void streamingSlopPut(ByteArray key,
                                                 Versioned<byte[]> value,
                                                 String storeName,
                                                 int failedNodeId) throws IOException {

        Slop slop = new Slop(storeName,
                             Slop.Operation.PUT,
                             key,
                             value.getValue(),
                             null,
                             failedNodeId,
                             new Date());

        ByteArray slopKey = slop.makeKey();
        Versioned<byte[]> slopValue = new Versioned<byte[]>(slopSerializer.toBytes(slop),
                                                            value.getVersion());

        Node failedNode = adminClient.getAdminClientCluster().getNodeById(failedNodeId);
        HandoffToAnyStrategy slopRoutingStrategy = new HandoffToAnyStrategy(adminClient.getAdminClientCluster(),
                                                                            true,
                                                                            failedNode.getZoneId());
        // node Id which will recieve the slop
        int slopDestination = slopRoutingStrategy.routeHint(failedNode).get(0).getId();

        VAdminProto.PartitionEntry partitionEntry = VAdminProto.PartitionEntry.newBuilder()
                                                                              .setKey(ProtoUtils.encodeBytes(slopKey))
                                                                              .setVersioned(ProtoUtils.encodeVersioned(slopValue))
                                                                              .build();

        VAdminProto.UpdatePartitionEntriesRequest.Builder updateRequest = VAdminProto.UpdatePartitionEntriesRequest.newBuilder()
                                                                                                                   .setStore(SLOP_STORE)
                                                                                                                   .setPartitionEntry(partitionEntry);

        DataOutputStream outputStream = nodeIdStoreToOutputStreamRequest.get(new Pair<String, Integer>(SLOP_STORE,
                                                                                                       slopDestination));

        if(nodeIdStoreInitialized.get(new Pair<String, Integer>(SLOP_STORE, slopDestination))) {
            ProtoUtils.writeMessage(outputStream, updateRequest.build());
        } else {
            ProtoUtils.writeMessage(outputStream,
                                    VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                     .setType(VAdminProto.AdminRequestType.UPDATE_PARTITION_ENTRIES)
                                                                     .setUpdatePartitionEntries(updateRequest)
                                                                     .build());
            outputStream.flush();
            nodeIdStoreInitialized.put(new Pair<String, Integer>(SLOP_STORE, slopDestination), true);

        }

        throttler.maybeThrottle(1);

    }

    protected void initializeWithFailedNodes() {

        // we only support single node failures

        if(blackListedNodes == null || blackListedNodes.size() == 0) {
            super.initStreamingSessions(stores,
                                        stubCheckpointCallback,
                                        stubRecoveryCallback,
                                        allowMerge,
                                        null);
        } else if(blackListedNodes != null && blackListedNodes.size() <= MAX_FAULTY_NODES) {
            super.initStreamingSessions(stores,
                                        stubCheckpointCallback,
                                        stubRecoveryCallback,
                                        allowMerge,
                                        blackListedNodes);

            super.addStoreToSession("slop");

        } else {
            throw new InsufficientOperationalNodesException("More than " + MAX_FAULTY_NODES
                                                            + " nodes are down, cannot stream");
        }
    }

    /**
     * Helper method to synchronously invoke a callback
     * 
     * @param call
     */
    @SuppressWarnings("rawtypes")
    private void synchronousInvokeCallback(Callable call) {

        Future future = streamingSlopResults.submit(call);

        try {
            future.get();

        } catch(InterruptedException e1) {

            logger.error("Callback failed", e1);
            throw new VoldemortException("Callback failed");

        } catch(ExecutionException e1) {

            logger.error("Callback failed during execution", e1);
            throw new VoldemortException("Callback failed during execution");
        }

    }

}
