/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort.client.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketDestination;
import voldemort.utils.ByteArray;
import voldemort.utils.EventThrottler;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

/**
 * 
 * 
 * The streaming API allows for send events into voldemort stores in the async
 * fashion. All the partition and replication logic will be taken care of
 * internally.
 * 
 * The users is expected to provide two callbacks, one for performing period
 * checkpoints and one for recovering the streaming process from the last
 * checkpoint.
 * 
 * NOTE: The API is not thread safe, since if multiple threads use this API we
 * cannot make any guarantees about correctness of the checkpointing mechanism.
 * 
 * Right now we expect this to used by a single thread per data source
 * 
 */
public class BaseStreamingClient {

    private static final Logger logger = Logger.getLogger(BaseStreamingClient.class);
    @SuppressWarnings("rawtypes")
    private Callable checkpointCallback = null;
    @SuppressWarnings("rawtypes")
    private Callable recoveryCallback = null;
    private boolean allowMerge = false;

    protected SocketPool streamingSocketPool;

    List<StoreDefinition> remoteStoreDefs;
    protected RoutingStrategy routingStrategy;

    boolean newBatch;
    boolean cleanedUp = false;

    boolean isMultiSession;
    ExecutorService streamingresults;

    // Every batch size we commit
    protected static int CHECKPOINT_COMMIT_SIZE;

    // we have to throttle to a certain qps
    private static int THROTTLE_QPS;
    protected int entriesProcessed;

    // In case a Recovery callback fails we got to stop it from getting any
    // worse
    // so we mark the session as bad and dont take any more requests
    protected static boolean MARKED_BAD = false;

    protected EventThrottler throttler;

    protected AdminClient adminClient;
    private AdminClientConfig adminClientConfig;

    String bootstrapURL;
    
    boolean overWriteIfLatestTs;

    // Data structures for the streaming maps from Pair<Store, Node Id> to
    // Resource

    protected HashMap<String, RoutingStrategy> storeToRoutingStrategy;
    protected HashMap<Pair<String, Integer>, Boolean> nodeIdStoreInitialized;

    private HashMap<Pair<String, Integer>, SocketDestination> nodeIdStoreToSocketRequest;
    protected HashMap<Pair<String, Integer>, DataOutputStream> nodeIdStoreToOutputStreamRequest;
    private HashMap<Pair<String, Integer>, DataInputStream> nodeIdStoreToInputStreamRequest;

    private HashMap<Pair<String, Integer>, SocketAndStreams> nodeIdStoreToSocketAndStreams;

    private List<String> storeNames;

    protected List<Node> nodesToStream;
    protected List<Integer> blackListedNodes;

    protected List<Integer> faultyNodes;

    private final static int MAX_STORES_PER_SESSION = 100;

    Calendar calendar = Calendar.getInstance();

    public BaseStreamingClient(StreamingClientConfig config) {
        this.bootstrapURL = config.getBootstrapURL();
        CHECKPOINT_COMMIT_SIZE = config.getBatchSize();
        THROTTLE_QPS = config.getThrottleQPS();
        this.overWriteIfLatestTs = config.isOverWriteIfLatestTs();
        
        adminClientConfig = new AdminClientConfig();
        adminClient = new AdminClient(bootstrapURL, adminClientConfig, new ClientConfig());
        faultyNodes = new ArrayList<Integer>();
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public List<Integer> getFaultyNodes() {
        return faultyNodes;
    }

    public synchronized void updateThrottleLimit(int throttleQPS) {
        THROTTLE_QPS = throttleQPS;

        this.throttler = new EventThrottler(THROTTLE_QPS);
    }

    /**
     ** 
     * @param store - the name of the store to be streamed to
     * 
     * @param checkpointCallback - the callback that allows for the user to
     *        record the progress, up to the last event delivered. This callable
     *        would be invoked every so often internally.
     * 
     * @param recoveryCallback - the callback that allows the user to rewind the
     *        upstream to the position recorded by the last complete call on
     *        checkpointCallback whenever an exception occurs during the
     *        streaming session.
     * 
     * @param allowMerge - whether to allow for the streaming event to be merged
     *        with online writes. If not, all online writes since the completion
     *        of the last streaming session will be lost at the end of the
     *        current streaming session.
     **/
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public synchronized void initStreamingSession(String store,
                                                  Callable checkpointCallback,
                                                  Callable recoveryCallback,
                                                  boolean allowMerge) {

        // internally call initsessions with a single store
        List<String> stores = new ArrayList();
        stores.add(store);
        initStreamingSessions(stores, checkpointCallback, recoveryCallback, allowMerge);

    }

    /**
     * A Streaming Put call
     ** 
     * @param key - The key
     * 
     * @param value - The value
     **/
    @SuppressWarnings({})
    public synchronized void streamingPut(ByteArray key, Versioned<byte[]> value) {

        if(MARKED_BAD) {
            logger.error("Cannot stream more entries since Recovery Callback Failed!");
            throw new VoldemortException("Cannot stream more entries since Recovery Callback Failed!");
        }

        for(String store: storeNames) {
            streamingPut(key, value, store);
        }

    }

    /**
     ** 
     * @param resetCheckpointCallback - the callback that allows for the user to
     *        clean up the checkpoint at the end of the streaming session so a
     *        new session could, if necessary, start from 0 position.
     **/
    @SuppressWarnings({ "rawtypes" })
    public synchronized void closeStreamingSession(Callable resetCheckpointCallback) {

        closeStreamingSessions(resetCheckpointCallback);

    }

    /**
     * Close the streaming session Flush all n/w buffers and call the commit
     * callback
     **/
    @SuppressWarnings({})
    public synchronized void closeStreamingSession() {

        closeStreamingSessions();

    }

    protected void close(Socket socket) {
        try {
            socket.close();
        } catch(IOException e) {
            logger.warn("Failed to close socket");
        }
    }

    @Override
    protected void finalize() {

        if(!cleanedUp) {
            cleanupSessions();
        }

    }

    /**
     ** 
     * @param stores - the list of name of the stores to be streamed to
     * 
     * 
     * @param checkpointCallback - the callback that allows for the user to
     *        record the progress, up to the last event delivered. This callable
     *        would be invoked every so often internally.
     * 
     * @param recoveryCallback - the callback that allows the user to rewind the
     *        upstream to the position recorded by the last complete call on
     *        checkpointCallback whenever an exception occurs during the
     *        streaming session.
     * 
     * @param allowMerge - whether to allow for the streaming event to be merged
     *        with online writes. If not, all online writes since the completion
     *        of the last streaming session will be lost at the end of the
     *        current streaming session.
     **/
    @SuppressWarnings({ "rawtypes" })
    public synchronized void initStreamingSessions(List<String> stores,
                                                   Callable checkpointCallback,
                                                   Callable recoveryCallback,
                                                   boolean allowMerge) {

        this.initStreamingSessions(stores, checkpointCallback, recoveryCallback, allowMerge, null);

    }

    /**
     ** 
     * @param stores - the list of name of the stores to be streamed to
     * 
     * 
     * @param checkpointCallback - the callback that allows for the user to
     *        record the progress, up to the last event delivered. This callable
     *        would be invoked every so often internally.
     * 
     * @param recoveryCallback - the callback that allows the user to rewind the
     *        upstream to the position recorded by the last complete call on
     *        checkpointCallback whenever an exception occurs during the
     *        streaming session.
     * 
     * @param allowMerge - whether to allow for the streaming event to be merged
     *        with online writes. If not, all online writes since the completion
     *        of the last streaming session will be lost at the end of the
     *        current streaming session.
     * 
     * @param blackListedNodes - the list of Nodes not to stream to; we can
     *        probably recover them later from the replicas
     **/

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public synchronized void initStreamingSessions(List<String> stores,
                                                   Callable checkpointCallback,
                                                   Callable recoveryCallback,
                                                   boolean allowMerge,
                                                   List<Integer> blackListedNodes) {

        logger.info("Initializing a streaming session");
        this.checkpointCallback = checkpointCallback;
        this.recoveryCallback = recoveryCallback;
        this.allowMerge = allowMerge;
        streamingresults = Executors.newFixedThreadPool(3);
        entriesProcessed = 0;
        newBatch = true;
        isMultiSession = true;
        storeNames = new ArrayList();
        this.throttler = new EventThrottler(THROTTLE_QPS);

        TimeUnit unit = TimeUnit.SECONDS;

        Collection<Node> nodesInCluster = adminClient.getAdminClientCluster().getNodes();
        nodesToStream = new ArrayList();

        if(blackListedNodes != null && blackListedNodes.size() > 0) {

            this.blackListedNodes = blackListedNodes;

        }
        for(Node node: nodesInCluster) {
            if(blackListedNodes != null && blackListedNodes.size() > 0) {
                if(!blackListedNodes.contains(node.getId())) {
                    nodesToStream.add(node);
                }
            } else
                nodesToStream.add(node);

        }
        // socket pool
        streamingSocketPool = new SocketPool(adminClient.getAdminClientCluster().getNumberOfNodes()
                                                     * MAX_STORES_PER_SESSION,
                                             (int) unit.toMillis(adminClientConfig.getAdminConnectionTimeoutSec()),
                                             (int) unit.toMillis(adminClientConfig.getAdminSocketTimeoutSec()),
                                             adminClientConfig.getAdminSocketBufferSize(),
                                             adminClientConfig.getAdminSocketKeepAlive());

        nodeIdStoreToSocketRequest = new HashMap();
        nodeIdStoreToOutputStreamRequest = new HashMap();
        nodeIdStoreToInputStreamRequest = new HashMap();
        nodeIdStoreInitialized = new HashMap();
        storeToRoutingStrategy = new HashMap();
        nodeIdStoreToSocketAndStreams = new HashMap();
        for(String store: stores) {

            addStoreToSession(store);
        }

    }

    /**
     * Add another store destination to an existing streaming session
     * 
     * 
     * @param store the name of the store to stream to
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected void addStoreToSession(String store) {

        Exception initializationException = null;

        storeNames.add(store);

        for(Node node: nodesToStream) {

            SocketDestination destination = null;
            SocketAndStreams sands = null;

            try {
                destination = new SocketDestination(node.getHost(),
                                                    node.getAdminPort(),
                                                    RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
                sands = streamingSocketPool.checkout(destination);
                DataOutputStream outputStream = sands.getOutputStream();
                DataInputStream inputStream = sands.getInputStream();

                nodeIdStoreToSocketRequest.put(new Pair(store, node.getId()), destination);
                nodeIdStoreToOutputStreamRequest.put(new Pair(store, node.getId()), outputStream);
                nodeIdStoreToInputStreamRequest.put(new Pair(store, node.getId()), inputStream);
                nodeIdStoreToSocketAndStreams.put(new Pair(store, node.getId()), sands);
                nodeIdStoreInitialized.put(new Pair(store, node.getId()), false);

                remoteStoreDefs = adminClient.metadataMgmtOps.getRemoteStoreDefList(node.getId())
                                                             .getValue();

            } catch(Exception e) {
                logger.error(e);
                try {
                    close(sands.getSocket());
                    streamingSocketPool.checkin(destination, sands);
                } catch(Exception ioE) {
                    logger.error(ioE);
                }

                if(!faultyNodes.contains(node.getId()))
                    faultyNodes.add(node.getId());
                initializationException = e;
            }

        }

        if(initializationException != null)
            throw new VoldemortException(initializationException);

        if(store.equals("slop"))
            return;

        boolean foundStore = false;

        for(StoreDefinition remoteStoreDef: remoteStoreDefs) {
            if(remoteStoreDef.getName().equals(store)) {
                RoutingStrategyFactory factory = new RoutingStrategyFactory();
                RoutingStrategy storeRoutingStrategy = factory.updateRoutingStrategy(remoteStoreDef,
                                                                                     adminClient.getAdminClientCluster());

                storeToRoutingStrategy.put(store, storeRoutingStrategy);
                validateSufficientNodesAvailable(blackListedNodes, remoteStoreDef);
                foundStore = true;
                break;
            }
        }
        if(!foundStore) {
            logger.error("Store Name not found on the cluster");
            throw new VoldemortException("Store Name not found on the cluster");

        }

    }

    private void validateSufficientNodesAvailable(List<Integer> blackListedNodes,
                                                  StoreDefinition remoteStoreDef) {

        int faultyNodes = 0;
        if(blackListedNodes != null && blackListedNodes.size() > 0) {
            faultyNodes = blackListedNodes.size();
        }
        int repFactor = remoteStoreDef.getReplicationFactor();
        int numNodes = repFactor - faultyNodes;
        if(numNodes < remoteStoreDef.getRequiredWrites())
            throw new InsufficientOperationalNodesException("Only " + numNodes
                                                            + " nodes in preference list, but "
                                                            + remoteStoreDef.getRequiredWrites()
                                                            + " writes required.");
    }

    /**
     * Remove a list of stores from the session
     * 
     * First commit all entries for these stores and then cleanup resources
     * 
     * @param storeNameToRemove List of stores to be removed from the current
     *        streaming session
     * 
     **/
    @SuppressWarnings({})
    public synchronized void removeStoreFromSession(List<String> storeNameToRemove) {

        logger.info("closing the Streaming session for a few stores");

        commitToVoldemort(storeNameToRemove);
        cleanupSessions(storeNameToRemove);

    }

    /**
     ** 
     * @param key - The key
     * 
     * @param value - The value
     * 
     * @param storeName takes an additional store name as a parameter
     * 
     *        If a store is added mid way through a streaming session we do not
     *        play catchup and entries that were processed earlier during the
     *        session will not be applied for the store.
     * 
     **/
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public synchronized void streamingPut(ByteArray key, Versioned<byte[]> value, String storeName) {

        // If store does not exist in the stores list
        // add it and checkout a socket
        if(!storeNames.contains(storeName)) {
            addStoreToSession(storeName);
        }

        if(MARKED_BAD) {
            logger.error("Cannot stream more entries since Recovery Callback Failed!");
            throw new VoldemortException("Cannot stream more entries since Recovery Callback Failed! You Need to restart the session");
        }

        List<Node> nodeList = storeToRoutingStrategy.get(storeName).routeRequest(key.get());

        int nodesWithException = 0;
        // sent the k/v pair to the nodes
        for(Node node: nodeList) {

            if(blackListedNodes != null && blackListedNodes.size() > 0) {
                if(blackListedNodes.contains(node.getId()))
                    continue;
            }
            // if node! in blacklistednodes

            VAdminProto.PartitionEntry partitionEntry = VAdminProto.PartitionEntry.newBuilder()
                                                                                  .setKey(ProtoUtils.encodeBytes(key))
                                                                                  .setVersioned(ProtoUtils.encodeVersioned(value))
                                                                                  .build();

            
            VAdminProto.UpdatePartitionEntriesRequest.Builder updateRequest = null;
            
            if(overWriteIfLatestTs) {
                updateRequest = VAdminProto.UpdatePartitionEntriesRequest.newBuilder()
                                                                         .setStore(storeName)
                                                                         .setPartitionEntry(partitionEntry)
                                                                         .setOverwriteIfLatestTs(overWriteIfLatestTs);
            } else {
                updateRequest = VAdminProto.UpdatePartitionEntriesRequest.newBuilder()
                                                                         .setStore(storeName)
                                                                         .setPartitionEntry(partitionEntry);
            }
            
            DataOutputStream outputStream = nodeIdStoreToOutputStreamRequest.get(new Pair(storeName,
                                                                                          node.getId()));
            try {
                if(nodeIdStoreInitialized.get(new Pair(storeName, node.getId()))) {
                    ProtoUtils.writeMessage(outputStream, updateRequest.build());
                } else {
                    ProtoUtils.writeMessage(outputStream,
                                            VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                             .setType(VAdminProto.AdminRequestType.UPDATE_PARTITION_ENTRIES)
                                                                             .setUpdatePartitionEntries(updateRequest)
                                                                             .build());
                    outputStream.flush();
                    nodeIdStoreInitialized.put(new Pair(storeName, node.getId()), true);

                }

            } catch(IOException e) {
                e.printStackTrace();
                nodesWithException++;
                if(!faultyNodes.contains(node.getId()))
                    faultyNodes.add(node.getId());
            }

        }

        if(nodesWithException > 0) {
            logger.warn("Invoking the Recovery Callback");
            Future future = streamingresults.submit(recoveryCallback);
            try {
                future.get();

            } catch(InterruptedException e1) {
                MARKED_BAD = true;
                logger.error("Recovery Callback failed", e1);
                throw new VoldemortException("Recovery Callback failed");
            } catch(ExecutionException e1) {
                MARKED_BAD = true;
                logger.error("Recovery Callback failed during execution", e1);
                throw new VoldemortException("Recovery Callback failed during execution");
            }

        } else {
            entriesProcessed++;
            if(entriesProcessed == CHECKPOINT_COMMIT_SIZE) {
                entriesProcessed = 0;
                commitToVoldemort();
            }

            throttler.maybeThrottle(1);
        }

    }

    /**
     * Flush the network buffer and write all entries to the server Wait for an
     * ack from the server This is a blocking call. It is invoked on every
     * Commit batch size of entries It is also called on the close session call
     */

    public synchronized void commitToVoldemort() {
        entriesProcessed = 0;
        commitToVoldemort(storeNames);
    }

    /**
     * Reset streaming session by unmarking it as bad
     */
    public void unmarkBad() {
        MARKED_BAD = false;
    }

    /**
     * mark a node as blacklisted
     * 
     * @param nodeId Integer node id of the node to be balcklisted
     */

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void blacklistNode(int nodeId) {
        Collection<Node> nodesInCluster = adminClient.getAdminClientCluster().getNodes();

        if(blackListedNodes == null) {
            blackListedNodes = new ArrayList();
        }
        blackListedNodes.add(nodeId);

        for(Node node: nodesInCluster) {

            if(node.getId() == nodeId) {
                nodesToStream.remove(node);
                break;
            }

        }

        for(String store: storeNames) {
            try {
                SocketAndStreams sands = nodeIdStoreToSocketAndStreams.get(new Pair(store, nodeId));
                close(sands.getSocket());
                SocketDestination destination = nodeIdStoreToSocketRequest.get(new Pair(store,
                                                                                        nodeId));
                streamingSocketPool.checkin(destination, sands);
            } catch(Exception ioE) {
                logger.error(ioE);
            }
        }
    }

    /**
     * Flush the network buffer and write all entries to the serve. then wait
     * for an ack from the server. This is a blocking call. It is invoked on
     * every Commit batch size of entries, It is also called on the close
     * session call
     * 
     * @param storeNameToCommit List of stores to be flushed and committed
     * 
     */
    @SuppressWarnings({ "unchecked", "rawtypes", "unused" })
    private void commitToVoldemort(List<String> storeNamesToCommit) {

        if(logger.isDebugEnabled()) {
            logger.debug("Trying to commit to Voldemort");
        }

        boolean hasError = false;
        for(Node node: nodesToStream) {

            for(String store: storeNamesToCommit) {
                if(!nodeIdStoreInitialized.get(new Pair(store, node.getId())))
                    continue;

                nodeIdStoreInitialized.put(new Pair(store, node.getId()), false);

                DataOutputStream outputStream = nodeIdStoreToOutputStreamRequest.get(new Pair(store,
                                                                                              node.getId()));

                try {
                    ProtoUtils.writeEndOfStream(outputStream);
                    outputStream.flush();
                    DataInputStream inputStream = nodeIdStoreToInputStreamRequest.get(new Pair(store,
                                                                                               node.getId()));
                    VAdminProto.UpdatePartitionEntriesResponse.Builder updateResponse = ProtoUtils.readToBuilder(inputStream,
                                                                                                                 VAdminProto.UpdatePartitionEntriesResponse.newBuilder());
                    if(updateResponse.hasError()) {
                        hasError = true;
                    }

                } catch(IOException e) {
                    logger.error("Exception during commit", e);
                    hasError = true;
                    if(!faultyNodes.contains(node.getId()))
                        faultyNodes.add(node.getId());
                }
            }

        }

        // remove redundant callbacks
        if(hasError) {

            logger.warn("Invoking the Recovery Callback");
            Future future = streamingresults.submit(recoveryCallback);
            try {
                future.get();

            } catch(InterruptedException e1) {
                MARKED_BAD = true;
                logger.error("Recovery Callback failed", e1);
                throw new VoldemortException("Recovery Callback failed");
            } catch(ExecutionException e1) {
                MARKED_BAD = true;
                logger.error("Recovery Callback failed during execution", e1);
                throw new VoldemortException("Recovery Callback failed during execution");
            }
        } else {
            if(logger.isDebugEnabled()) {
                logger.debug("Commit successful");
                logger.debug("calling checkpoint callback");
            }
            Future future = streamingresults.submit(checkpointCallback);
            try {
                future.get();

            } catch(InterruptedException e1) {
                logger.warn("Checkpoint callback failed!", e1);
            } catch(ExecutionException e1) {
                logger.warn("Checkpoint callback failed during execution!", e1);
            }
        }

    }

    /**
     ** 
     * @param resetCheckpointCallback - the callback that allows for the user to
     *        clean up the checkpoint at the end of the streaming session so a
     *        new session could, if necessary, start from 0 position.
     **/
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public synchronized void closeStreamingSessions(Callable resetCheckpointCallback) {

        closeStreamingSessions();

        Future future = streamingresults.submit(resetCheckpointCallback);
        try {
            future.get();

        } catch(InterruptedException e1) {
            logger.warn("Reset check point interrupted" + e1);
        } catch(ExecutionException e1) {
            logger.warn("Reset check point interrupted during execution" + e1);
        }
    }

    /**
     * Close the streaming session Flush all n/w buffers and call the commit
     * callback
     **/
    @SuppressWarnings({})
    public synchronized void closeStreamingSessions() {

        logger.info("closing the Streaming session");

        commitToVoldemort();
        cleanupSessions();

    }

    /**
     * Helper method to Close all open socket connections and checkin back to
     * the pool
     */
    private void cleanupSessions() {

        cleanupSessions(storeNames);
    }

    /**
     * Helper method to Close all open socket connections and checkin back to
     * the pool
     * 
     * @param storeNameToCleanUp List of stores to be cleanedup from the current
     *        streaming session
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void cleanupSessions(List<String> storeNamesToCleanUp) {

        logger.info("Performing cleanup");
        for(String store: storeNamesToCleanUp) {

            for(Node node: nodesToStream) {
                try {
                    SocketAndStreams sands = nodeIdStoreToSocketAndStreams.get(new Pair(store,
                                                                                        node.getId()));
                    close(sands.getSocket());
                    SocketDestination destination = nodeIdStoreToSocketRequest.get(new Pair(store,
                                                                                            node.getId()));
                    streamingSocketPool.checkin(destination, sands);
                } catch(Exception ioE) {
                    logger.error(ioE);
                }
            }

        }

        cleanedUp = true;

    }

}
