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
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketDestination;
import voldemort.utils.ByteArray;
import voldemort.utils.EventThrottler;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

/**
 * 
 * @author anagpal
 * 
 *         The streaming API allows for send events into voldemort stores in the
 *         async fashion. All the partition and replication logic will be taken
 *         care of internally.
 * 
 *         The users is expected to provide two callbacks, one for performing
 *         period checkpoints and one for recovering the streaming process from
 *         the last checkpoint.
 * 
 *         NOTE: The API is not thread safe, since if multiple threads use this
 *         API we cannot make any guarantees about correctness of the
 *         checkpointing mechanism.
 * 
 *         Right now we expect this to used by a single thread per data source
 * 
 */
public class StreamingClient {

    private static final Logger logger = Logger.getLogger(StreamingClient.class);
    @SuppressWarnings("rawtypes")
    private Callable checkpointCallback = null;
    @SuppressWarnings("rawtypes")
    private Callable recoveryCallback = null;
    private boolean allowMerge = false;

    private SocketPool streamingSocketPool;

    List<StoreDefinition> remoteStoreDefs;
    protected RoutingStrategy routingStrategy;

    boolean newBatch;
    boolean cleanedUp = false;

    boolean isMultiSession;
    ExecutorService streamingresults;

    // Every batch size we commit
    private static int CHECKPOINT_COMMIT_SIZE;

    // TODO
    // provide knobs to tune this
    private static int TIME_COMMIT_SIZE = 30;
    // we have to throttle to a certain qps
    private static int THROTTLE_QPS;
    private int entriesProcessed;

    // In case a Recovery callback fails we got to stop it from getting any
    // worse
    // so we mark the session as bad and dont take any more requests
    private static boolean MARKED_BAD = false;

    protected EventThrottler throttler;

    AdminClient adminClient;
    AdminClientConfig adminClientConfig;

    String bootstrapURL;

    // Data structures for the streaming maps from Pair<Store, Node Id> to
    // Resource

    private HashMap<String, RoutingStrategy> storeToRoutingStrategy;
    private HashMap<Pair<String, Integer>, Boolean> nodeIdStoreInitialized;

    private HashMap<Pair<String, Integer>, SocketDestination> nodeIdStoreToSocketRequest;
    private HashMap<Pair<String, Integer>, DataOutputStream> nodeIdStoreToOutputStreamRequest;
    private HashMap<Pair<String, Integer>, DataInputStream> nodeIdStoreToInputStreamRequest;

    private HashMap<Pair<String, Integer>, SocketAndStreams> nodeIdStoreToSocketAndStreams;

    private List<String> storeNames;

    private List<Node> nodesToStream;
    private List<Integer> blackListedNodes;

    private final static int MAX_STORES_PER_SESSION = 100;

    Calendar calendar = Calendar.getInstance();

    public StreamingClient(StreamingClientConfig config) {
        this.bootstrapURL = config.getBootstrapURL();
        CHECKPOINT_COMMIT_SIZE = config.getBatchSize();
        THROTTLE_QPS = config.getThrottleQPS();

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

    private void close(Socket socket) {
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

        initStreamingSessions(stores, checkpointCallback, recoveryCallback, allowMerge, null);

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
        adminClientConfig = new AdminClientConfig();
        adminClient = new AdminClient(bootstrapURL, adminClientConfig, new ClientConfig());
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
     * @param store: the name of the store to stream to
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void addStoreToSession(String store) {

        storeNames.add(store);

        for(Node node: nodesToStream) {

            SocketDestination destination = new SocketDestination(node.getHost(),
                                                                  node.getAdminPort(),
                                                                  RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
            SocketAndStreams sands = streamingSocketPool.checkout(destination);

            try {
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
                close(sands.getSocket());
                streamingSocketPool.checkin(destination, sands);
                throw new VoldemortException(e);
            }

        }

        boolean foundStore = false;

        for(StoreDefinition remoteStoreDef: remoteStoreDefs) {
            if(remoteStoreDef.getName().equals(store)) {
                RoutingStrategyFactory factory = new RoutingStrategyFactory();
                RoutingStrategy storeRoutingStrategy = factory.updateRoutingStrategy(remoteStoreDef,
                                                                                     adminClient.getAdminClientCluster());

                storeToRoutingStrategy.put(store, storeRoutingStrategy);
                foundStore = true;
                break;
            }
        }
        if(!foundStore) {
            logger.error("Store Name not found on the cluster");
            throw new VoldemortException("Store Name not found on the cluster");

        }

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

            VAdminProto.UpdatePartitionEntriesRequest.Builder updateRequest = VAdminProto.UpdatePartitionEntriesRequest.newBuilder()
                                                                                                                       .setStore(storeName)
                                                                                                                       .setPartitionEntry(partitionEntry);

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

                entriesProcessed++;

            } catch(IOException e) {
                logger.warn("Invoking the Recovery Callback");
                Future future = streamingresults.submit(recoveryCallback);
                try {
                    future.get();

                } catch(InterruptedException e1) {
                    MARKED_BAD = true;
                    logger.error("Recovery Callback failed");
                    e1.printStackTrace();
                    throw new VoldemortException("Recovery Callback failed");
                } catch(ExecutionException e1) {
                    MARKED_BAD = true;
                    logger.error("Recovery Callback failed");
                    e1.printStackTrace();
                    throw new VoldemortException("Recovery Callback failed");
                }

                e.printStackTrace();
            }

        }

        int secondsTime = calendar.get(Calendar.SECOND);
        if(entriesProcessed == CHECKPOINT_COMMIT_SIZE || secondsTime % TIME_COMMIT_SIZE == 0) {
            entriesProcessed = 0;

            commitToVoldemort();

        }

        throttler.maybeThrottle(1);

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
                        logger.warn("Invoking the Recovery Callback");
                        Future future = streamingresults.submit(recoveryCallback);
                        try {
                            future.get();

                        } catch(InterruptedException e1) {
                            MARKED_BAD = true;
                            logger.error("Recovery Callback failed");
                            e1.printStackTrace();
                            throw new VoldemortException("Recovery Callback failed");
                        } catch(ExecutionException e1) {
                            MARKED_BAD = true;
                            logger.error("Recovery Callback failed");
                            e1.printStackTrace();
                            throw new VoldemortException("Recovery Callback failed");
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

                            logger.warn("Checkpoint callback failed!");
                            e1.printStackTrace();
                        } catch(ExecutionException e1) {
                            logger.warn("Checkpoint callback failed!");
                            e1.printStackTrace();
                        }
                    }

                } catch(IOException e) {

                    logger.warn("Invoking the Recovery Callback");
                    Future future = streamingresults.submit(recoveryCallback);
                    try {
                        future.get();

                    } catch(InterruptedException e1) {
                        MARKED_BAD = true;
                        logger.error("Recovery Callback failed");
                        e1.printStackTrace();
                        throw new VoldemortException("Recovery Callback failed");
                    } catch(ExecutionException e1) {
                        MARKED_BAD = true;
                        logger.error("Recovery Callback failed");
                        e1.printStackTrace();
                        throw new VoldemortException("Recovery Callback failed");
                    }

                    e.printStackTrace();
                }
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
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } catch(ExecutionException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
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

                SocketAndStreams sands = nodeIdStoreToSocketAndStreams.get(new Pair(store,
                                                                                    node.getId()));
                close(sands.getSocket());
                SocketDestination destination = nodeIdStoreToSocketRequest.get(new Pair(store,
                                                                                        node.getId()));
                streamingSocketPool.checkin(destination, sands);
            }

        }

        cleanedUp = true;

    }

}
