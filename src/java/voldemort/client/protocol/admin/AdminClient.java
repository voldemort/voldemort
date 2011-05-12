/*
 * Copyright 2008-2009 LinkedIn, Inc
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
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.Socket;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VProto;
import voldemort.client.protocol.pb.VAdminProto.ROStoreVersionDirMap;
import voldemort.client.protocol.pb.VAdminProto.UpdateGrandfatherMetadataRequest;
import voldemort.client.protocol.pb.VProto.RequestType;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.mysql.MysqlStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.store.slop.Slop;
import voldemort.store.slop.Slop.Operation;
import voldemort.store.socket.SocketDestination;
import voldemort.store.views.ViewStorageConfiguration;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.NetworkClassLoader;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

/**
 * AdminClient is intended for administrative functionality that is useful and
 * often needed, but should be used sparingly (if at all) at the application
 * level.
 * <p>
 * Some of the uses of AdminClient include
 * <ul>
 * <li>Extraction of data for backups</li>
 * <li>Extraction of all keys</li>
 * <li>Bulk loading entries</li>
 * <li>Migrating partitions</li>
 * <li>Get/Update metadata info from selective Nodes</li>
 * <li>Used extensively by rebalancing (dynamic node addition/deletion) feature
 * (presently in development).</li>
 * </ul>
 * 
 */
public class AdminClient {

    private static final Logger logger = Logger.getLogger(AdminClient.class);
    private final ErrorCodeMapper errorMapper;
    private final SocketPool pool;
    private final NetworkClassLoader networkClassLoader;
    private static final ClusterMapper clusterMapper = new ClusterMapper();
    private static final StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();

    // Parameters for exponential back off
    private static final long INITIAL_DELAY = 250; // Initial delay
    private final AdminClientConfig adminClientConfig;

    public final static List<String> restoreStoreEngineBlackList = Arrays.asList(MysqlStorageConfiguration.TYPE_NAME,
                                                                                 ReadOnlyStorageConfiguration.TYPE_NAME,
                                                                                 ViewStorageConfiguration.TYPE_NAME);

    private Cluster currentCluster;

    /**
     * Create an instance of AdminClient given a URL of a node in the cluster.
     * The bootstrap URL is used to get the cluster metadata.
     * 
     * @param bootstrapURL URL pointing to the bootstrap node
     * @param adminClientConfig Configuration for AdminClient specifying client
     *        parameters eg. <br>
     *        <ul>
     *        <t>
     *        <li>number of threads</li>
     *        <li>number of sockets per node</li>
     *        <li>socket buffer size</li>
     *        </ul>
     */
    public AdminClient(String bootstrapURL, AdminClientConfig adminClientConfig) {
        this.currentCluster = getClusterFromBootstrapURL(bootstrapURL);
        this.errorMapper = new ErrorCodeMapper();
        this.pool = createSocketPool(adminClientConfig);
        this.networkClassLoader = new NetworkClassLoader(Thread.currentThread()
                                                               .getContextClassLoader());
        this.adminClientConfig = adminClientConfig;
    }

    /**
     * Create an instance of AdminClient given a {@link Cluster} object.
     * 
     * @param cluster Initialized cluster object, describing the nodes we wish
     *        to contact
     * @param adminClientConfig Configuration for AdminClient specifying client
     *        parameters eg. <br>
     *        <ul>
     *        <t>
     *        <li>number of threads</li>
     *        <li>number of sockets per node</li>
     *        <li>socket buffer size</li>
     *        </ul>
     */
    public AdminClient(Cluster cluster, AdminClientConfig adminClientConfig) {
        this.currentCluster = cluster;
        this.errorMapper = new ErrorCodeMapper();
        this.pool = createSocketPool(adminClientConfig);
        this.networkClassLoader = new NetworkClassLoader(Thread.currentThread()
                                                               .getContextClassLoader());
        this.adminClientConfig = adminClientConfig;
    }

    private Cluster getClusterFromBootstrapURL(String bootstrapURL) {
        ClientConfig config = new ClientConfig();
        // try to bootstrap metadata from bootstrapUrl
        config.setBootstrapUrls(bootstrapURL);
        SocketStoreClientFactory factory = new SocketStoreClientFactory(config);
        // get Cluster from bootStrapUrl
        String clusterXml = factory.bootstrapMetadataWithRetries(MetadataStore.CLUSTER_KEY,
                                                                 factory.validateUrls(config.getBootstrapUrls()));
        // release all threads/sockets hold by the factory.
        factory.close();

        return clusterMapper.readCluster(new StringReader(clusterXml), false);
    }

    private SocketPool createSocketPool(AdminClientConfig config) {
        TimeUnit unit = TimeUnit.SECONDS;
        return new SocketPool(config.getMaxConnectionsPerNode(),
                              (int) unit.toMillis(config.getAdminConnectionTimeoutSec()),
                              (int) unit.toMillis(config.getAdminSocketTimeoutSec()),
                              config.getAdminSocketBufferSize(),
                              config.getAdminSocketKeepAlive());
    }

    private <T extends Message.Builder> T sendAndReceive(int nodeId, Message message, T builder) {
        Node node = this.getAdminClientCluster().getNodeById(nodeId);
        SocketDestination destination = new SocketDestination(node.getHost(),
                                                              node.getAdminPort(),
                                                              RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
        SocketAndStreams sands = pool.checkout(destination);

        try {
            DataOutputStream outputStream = sands.getOutputStream();
            DataInputStream inputStream = sands.getInputStream();
            ProtoUtils.writeMessage(outputStream, message);
            outputStream.flush();

            return ProtoUtils.readToBuilder(inputStream, builder);
        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    /**
     * Update a stream of key/value entries at the given node. The iterator
     * entries are <em>streamed</em> from the client to the server:
     * <ol>
     * <li>Client performs a handshake with the server (sending in the update
     * entries request with a store name and a {@link VoldemortFilter} instance.
     * </li>
     * <li>While entryIterator has entries, the client will keep sending the
     * updates one after another to the server, buffering the data, without
     * waiting for a response from the server.</li>
     * <li>After iteration is complete, send an end of stream message, force a
     * flush of the buffer, check the response on the server to check if a
     * {@link VoldemortException} has occured.</li>
     * </ol>
     * 
     * @param nodeId Id of the remote node (where we wish to update the entries)
     * @param storeName Store name for the entries
     * @param entryIterator Iterator of key-value pairs for the entries
     * @param filter Custom filter implementation to filter out entries which
     *        should not be updated.
     * @throws VoldemortException
     */
    public void updateEntries(int nodeId,
                              String storeName,
                              Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator,
                              VoldemortFilter filter) {
        Node node = this.getAdminClientCluster().getNodeById(nodeId);
        SocketDestination destination = new SocketDestination(node.getHost(),
                                                              node.getAdminPort(),
                                                              RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
        SocketAndStreams sands = pool.checkout(destination);
        DataOutputStream outputStream = sands.getOutputStream();
        DataInputStream inputStream = sands.getInputStream();
        boolean firstMessage = true;

        try {
            if(entryIterator.hasNext()) {
                while(entryIterator.hasNext()) {
                    Pair<ByteArray, Versioned<byte[]>> entry = entryIterator.next();
                    VAdminProto.PartitionEntry partitionEntry = VAdminProto.PartitionEntry.newBuilder()
                                                                                          .setKey(ProtoUtils.encodeBytes(entry.getFirst()))
                                                                                          .setVersioned(ProtoUtils.encodeVersioned(entry.getSecond()))
                                                                                          .build();
                    VAdminProto.UpdatePartitionEntriesRequest.Builder updateRequest = VAdminProto.UpdatePartitionEntriesRequest.newBuilder()
                                                                                                                               .setStore(storeName)
                                                                                                                               .setPartitionEntry(partitionEntry);

                    if(firstMessage) {
                        if(filter != null) {
                            updateRequest.setFilter(encodeFilter(filter));
                        }

                        ProtoUtils.writeMessage(outputStream,
                                                VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                 .setType(VAdminProto.AdminRequestType.UPDATE_PARTITION_ENTRIES)
                                                                                 .setUpdatePartitionEntries(updateRequest)
                                                                                 .build());
                        outputStream.flush();
                        firstMessage = false;
                    } else {
                        ProtoUtils.writeMessage(outputStream, updateRequest.build());
                    }
                }
                ProtoUtils.writeEndOfStream(outputStream);
                outputStream.flush();
                VAdminProto.UpdatePartitionEntriesResponse.Builder updateResponse = ProtoUtils.readToBuilder(inputStream,
                                                                                                             VAdminProto.UpdatePartitionEntriesResponse.newBuilder());
                if(updateResponse.hasError()) {
                    throwException(updateResponse.getError());
                }
            }
        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    private void initiateFetchRequest(DataOutputStream outputStream,
                                      String storeName,
                                      List<Integer> partitionList,
                                      VoldemortFilter filter,
                                      boolean fetchValues,
                                      boolean fetchMasterEntries,
                                      long skipRecords) throws IOException {
        VAdminProto.FetchPartitionEntriesRequest.Builder fetchRequest = VAdminProto.FetchPartitionEntriesRequest.newBuilder()
                                                                                                                .addAllPartitions(partitionList)
                                                                                                                .setFetchValues(fetchValues)
                                                                                                                .setFetchMasterEntries(fetchMasterEntries)
                                                                                                                .setStore(storeName)
                                                                                                                .setSkipRecords(skipRecords);

        if(filter != null) {
            fetchRequest.setFilter(encodeFilter(filter));
        }

        VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                     .setType(VAdminProto.AdminRequestType.FETCH_PARTITION_ENTRIES)
                                                                                     .setFetchPartitionEntries(fetchRequest)
                                                                                     .build();
        ProtoUtils.writeMessage(outputStream, request);
        outputStream.flush();

    }

    private VAdminProto.FetchPartitionEntriesResponse responseFromStream(DataInputStream inputStream,
                                                                         int size)
            throws IOException {
        byte[] input = new byte[size];
        ByteUtils.read(inputStream, input);
        VAdminProto.FetchPartitionEntriesResponse.Builder response = VAdminProto.FetchPartitionEntriesResponse.newBuilder();
        response.mergeFrom(input);

        return response.build();
    }

    /**
     * Fetch key/value entries belonging to partitionList from requested node.
     * <p>
     * 
     * <b>this is a streaming API.</b> The server keeps sending the messages as
     * it's iterating over the data. Once iteration has finished, the server
     * sends an "end of stream" marker and flushes its buffer. A response
     * indicating a {@link VoldemortException} may be sent at any time during
     * the process.<br>
     * 
     * Entries are being streamed <em>as the iteration happens</em> the whole
     * result set is <b>not</b> buffered in memory.
     * 
     * @param nodeId Id of the node to fetch from
     * @param storeName Name of the store
     * @param partitionList List of the partitions
     * @param filter Custom filter implementation to filter out entries which
     *        should not be fetched.
     * @param fetchMasterEntries Fetch an entry only if master replica
     * @param skipRecords Number of records to skip
     * @return An iterator which allows entries to be streamed as they're being
     *         iterated over.
     * @throws VoldemortException
     */
    public Iterator<Pair<ByteArray, Versioned<byte[]>>> fetchEntries(int nodeId,
                                                                     String storeName,
                                                                     List<Integer> partitionList,
                                                                     VoldemortFilter filter,
                                                                     boolean fetchMasterEntries,
                                                                     long skipRecords) {

        Node node = this.getAdminClientCluster().getNodeById(nodeId);
        final SocketDestination destination = new SocketDestination(node.getHost(),
                                                                    node.getAdminPort(),
                                                                    RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
        final SocketAndStreams sands = pool.checkout(destination);
        DataOutputStream outputStream = sands.getOutputStream();
        final DataInputStream inputStream = sands.getInputStream();

        try {
            initiateFetchRequest(outputStream,
                                 storeName,
                                 partitionList,
                                 filter,
                                 true,
                                 fetchMasterEntries,
                                 skipRecords);
        } catch(IOException e) {
            close(sands.getSocket());
            pool.checkin(destination, sands);
            throw new VoldemortException(e);
        }

        return new AbstractIterator<Pair<ByteArray, Versioned<byte[]>>>() {

            @Override
            public Pair<ByteArray, Versioned<byte[]>> computeNext() {
                try {
                    int size = inputStream.readInt();
                    if(size == -1) {
                        pool.checkin(destination, sands);
                        return endOfData();
                    }

                    VAdminProto.FetchPartitionEntriesResponse response = responseFromStream(inputStream,
                                                                                            size);

                    if(response.hasError()) {
                        pool.checkin(destination, sands);
                        throwException(response.getError());
                    }

                    VAdminProto.PartitionEntry partitionEntry = response.getPartitionEntry();

                    return Pair.create(ProtoUtils.decodeBytes(partitionEntry.getKey()),
                                       ProtoUtils.decodeVersioned(partitionEntry.getVersioned()));
                } catch(IOException e) {
                    close(sands.getSocket());
                    pool.checkin(destination, sands);
                    throw new VoldemortException(e);
                }
            }
        };

    }

    /**
     * See documentation for
     * {@link AdminClient#fetchEntries(int, String, List, VoldemortFilter, boolean, long)}
     * . Kept for backwards compatibility
     */
    public Iterator<Pair<ByteArray, Versioned<byte[]>>> fetchEntries(int nodeId,
                                                                     String storeName,
                                                                     List<Integer> partitionList,
                                                                     VoldemortFilter filter,
                                                                     boolean fetchMasterEntries) {
        return fetchEntries(nodeId, storeName, partitionList, filter, fetchMasterEntries, 0);
    }

    /**
     * Fetch All keys belonging to partitionList from requested node. Identical
     * to {@link AdminClient#fetchEntries} but will <em>only fetch the keys</em>
     * 
     * @param nodeId See documentation for {@link AdminClient#fetchEntries}
     * @param storeName See documentation for {@link AdminClient#fetchEntries}
     * @param partitionList See documentation for
     *        {@link AdminClient#fetchEntries}
     * @param filter See documentation for {@link AdminClient#fetchEntries}
     * @param skipRecords See documentation for
     *        {@link AdminClient#fetchEntries(int, String, List, VoldemortFilter, boolean, long)}
     * @return See documentation for {@link AdminClient#fetchEntries}
     */
    public Iterator<ByteArray> fetchKeys(int nodeId,
                                         String storeName,
                                         List<Integer> partitionList,
                                         VoldemortFilter filter,
                                         boolean fetchMasterEntries,
                                         long skipRecords) {
        Node node = this.getAdminClientCluster().getNodeById(nodeId);
        final SocketDestination destination = new SocketDestination(node.getHost(),
                                                                    node.getAdminPort(),
                                                                    RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
        final SocketAndStreams sands = pool.checkout(destination);
        DataOutputStream outputStream = sands.getOutputStream();
        final DataInputStream inputStream = sands.getInputStream();

        try {
            initiateFetchRequest(outputStream,
                                 storeName,
                                 partitionList,
                                 filter,
                                 false,
                                 fetchMasterEntries,
                                 skipRecords);
        } catch(IOException e) {
            close(sands.getSocket());
            pool.checkin(destination, sands);
            throw new VoldemortException(e);
        }

        return new AbstractIterator<ByteArray>() {

            @Override
            public ByteArray computeNext() {
                try {
                    int size = inputStream.readInt();
                    if(size == -1) {
                        pool.checkin(destination, sands);
                        return endOfData();
                    }

                    VAdminProto.FetchPartitionEntriesResponse response = responseFromStream(inputStream,
                                                                                            size);

                    if(response.hasError()) {
                        pool.checkin(destination, sands);
                        throwException(response.getError());
                    }

                    return ProtoUtils.decodeBytes(response.getKey());
                } catch(IOException e) {
                    close(sands.getSocket());
                    pool.checkin(destination, sands);
                    throw new VoldemortException(e);
                }

            }
        };
    }

    /**
     * See documentation for
     * {@link AdminClient#fetchKeys(int, String, List, VoldemortFilter, boolean, long)}
     * . Kept for backwards compatibility
     */
    public Iterator<ByteArray> fetchKeys(int nodeId,
                                         String storeName,
                                         List<Integer> partitionList,
                                         VoldemortFilter filter,
                                         boolean fetchMasterEntries) {
        return fetchKeys(nodeId, storeName, partitionList, filter, fetchMasterEntries, 0);
    }

    /**
     * RestoreData from copies on other machines for the given nodeId
     * <p>
     * Recovery mechanism to recover and restore data actively from replicated
     * copies in the cluster.<br>
     * 
     * @param nodeId Id of the node to restoreData
     * @param parallelTransfers number of transfers
     * @throws InterruptedException
     */
    public void restoreDataFromReplications(int nodeId, int parallelTransfers) {
        ExecutorService executors = Executors.newFixedThreadPool(parallelTransfers,
                                                                 new ThreadFactory() {

                                                                     public Thread newThread(Runnable r) {
                                                                         Thread thread = new Thread(r);
                                                                         thread.setName("restore-data-thread");
                                                                         return thread;
                                                                     }
                                                                 });
        try {
            List<StoreDefinition> storeDefList = getRemoteStoreDefList(nodeId).getValue();
            Cluster cluster = getRemoteCluster(nodeId).getValue();

            List<StoreDefinition> writableStores = Lists.newArrayList();
            for(StoreDefinition def: storeDefList) {
                if(def.isView() || restoreStoreEngineBlackList.contains(def.getType())) {
                    logger.info("Ignoring store " + def.getName() + " for restoring");
                } else {
                    writableStores.add(def);
                }
            }
            for(StoreDefinition def: writableStores) {
                restoreStoreFromReplication(nodeId, cluster, def, executors);
            }
        } finally {
            executors.shutdown();
            try {
                executors.awaitTermination(adminClientConfig.getRestoreDataTimeout(),
                                           TimeUnit.SECONDS);
            } catch(InterruptedException e) {
                logger.error("Interrupted while waiting restore operation to finish.");
            }
            logger.info("Finished restoring data.");
        }
    }

    /**
     * For a particular store and node, runs the replication job
     * 
     * @param restoringNodeId The node which we want to restore
     * @param cluster The cluster metadata
     * @param storeDef The definition of the store which we want to restore
     * @param executorService An executor to allow us to run the replication job
     */
    private void restoreStoreFromReplication(final int restoringNodeId,
                                             final Cluster cluster,
                                             final StoreDefinition storeDef,
                                             final ExecutorService executorService) {
        logger.info("Restoring data for store " + storeDef.getName() + " on node "
                    + restoringNodeId);
        RoutingStrategyFactory factory = new RoutingStrategyFactory();
        RoutingStrategy strategy = factory.updateRoutingStrategy(storeDef, cluster);

        Map<Integer, List<Integer>> restoreMapping = getReplicationMapping(cluster,
                                                                           restoringNodeId,
                                                                           strategy);
        // migrate partition
        for(final Entry<Integer, List<Integer>> replicationEntry: restoreMapping.entrySet()) {
            final int donorNodeId = replicationEntry.getKey();
            executorService.submit(new Runnable() {

                public void run() {
                    try {
                        logger.info("restoring data for store " + storeDef.getName() + " at node "
                                    + restoringNodeId + " from node " + replicationEntry.getKey()
                                    + " partitions:" + replicationEntry.getValue());

                        int migrateAsyncId = migratePartitions(donorNodeId,
                                                               restoringNodeId,
                                                               storeDef.getName(),
                                                               replicationEntry.getValue(),
                                                               null);
                        waitForCompletion(restoringNodeId,
                                          migrateAsyncId,
                                          adminClientConfig.getRestoreDataTimeout(),
                                          TimeUnit.SECONDS);

                        logger.info("restoring data for store:" + storeDef.getName()
                                    + " from node " + donorNodeId + " completed.");
                    } catch(Exception e) {
                        logger.error("restore operation for store " + storeDef.getName()
                                     + "from node " + donorNodeId + " failed.", e);
                    }
                }
            });
        }
    }

    /**
     * For a particular node and routing strategy, generates a mapping of node
     * to their corresponding list of replica partitions.
     * 
     * @param cluster The cluster metadata
     * @param nodeId The id of the node
     * @param strategy The routing strategy used
     * @return Mapping of node to replica partitions of nodeId
     */
    public Map<Integer, List<Integer>> getReplicationMapping(Cluster cluster,
                                                             int nodeId,
                                                             RoutingStrategy strategy) {
        Map<Integer, Integer> partitionsToNodeMapping = RebalanceUtils.getCurrentPartitionMapping(cluster);
        HashMap<Integer, List<Integer>> restoreMapping = new HashMap<Integer, List<Integer>>();

        for(int partition: getNodePartitions(cluster, nodeId, strategy)) {
            List<Integer> replicationPartitionsList = strategy.getReplicatingPartitionList(partition);
            if(replicationPartitionsList.size() > 1) {
                int index = 0;
                int replicatingPartition = replicationPartitionsList.get(index++);
                while(partitionsToNodeMapping.get(replicatingPartition) == nodeId) {
                    replicatingPartition = replicationPartitionsList.get(index++);
                }

                int replicatingNode = partitionsToNodeMapping.get(replicatingPartition);

                if(!restoreMapping.containsKey(replicatingNode)) {
                    restoreMapping.put(replicatingNode, new ArrayList<Integer>());
                }

                if(!restoreMapping.get(replicatingNode).contains(replicatingPartition))
                    restoreMapping.get(replicatingNode).add(replicatingPartition);
            }
        }

        return restoreMapping;
    }

    /**
     * For a particular node id and routing strategy, finds all the partitions
     * which are replicas to the partitions belonging to this particular node.
     * This returned list includes the partitions belonging to the particular
     * node as well. <br>
     * 
     * For example, say we have 4 nodes, N_0 => P_3, N_1 => P_0, N_2 => P_1 and
     * N_3 => P_2 and if zone routing is being used then the replica mapping is
     * P_3 => P_1, P_0 => P_1, P_1 => P_3 and P_2 => P_3. So if we're moving
     * node N_0, the replicas of partition P_3 would be P_1 and P_2 ( in other
     * words we need to read from nodes N_2 and N_3 respectively)
     * 
     * @param cluster The cluster metadata
     * @param nodeId The id of the node
     * @param strategy Routing strategy used
     * @return List of replica partitions
     */
    private List<Integer> getNodePartitions(Cluster cluster, int nodeId, RoutingStrategy strategy) {
        List<Integer> partitionsList = new ArrayList<Integer>(cluster.getNodeById(nodeId)
                                                                     .getPartitionIds());
        Map<Integer, Integer> partitionsToNodeMapping = RebalanceUtils.getCurrentPartitionMapping(cluster);

        // add all partitions which nodeId replicates
        for(Node node: cluster.getNodes()) {
            if(node.getId() != nodeId) {
                for(int partition: node.getPartitionIds()) {
                    List<Integer> replicatedPartitions = strategy.getReplicatingPartitionList(partition);
                    for(int replicationPartition: replicatedPartitions) {
                        if(partitionsToNodeMapping.get(replicationPartition) == nodeId) {
                            partitionsList.add(partition);
                        }
                    }
                }
            }
        }

        return partitionsList;
    }

    /**
     * Rebalance a stealer,donor node pair for the given storeName.<br>
     * stealInfo also have a storeName list, this is passed to client to persist
     * in case of failure and start balancing all the stores in the list only.
     * 
     * @param stealInfo
     * @return The request id of the async operation
     */
    public int rebalanceNode(RebalancePartitionsInfo stealInfo) {
        VAdminProto.InitiateRebalanceNodeRequest rebalanceNodeRequest = VAdminProto.InitiateRebalanceNodeRequest.newBuilder()
                                                                                                                .setAttempt(stealInfo.getAttempt())
                                                                                                                .setDonorId(stealInfo.getDonorId())
                                                                                                                .setStealerId(stealInfo.getStealerId())
                                                                                                                .addAllPartitions(stealInfo.getPartitionList())
                                                                                                                .addAllUnbalancedStore(stealInfo.getUnbalancedStoreList())
                                                                                                                .addAllDeletePartitions(stealInfo.getDeletePartitionsList())
                                                                                                                .addAllStealMasterPartitions(stealInfo.getStealMasterPartitions())
                                                                                                                .addAllStealerRoStoreToDir(decodeROStoreVersionDirMap(stealInfo.getStealerNodeROStoreToDir()))
                                                                                                                .addAllDonorRoStoreToDir(decodeROStoreVersionDirMap(stealInfo.getDonorNodeROStoreToDir()))
                                                                                                                .build();
        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                          .setType(VAdminProto.AdminRequestType.INITIATE_REBALANCE_NODE)
                                                                                          .setInitiateRebalanceNode(rebalanceNodeRequest)
                                                                                          .build();
        VAdminProto.AsyncOperationStatusResponse.Builder response = sendAndReceive(stealInfo.getStealerId(),
                                                                                   adminRequest,
                                                                                   VAdminProto.AsyncOperationStatusResponse.newBuilder());

        if(response.hasError())
            throwException(response.getError());

        return response.getRequestId();
    }

    /**
     * Migrate keys/values belonging to stealPartitionList from donorNode to
     * stealerNode. <b>Does not delete the partitions from donorNode, merely
     * copies them. </b>
     * <p>
     * This is a background operation (see
     * {@link voldemort.server.protocol.admin.AsyncOperation} that runs on the
     * node on which the updates are performed (the "stealer" node). See
     * {@link AdminClient#updateEntries} for more informaiton on the "streaming"
     * mode.
     * 
     * @param donorNodeId Node <em>from</em> which the partitions are to be
     *        streamed.
     * @param stealerNodeId Node <em>to</em> which the partitions are to be
     *        streamed.
     * @param storeName Name of the store to stream.
     * @param stealPartitionList List of partitions to stream.
     * @param filter Custom filter implementation to filter out entries which
     *        should not be deleted.
     * @return The value of the
     *         {@link voldemort.server.protocol.admin.AsyncOperation} created on
     *         stealerNodeId which is performing the operation.
     */
    public int migratePartitions(int donorNodeId,
                                 int stealerNodeId,
                                 String storeName,
                                 List<Integer> stealPartitionList,
                                 VoldemortFilter filter) {
        VAdminProto.InitiateFetchAndUpdateRequest.Builder initiateFetchAndUpdateRequest = VAdminProto.InitiateFetchAndUpdateRequest.newBuilder()
                                                                                                                                   .setNodeId(donorNodeId)
                                                                                                                                   .addAllPartitions(stealPartitionList)
                                                                                                                                   .setStore(storeName);
        try {
            if(filter != null) {
                initiateFetchAndUpdateRequest.setFilter(encodeFilter(filter));
            }
        } catch(IOException e) {
            throw new VoldemortException(e);
        }

        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                          .setInitiateFetchAndUpdate(initiateFetchAndUpdateRequest)
                                                                                          .setType(VAdminProto.AdminRequestType.INITIATE_FETCH_AND_UPDATE)
                                                                                          .build();
        VAdminProto.AsyncOperationStatusResponse.Builder response = sendAndReceive(stealerNodeId,
                                                                                   adminRequest,
                                                                                   VAdminProto.AsyncOperationStatusResponse.newBuilder());

        if(response.hasError()) {
            throwException(response.getError());
        }

        return response.getRequestId();
    }

    /**
     * Delete the store completely (<b>Deletes all Data</b>) from the remote
     * node.
     * <p>
     * 
     * @param nodeId The node id on which the store is present
     * @param storeName The name of the store
     */
    public void truncate(int nodeId, String storeName) {
        VAdminProto.TruncateEntriesRequest.Builder truncateRequest = VAdminProto.TruncateEntriesRequest.newBuilder()
                                                                                                       .setStore(storeName);

        VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                     .setType(VAdminProto.AdminRequestType.TRUNCATE_ENTRIES)
                                                                                     .setTruncateEntries(truncateRequest)
                                                                                     .build();
        VAdminProto.TruncateEntriesResponse.Builder response = sendAndReceive(nodeId,
                                                                              request,
                                                                              VAdminProto.TruncateEntriesResponse.newBuilder());

        if(response.hasError()) {
            throwException(response.getError());
        }
    }

    /**
     * Get the status of an Async Operation running at (remote) node.
     * 
     * <b>If The operation is complete, then the operation will be removed from
     * a list of currently running operations.</b>
     * 
     * @param nodeId Id on which the operation is running
     * @param requestId Id of the operation itself
     * @return The status of the operation
     */
    public AsyncOperationStatus getAsyncRequestStatus(int nodeId, int requestId) {
        VAdminProto.AsyncOperationStatusRequest asyncRequest = VAdminProto.AsyncOperationStatusRequest.newBuilder()
                                                                                                      .setRequestId(requestId)
                                                                                                      .build();
        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                          .setType(VAdminProto.AdminRequestType.ASYNC_OPERATION_STATUS)
                                                                                          .setAsyncOperationStatus(asyncRequest)
                                                                                          .build();
        VAdminProto.AsyncOperationStatusResponse.Builder response = sendAndReceive(nodeId,
                                                                                   adminRequest,
                                                                                   VAdminProto.AsyncOperationStatusResponse.newBuilder());

        if(response.hasError())
            throwException(response.getError());

        AsyncOperationStatus status = new AsyncOperationStatus(response.getRequestId(),
                                                               response.getDescription());
        status.setStatus(response.getStatus());
        status.setComplete(response.getComplete());

        return status;
    }

    /**
     * Retrieves a list of asynchronous request ids on the server. Does not
     * include the completed requests
     * 
     * @param nodeId The id of the node whose request ids we want
     * @return List of async request ids
     */
    public List<Integer> getAsyncRequestList(int nodeId) {
        return getAsyncRequestList(nodeId, false);
    }

    /**
     * Retrieves a list of asynchronous request ids on the server. Depending on
     * the boolean passed also retrieves the completed requests
     * 
     * @param nodeId The id of the node whose request ids we want
     * @param showComplete Boolean to indicate if we want to include the
     *        completed requests as well
     * @return List of async request ids
     */
    public List<Integer> getAsyncRequestList(int nodeId, boolean showComplete) {
        VAdminProto.AsyncOperationListRequest asyncOperationListRequest = VAdminProto.AsyncOperationListRequest.newBuilder()
                                                                                                               .setShowComplete(showComplete)
                                                                                                               .build();
        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                          .setType(VAdminProto.AdminRequestType.ASYNC_OPERATION_LIST)
                                                                                          .setAsyncOperationList(asyncOperationListRequest)
                                                                                          .build();
        VAdminProto.AsyncOperationListResponse.Builder response = sendAndReceive(nodeId,
                                                                                 adminRequest,
                                                                                 VAdminProto.AsyncOperationListResponse.newBuilder());
        if(response.hasError())
            throwException(response.getError());

        return response.getRequestIdsList();
    }

    /**
     * To stop an asynchronous request on the particular node
     * 
     * @param nodeId The id of the node on which the request is running
     * @param requestId The id of the request to terminate
     */
    public void stopAsyncRequest(int nodeId, int requestId) {
        VAdminProto.AsyncOperationStopRequest asyncOperationStopRequest = VAdminProto.AsyncOperationStopRequest.newBuilder()
                                                                                                               .setRequestId(requestId)
                                                                                                               .build();
        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                          .setType(VAdminProto.AdminRequestType.ASYNC_OPERATION_STOP)
                                                                                          .setAsyncOperationStop(asyncOperationStopRequest)
                                                                                          .build();
        VAdminProto.AsyncOperationStopResponse.Builder response = sendAndReceive(nodeId,
                                                                                 adminRequest,
                                                                                 VAdminProto.AsyncOperationStopResponse.newBuilder());

        if(response.hasError())
            throwException(response.getError());
    }

    private VAdminProto.VoldemortFilter encodeFilter(VoldemortFilter filter) throws IOException {
        Class<?> cl = filter.getClass();
        byte[] classBytes = networkClassLoader.dumpClass(cl);
        return VAdminProto.VoldemortFilter.newBuilder()
                                          .setName(cl.getName())
                                          .setData(ProtoUtils.encodeBytes(new ByteArray(classBytes)))
                                          .build();
    }

    /**
     * Delete all entries belonging to partitionList at requested node.
     * 
     * @param nodeId Node on which the entries to be deleted
     * @param storeName Name of the store holding the entries
     * @param partitionList List of partitions to delete.
     * @param filter Custom filter implementation to filter out entries which
     *        should not be deleted.
     * @throws VoldemortException
     * @return Number of partitions deleted
     */
    public int deletePartitions(int nodeId,
                                String storeName,
                                List<Integer> partitionList,
                                VoldemortFilter filter) {
        VAdminProto.DeletePartitionEntriesRequest.Builder deleteRequest = VAdminProto.DeletePartitionEntriesRequest.newBuilder()
                                                                                                                   .addAllPartitions(partitionList)
                                                                                                                   .setStore(storeName);

        try {
            if(filter != null) {
                deleteRequest.setFilter(encodeFilter(filter));
            }
        } catch(IOException e) {
            throw new VoldemortException(e);
        }

        VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                     .setType(VAdminProto.AdminRequestType.DELETE_PARTITION_ENTRIES)
                                                                                     .setDeletePartitionEntries(deleteRequest)
                                                                                     .build();
        VAdminProto.DeletePartitionEntriesResponse.Builder response = sendAndReceive(nodeId,
                                                                                     request,
                                                                                     VAdminProto.DeletePartitionEntriesResponse.newBuilder());

        if(response.hasError())
            throwException(response.getError());

        return response.getCount();
    }

    public void throwException(VProto.Error error) {
        throw errorMapper.getError((short) error.getErrorCode(), error.getErrorMessage());
    }

    private void close(Socket socket) {
        try {
            socket.close();
        } catch(IOException e) {
            logger.warn("Failed to close socket");
        }
    }

    /**
     * Stop the AdminClient cleanly freeing all resources.
     */
    public void stop() {
        this.pool.close();
    }

    /**
     * Wait for async task at (remote) nodeId to finish completion, using
     * exponential backoff to poll the task completion status.
     * <p>
     * 
     * <i>Logs the status at each status check if debug is enabled.</i>
     * 
     * @param nodeId Id of the node to poll
     * @param requestId Id of the request to check
     * @param maxWait Maximum time we'll keep checking a request until we give
     *        up
     * @param timeUnit Unit in which maxWait is expressed.
     * @return description The description attached with the response
     * @throws VoldemortException if task failed to finish in specified maxWait
     *         time.
     */
    public String waitForCompletion(int nodeId, int requestId, long maxWait, TimeUnit timeUnit) {
        long delay = INITIAL_DELAY;
        long waitUntil = System.currentTimeMillis() + timeUnit.toMillis(maxWait);

        String description = null;
        while(System.currentTimeMillis() < waitUntil) {
            try {
                AsyncOperationStatus status = getAsyncRequestStatus(nodeId, requestId);
                logger.info("Status from node " + nodeId + " (" + status.getDescription() + ") - "
                            + status.getStatus());
                description = status.getDescription();
                if(status.hasException())
                    throw status.getException();

                if(status.isComplete())
                    return status.getStatus();

                if(delay < adminClientConfig.getMaxBackoffDelayMs())
                    delay <<= 1;

                try {
                    Thread.sleep(delay);
                } catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } catch(Exception e) {
                throw new VoldemortException("Failed while waiting for async task " + description
                                             + " at node " + nodeId + " to finish", e);
            }
        }
        throw new VoldemortException("Failed to finish task requestId: " + requestId
                                     + " in maxWait " + maxWait + " " + timeUnit.toString());
    }

    /**
     * Wait till the passed value matches with the metadata value returned by
     * the remote node for the passed key.
     * <p>
     * 
     * <i>Logs the status at each status check if debug is enabled.</i>
     * 
     * @param nodeId Id of the node to poll
     * @param key metadata key to keep checking for current value
     * @param value metadata value should match for exit criteria.
     * @param maxWait Maximum time we'll keep checking a request until we give
     *        up
     * @param timeUnit Unit in which maxWait is expressed.
     */
    public void waitForCompletion(int nodeId,
                                  String key,
                                  String value,
                                  long maxWait,
                                  TimeUnit timeUnit) {
        long delay = INITIAL_DELAY;
        long waitUntil = System.currentTimeMillis() + timeUnit.toMillis(maxWait);

        while(System.currentTimeMillis() < waitUntil) {
            String currentValue = getRemoteMetadata(nodeId, key).getValue();
            if(value.equals(currentValue))
                return;

            logger.debug("waiting for value " + value + " for metadata key " + key
                         + " from remote node " + nodeId + " currentValue " + currentValue);

            if(delay < adminClientConfig.getMaxBackoffDelayMs())
                delay <<= 1;

            try {
                Thread.sleep(delay);
            } catch(InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        throw new VoldemortException("Failed to get matching value " + value + " for key " + key
                                     + " at remote node " + nodeId + " in maximum wait" + maxWait
                                     + " " + timeUnit.toString() + " time.");
    }

    /**
     * Update metadata at the given remoteNodeId.
     * <p>
     * 
     * Metadata keys can be one of {@link MetadataStore#METADATA_KEYS}<br>
     * eg.<br>
     * <li>cluster metadata (cluster.xml as string)
     * <li>stores definitions (stores.xml as string)
     * <li>Server states <br <br>
     * See {@link voldemort.store.metadata.MetadataStore} for more information.
     * 
     * @param remoteNodeId Id of the node
     * @param key Metadata key to update
     * @param value Value for the metadata key
     */
    public void updateRemoteMetadata(int remoteNodeId, String key, Versioned<String> value) {
        ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(key, "UTF-8"));
        Versioned<byte[]> valueBytes = new Versioned<byte[]>(ByteUtils.getBytes(value.getValue(),
                                                                                "UTF-8"),
                                                             value.getVersion());

        VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                     .setType(VAdminProto.AdminRequestType.UPDATE_METADATA)
                                                                                     .setUpdateMetadata(VAdminProto.UpdateMetadataRequest.newBuilder()
                                                                                                                                         .setKey(ByteString.copyFrom(keyBytes.get()))
                                                                                                                                         .setVersioned(ProtoUtils.encodeVersioned(valueBytes))
                                                                                                                                         .build())
                                                                                     .build();
        VAdminProto.UpdateMetadataResponse.Builder response = sendAndReceive(remoteNodeId,
                                                                             request,
                                                                             VAdminProto.UpdateMetadataResponse.newBuilder());
        if(response.hasError())
            throwException(response.getError());
    }

    /**
     * Get the metadata on a remote node.
     * <p>
     * Metadata keys can be one of {@link MetadataStore#METADATA_KEYS}<br>
     * eg.<br>
     * <li>cluster metadata (cluster.xml as string)
     * <li>stores definitions (stores.xml as string)
     * <li>Server states <br <br>
     * See {@link voldemort.store.metadata.MetadataStore} for more information.
     * 
     * @param remoteNodeId Id of the node
     * @param key Metadata key to update
     * @return Metadata with its associated {@link voldemort.versioning.Version}
     */
    public Versioned<String> getRemoteMetadata(int remoteNodeId, String key) {
        ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(key, "UTF-8"));
        VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                     .setType(VAdminProto.AdminRequestType.GET_METADATA)
                                                                                     .setGetMetadata(VAdminProto.GetMetadataRequest.newBuilder()
                                                                                                                                   .setKey(ByteString.copyFrom(keyBytes.get())))
                                                                                     .build();
        VAdminProto.GetMetadataResponse.Builder response = sendAndReceive(remoteNodeId,
                                                                          request,
                                                                          VAdminProto.GetMetadataResponse.newBuilder());

        if(response.hasError())
            throwException(response.getError());

        Versioned<byte[]> value = ProtoUtils.decodeVersioned(response.getVersion());
        return new Versioned<String>(ByteUtils.getString(value.getValue(), "UTF-8"),
                                     value.getVersion());
    }

    /**
     * Update the cluster information {@link MetadataStore#CLUSTER_KEY} on a
     * remote node.
     * <p>
     * 
     * @param nodeId Id of the remote node
     * @param cluster The new cluster object
     * @throws VoldemortException
     */
    public void updateRemoteCluster(int nodeId, Cluster cluster, Version clock)
            throws VoldemortException {
        updateRemoteMetadata(nodeId,
                             MetadataStore.CLUSTER_KEY,
                             new Versioned<String>(clusterMapper.writeCluster(cluster), clock));
    }

    /**
     * Get the cluster information from a remote node.
     * <p>
     * 
     * @param nodeId Node to retrieve information from
     * @return A cluster object with its {@link voldemort.versioning.Version}
     * @throws VoldemortException
     */
    public Versioned<Cluster> getRemoteCluster(int nodeId) throws VoldemortException {
        Versioned<String> value = getRemoteMetadata(nodeId, MetadataStore.CLUSTER_KEY);
        Cluster cluster = clusterMapper.readCluster(new StringReader(value.getValue()), false);
        return new Versioned<Cluster>(cluster, value.getVersion());
    }

    /**
     * Update the store definitions on a remote node.
     * <p>
     * 
     * @param nodeId The node id of the machine
     * @param storesList The new store list
     * @throws VoldemortException
     */
    public void updateRemoteStoreDefList(int nodeId, List<StoreDefinition> storesList)
            throws VoldemortException {
        // get current version.
        VectorClock oldClock = (VectorClock) getRemoteStoreDefList(nodeId).getVersion();

        updateRemoteMetadata(nodeId,
                             MetadataStore.STORES_KEY,
                             new Versioned<String>(storeMapper.writeStoreList(storesList),
                                                   oldClock.incremented(nodeId, 1)));
    }

    /**
     * Retrieve the store definitions from a remote node.
     * <p>
     * 
     * @param nodeId The node id from which we can to remote the store
     *        definition
     * @return The list of store definitions from the remote machine
     * @throws VoldemortException
     */
    public Versioned<List<StoreDefinition>> getRemoteStoreDefList(int nodeId)
            throws VoldemortException {
        Versioned<String> value = getRemoteMetadata(nodeId, MetadataStore.STORES_KEY);
        List<StoreDefinition> storeList = storeMapper.readStoreList(new StringReader(value.getValue()),
                                                                    false);
        return new Versioned<List<StoreDefinition>>(storeList, value.getVersion());
    }

    /**
     * Update the server state (
     * {@link voldemort.store.metadata.MetadataStore.VoldemortState}) on a
     * remote node.
     */
    public void updateRemoteServerState(int nodeId,
                                        MetadataStore.VoldemortState state,
                                        Version clock) {
        updateRemoteMetadata(nodeId,
                             MetadataStore.SERVER_STATE_KEY,
                             new Versioned<String>(state.toString(), clock));
    }

    /**
     * Retrieve the server
     * {@link voldemort.store.metadata.MetadataStore.VoldemortState state} from
     * a remote node.
     */
    public Versioned<VoldemortState> getRemoteServerState(int nodeId) {
        Versioned<String> value = getRemoteMetadata(nodeId, MetadataStore.SERVER_STATE_KEY);
        return new Versioned<VoldemortState>(VoldemortState.valueOf(value.getValue()),
                                             value.getVersion());
    }

    /**
     * Add a new store definition to all active nodes in the cluster.
     * <p>
     * 
     * @param def the definition of the store to add
     */
    public void addStore(StoreDefinition def) {
        VAdminProto.VoldemortAdminRequest request = createAddStoreRequest(def);
        for(Node node: currentCluster.getNodes()) {
            logger.info("Adding on node " + node.getHost() + ":" + node.getId());
            VAdminProto.AddStoreResponse.Builder response = sendAndReceive(node.getId(),
                                                                           request,
                                                                           VAdminProto.AddStoreResponse.newBuilder());
            if(response.hasError())
                throwException(response.getError());
            logger.info("Successfully added on node " + node.getHost() + ":" + node.getId());
        }
    }

    public void addStore(StoreDefinition def, int nodeId) {
        VAdminProto.VoldemortAdminRequest request = createAddStoreRequest(def);
        Node node = currentCluster.getNodeById(nodeId);
        if(null == node)
            throw new VoldemortException("Invalid node id (" + nodeId +") specified");
        logger.info("Adding on node " + node.getHost() + ":" + node.getId());
        VAdminProto.AddStoreResponse.Builder response = sendAndReceive(nodeId,
                                                                       request,
                                                                       VAdminProto.AddStoreResponse.newBuilder());
        if(response.hasError())
            throwException(response.getError());
        logger.info("Succesfully added on node " + node.getHost() + ": " + node.getId());
    }

    private VAdminProto.VoldemortAdminRequest createAddStoreRequest(StoreDefinition def) {
        String value = storeMapper.writeStore(def);

        VAdminProto.AddStoreRequest.Builder addStoreRequest = VAdminProto.AddStoreRequest.newBuilder()
                                                                                         .setStoreDefinition(value);
        return VAdminProto.VoldemortAdminRequest.newBuilder()
                                                .setType(VAdminProto.AdminRequestType.ADD_STORE)
                                                .setAddStore(addStoreRequest)
                                                .build();
    }

    /**
     * Delete a store from all active nodes in the cluster
     * <p>
     * 
     * @param storeName name of the store to delete
     */
    public void deleteStore(String storeName) {
        VAdminProto.DeleteStoreRequest.Builder deleteStoreRequest = VAdminProto.DeleteStoreRequest.newBuilder()
                                                                                                  .setStoreName(storeName);
        VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                     .setType(VAdminProto.AdminRequestType.DELETE_STORE)
                                                                                     .setDeleteStore(deleteStoreRequest)
                                                                                     .build();
        for(Node node: currentCluster.getNodes()) {
            logger.info("Deleting on node " + node.getHost() + ":" + node.getId());
            VAdminProto.DeleteStoreResponse.Builder response = sendAndReceive(node.getId(),
                                                                              request,
                                                                              VAdminProto.DeleteStoreResponse.newBuilder());
            if(response.hasError())
                throwException(response.getError());
            logger.info("Successfully deleted on node " + node.getHost() + ":" + node.getId());
        }
    }

    /**
     * Set cluster info for AdminClient to use.
     * 
     * @param cluster
     */
    public void setAdminClientCluster(Cluster cluster) {
        this.currentCluster = cluster;
    }

    /**
     * Get the cluster info AdminClient is using.
     * 
     * @return
     */
    public Cluster getAdminClientCluster() {
        return currentCluster;
    }

    /**
     * Rollback RO store to most recent backup of the current store
     * <p>
     * 
     * @param nodeId The node id on which to rollback
     * @param storeName The name of the RO Store to rollback
     * @param pushVersion The version of the push to revert back to
     */
    public void rollbackStore(int nodeId, String storeName, long pushVersion) {
        VAdminProto.RollbackStoreRequest.Builder rollbackStoreRequest = VAdminProto.RollbackStoreRequest.newBuilder()
                                                                                                        .setStoreName(storeName)
                                                                                                        .setPushVersion(pushVersion);

        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                          .setRollbackStore(rollbackStoreRequest)
                                                                                          .setType(VAdminProto.AdminRequestType.ROLLBACK_STORE)
                                                                                          .build();
        VAdminProto.RollbackStoreResponse.Builder response = sendAndReceive(nodeId,
                                                                            adminRequest,
                                                                            VAdminProto.RollbackStoreResponse.newBuilder());
        if(response.hasError()) {
            throwException(response.getError());
        }
        return;
    }

    /**
     * Fetch data from directory 'storeDir' on node id
     * <p>
     * 
     * @param nodeId The id of the node on which to fetch the data
     * @param storeName The name of the store
     * @param storeDir The directory from where to read the data
     * @param pushVersion The version of the push
     * @param timeoutMs Time timeout in milliseconds
     * @return The path of the directory where the data is stored finally
     */
    public String fetchStore(int nodeId,
                             String storeName,
                             String storeDir,
                             long pushVersion,
                             long timeoutMs) {
        VAdminProto.FetchStoreRequest.Builder fetchStoreRequest = VAdminProto.FetchStoreRequest.newBuilder()
                                                                                               .setStoreName(storeName)
                                                                                               .setStoreDir(storeDir);
        if(pushVersion > 0) {
            fetchStoreRequest.setPushVersion(pushVersion);
        }

        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                          .setFetchStore(fetchStoreRequest)
                                                                                          .setType(VAdminProto.AdminRequestType.FETCH_STORE)
                                                                                          .build();
        VAdminProto.AsyncOperationStatusResponse.Builder response = sendAndReceive(nodeId,
                                                                                   adminRequest,
                                                                                   VAdminProto.AsyncOperationStatusResponse.newBuilder());

        if(response.hasError()) {
            throwException(response.getError());
        }

        int asyncId = response.getRequestId();
        return waitForCompletion(nodeId, asyncId, timeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * When a fetch store fails, we don't need to keep the pushed data around.
     * This function deletes its...
     * 
     * @param nodeId The node id on which to delete the data
     * @param storeName The name of the store
     * @param storeDir The directory to delete
     */
    public void failedFetchStore(int nodeId, String storeName, String storeDir) {
        VAdminProto.FailedFetchStoreRequest.Builder failedFetchStoreRequest = VAdminProto.FailedFetchStoreRequest.newBuilder()
                                                                                                                 .setStoreDir(storeDir)
                                                                                                                 .setStoreName(storeName);

        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                          .setFailedFetchStore(failedFetchStoreRequest)
                                                                                          .setType(VAdminProto.AdminRequestType.FAILED_FETCH_STORE)
                                                                                          .build();
        VAdminProto.FailedFetchStoreResponse.Builder response = sendAndReceive(nodeId,
                                                                               adminRequest,
                                                                               VAdminProto.FailedFetchStoreResponse.newBuilder());
        if(response.hasError()) {
            throwException(response.getError());
        }
        return;
    }

    /**
     * Swap store data atomically on a single node
     * <p>
     * 
     * @param nodeId The node id where we would want to swap the data
     * @param storeName Name of the store
     * @param storeDir The directory where the data is present
     * @return Returns the location of the previous directory
     */
    public String swapStore(int nodeId, String storeName, String storeDir) {
        VAdminProto.SwapStoreRequest.Builder swapStoreRequest = VAdminProto.SwapStoreRequest.newBuilder()
                                                                                            .setStoreDir(storeDir)
                                                                                            .setStoreName(storeName);
        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                          .setSwapStore(swapStoreRequest)
                                                                                          .setType(VAdminProto.AdminRequestType.SWAP_STORE)
                                                                                          .build();
        VAdminProto.SwapStoreResponse.Builder response = sendAndReceive(nodeId,
                                                                        adminRequest,
                                                                        VAdminProto.SwapStoreResponse.newBuilder());
        if(response.hasError()) {
            throwException(response.getError());
        }
        return response.getPreviousStoreDir();
    }

    /**
     * Swap multiple read-only stores and clear the rebalancing state
     * 
     * @param nodeId The node id on which to swap the stores
     * @param storeToDir A map of read-only store names with their respective
     *        directories
     */
    public void swapStoresAndCleanState(int nodeId, Map<String, String> storeToDir) {
        VAdminProto.SwapStoresAndCleanStateRequest.Builder swapStoreRequest = VAdminProto.SwapStoresAndCleanStateRequest.newBuilder()
                                                                                                                        .addAllRoStoreVersions(decodeROStoreVersionDirMap(storeToDir));

        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                          .setSwapStoresAndCleanState(swapStoreRequest)
                                                                                          .setType(VAdminProto.AdminRequestType.SWAP_STORES_AND_CLEAN_STATE)
                                                                                          .build();
        VAdminProto.SwapStoresAndCleanStateResponse.Builder response = sendAndReceive(nodeId,
                                                                                      adminRequest,
                                                                                      VAdminProto.SwapStoresAndCleanStateResponse.newBuilder());
        if(response.hasError()) {
            throwException(response.getError());
        }
        return;
    }

    /**
     * Returns the max version of push currently being used by read-only store.
     * Important to remember that this may not be the 'current' version since
     * multiple pushes (with greater version numbers) may be in progress
     * currently
     * 
     * @param nodeId The id of the node on which the store is present
     * @param storeNames List of all the stores
     * @return Returns a map of store name to the respective max version number
     */
    public Map<String, Long> getROMaxVersion(int nodeId, List<String> storeNames) {
        Map<String, Long> returnMap = Maps.newHashMapWithExpectedSize(storeNames.size());
        Map<String, String> versionDirs = getROMaxVersionDir(nodeId, storeNames);
        for(String storeName: versionDirs.keySet()) {
            returnMap.put(storeName,
                          ReadOnlyUtils.getVersionId(new File(versionDirs.get(storeName))));
        }
        return returnMap;
    }

    /**
     * Returns the max version of push currently being used by read-only store.
     * Important to remember that this may not be the 'current' version since
     * multiple pushes (with greater version numbers) may be in progress
     * currently
     * 
     * @param nodeId The id of the node on which the store is present
     * @param storeNames List of all the stores
     * @return Returns a map of store name to the respective store directory
     */
    public Map<String, String> getROMaxVersionDir(int nodeId, List<String> storeNames) {

        VAdminProto.GetROMaxVersionDirRequest.Builder getROMaxVersionDirRequest = VAdminProto.GetROMaxVersionDirRequest.newBuilder()
                                                                                                                       .addAllStoreName(storeNames);
        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                          .setGetRoMaxVersionDir(getROMaxVersionDirRequest)
                                                                                          .setType(VAdminProto.AdminRequestType.GET_RO_MAX_VERSION_DIR)
                                                                                          .build();
        VAdminProto.GetROMaxVersionDirResponse.Builder response = sendAndReceive(nodeId,
                                                                                 adminRequest,
                                                                                 VAdminProto.GetROMaxVersionDirResponse.newBuilder());
        if(response.hasError()) {
            throwException(response.getError());
        }

        // generate map of store-name to max version
        Map<String, String> storeToVersionDir = encodeROStoreVersionDirMap(response.getRoStoreVersionsList());

        if(storeToVersionDir.size() != storeNames.size()) {
            storeNames.removeAll(storeToVersionDir.keySet());
            throw new VoldemortException("Did not retrieve max version id for " + storeNames);
        }
        return storeToVersionDir;
    }

    /**
     * Returns the 'current' version of RO store
     * 
     * @param nodeId The id of the node on which the store is present
     * @param storeNames List of all the stores
     * @return Returns a map of store name to the respective max version
     *         directory
     */
    public Map<String, String> getROCurrentVersionDir(int nodeId, List<String> storeNames) {
        VAdminProto.GetROCurrentVersionDirRequest.Builder getROCurrentVersionDirRequest = VAdminProto.GetROCurrentVersionDirRequest.newBuilder()
                                                                                                                                   .addAllStoreName(storeNames);
        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                          .setGetRoCurrentVersionDir(getROCurrentVersionDirRequest)
                                                                                          .setType(VAdminProto.AdminRequestType.GET_RO_CURRENT_VERSION_DIR)
                                                                                          .build();
        VAdminProto.GetROCurrentVersionDirResponse.Builder response = sendAndReceive(nodeId,
                                                                                     adminRequest,
                                                                                     VAdminProto.GetROCurrentVersionDirResponse.newBuilder());
        if(response.hasError()) {
            throwException(response.getError());
        }

        // generate map of store-name to current version
        Map<String, String> storeToVersionDir = encodeROStoreVersionDirMap(response.getRoStoreVersionsList());

        if(storeToVersionDir.size() != storeNames.size()) {
            storeNames.removeAll(storeToVersionDir.keySet());
            throw new VoldemortException("Did not retrieve current version id for " + storeNames);
        }
        return storeToVersionDir;
    }

    /**
     * Returns the 'current' version of RO store
     * 
     * @param nodeId The id of the node on which the store is present
     * @param storeNames List of all the stores
     * @return Returns a map of store name to the respective max version number
     */
    public Map<String, Long> getROCurrentVersion(int nodeId, List<String> storeNames) {
        Map<String, Long> returnMap = Maps.newHashMapWithExpectedSize(storeNames.size());
        Map<String, String> versionDirs = getROCurrentVersionDir(nodeId, storeNames);
        for(String storeName: versionDirs.keySet()) {
            returnMap.put(storeName,
                          ReadOnlyUtils.getVersionId(new File(versionDirs.get(storeName))));
        }
        return returnMap;
    }

    private List<ROStoreVersionDirMap> decodeROStoreVersionDirMap(Map<String, String> storeVersionDirMap) {
        List<ROStoreVersionDirMap> storeToVersionDir = Lists.newArrayList();
        for(String storeName: storeVersionDirMap.keySet()) {
            storeToVersionDir.add(ROStoreVersionDirMap.newBuilder()
                                                      .setStoreName(storeName)
                                                      .setStoreDir(storeVersionDirMap.get(storeName))
                                                      .build());
        }
        return storeToVersionDir;
    }

    private Map<String, String> encodeROStoreVersionDirMap(List<ROStoreVersionDirMap> storeVersionDirMap) {
        Map<String, String> storeToVersionDir = Maps.newHashMap();
        for(ROStoreVersionDirMap currentStore: storeVersionDirMap) {
            storeToVersionDir.put(currentStore.getStoreName(), currentStore.getStoreDir());
        }
        return storeToVersionDir;
    }

    /**
     * This is a wrapper around
     * {@link voldemort.client.protocol.admin.AdminClient#getROMaxVersion(int, List)}
     * where-in we find the max versions on each machine and then return the max
     * of all of them
     * 
     * @param storeName List of all read-only stores
     * @return A map of store-name to their corresponding max version id
     */
    public Map<String, Long> getROMaxVersion(List<String> storeNames) {
        Map<String, Long> storeToMaxVersion = Maps.newHashMapWithExpectedSize(storeNames.size());
        for(String storeName: storeNames) {
            storeToMaxVersion.put(storeName, 0L);
        }

        for(Node node: currentCluster.getNodes()) {
            Map<String, Long> currentNodeVersions = getROMaxVersion(node.getId(), storeNames);
            for(String storeName: currentNodeVersions.keySet()) {
                Long maxVersion = storeToMaxVersion.get(storeName);
                if(maxVersion != null && maxVersion < currentNodeVersions.get(storeName)) {
                    storeToMaxVersion.put(storeName, currentNodeVersions.get(storeName));
                }
            }
        }
        return storeToMaxVersion;
    }

    /**
     * Update slops which may be meant for multiple stores
     * 
     * @param nodeId The id of the node
     * @param entryIterator An iterator over all the slops for this particular
     *        node
     */
    public void updateSlopEntries(int nodeId, Iterator<Versioned<Slop>> entryIterator) {
        Node node = this.getAdminClientCluster().getNodeById(nodeId);
        SocketDestination destination = new SocketDestination(node.getHost(),
                                                              node.getAdminPort(),
                                                              RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
        SocketAndStreams sands = pool.checkout(destination);
        DataOutputStream outputStream = sands.getOutputStream();
        DataInputStream inputStream = sands.getInputStream();
        boolean firstMessage = true;

        try {
            if(entryIterator.hasNext()) {
                while(entryIterator.hasNext()) {
                    Versioned<Slop> versionedSlop = entryIterator.next();
                    Slop slop = versionedSlop.getValue();

                    // Build the message
                    RequestType requestType = null;
                    if(slop.getOperation().equals(Operation.PUT)) {
                        requestType = RequestType.PUT;
                    } else if(slop.getOperation().equals(Operation.DELETE)) {
                        requestType = RequestType.DELETE;
                    } else {
                        logger.error("Unsupported operation. Skipping");
                        continue;
                    }
                    VAdminProto.UpdateSlopEntriesRequest.Builder updateRequest = VAdminProto.UpdateSlopEntriesRequest.newBuilder()
                                                                                                                     .setStore(slop.getStoreName())
                                                                                                                     .setKey(ProtoUtils.encodeBytes(slop.getKey()))
                                                                                                                     .setVersion(ProtoUtils.encodeClock(versionedSlop.getVersion()))
                                                                                                                     .setRequestType(requestType);
                    // Add transforms and value only if required
                    if(slop.getTransforms() != null)
                        updateRequest.setTransform(ProtoUtils.encodeTransform(slop.getTransforms()));
                    if(slop.getValue() != null)
                        updateRequest.setValue(ByteString.copyFrom(slop.getValue()));

                    if(firstMessage) {
                        ProtoUtils.writeMessage(outputStream,
                                                VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                 .setType(VAdminProto.AdminRequestType.UPDATE_SLOP_ENTRIES)
                                                                                 .setUpdateSlopEntries(updateRequest)
                                                                                 .build());
                        outputStream.flush();
                        firstMessage = false;
                    } else {
                        ProtoUtils.writeMessage(outputStream, updateRequest.build());
                    }
                }
                ProtoUtils.writeEndOfStream(outputStream);
                outputStream.flush();
                VAdminProto.UpdateSlopEntriesResponse.Builder updateResponse = ProtoUtils.readToBuilder(inputStream,
                                                                                                        VAdminProto.UpdateSlopEntriesResponse.newBuilder());
                if(updateResponse.hasError()) {
                    throwException(updateResponse.getError());
                }
            }
        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }

    }

    /**
     * Fetch read-only store files to a specified directory
     * 
     * @param nodeId The node id from where to copy
     * @param storeName The name of the read-only store
     * @param partitionIds The partition ids from whom to copy data
     * @param destinationDirPath The destination path
     */
    public void fetchPartitionFiles(int nodeId,
                                    String storeName,
                                    List<Integer> partitionIds,
                                    String destinationDirPath) {
        if(!Utils.isReadableDir(destinationDirPath)) {
            throw new VoldemortException("The destination path (" + destinationDirPath
                                         + ") to store " + storeName + " does not exist");
        }

        Node node = this.getAdminClientCluster().getNodeById(nodeId);
        final SocketDestination destination = new SocketDestination(node.getHost(),
                                                                    node.getAdminPort(),
                                                                    RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
        final SocketAndStreams sands = pool.checkout(destination);
        DataOutputStream outputStream = sands.getOutputStream();
        final DataInputStream inputStream = sands.getInputStream();

        try {
            VAdminProto.FetchPartitionFilesRequest fetchPartitionFileRequest = VAdminProto.FetchPartitionFilesRequest.newBuilder()
                                                                                                                     .addAllPartitions(partitionIds)
                                                                                                                     .setStore(storeName)
                                                                                                                     .build();
            VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                         .setFetchPartitionFiles(fetchPartitionFileRequest)
                                                                                         .setType(VAdminProto.AdminRequestType.FETCH_PARTITION_FILES)
                                                                                         .build();
            ProtoUtils.writeMessage(outputStream, request);
            outputStream.flush();

            while(true) {
                int size = 0;

                try {
                    size = inputStream.readInt();
                } catch(EOFException e) {
                    logger.error("Received EOF Exception while fetching files", e);
                    close(sands.getSocket());
                    return;
                }

                if(size == -1) {
                    close(sands.getSocket());
                    break;
                }

                byte[] input = new byte[size];
                ByteUtils.read(inputStream, input);
                VAdminProto.FileEntry fileEntry = VAdminProto.FileEntry.newBuilder()
                                                                       .mergeFrom(input)
                                                                       .build();
                logger.info("Receiving file " + fileEntry.getFileName());
                FileChannel fileChannel = new FileOutputStream(new File(destinationDirPath,
                                                                        fileEntry.getFileName())).getChannel();
                ReadableByteChannel channelIn = Channels.newChannel(inputStream);
                fileChannel.transferFrom(channelIn, 0, fileEntry.getFileSizeBytes());
                fileChannel.force(true);
                fileChannel.close();

                logger.info("Completed file " + fileEntry.getFileName());
            }

        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }

    }

    /**
     * Updates the grandfather metadata on the remote machine and returns the
     * current
     * 
     * @param donorNodeId The node id on which to update the metadata
     * @param plan The list of all migrating partitions
     * @return Returns the current state of the server. If we get back a state
     *         other than rebalancing it means we have a problem
     */
    public Versioned<String> updateGrandfatherMetadata(int donorNodeId,
                                                       List<RebalancePartitionsInfo> plans) {
        List<VAdminProto.InitiateRebalanceNodeRequest> rebalanceRequests = Lists.newArrayList();

        for(RebalancePartitionsInfo plan: plans) {
            rebalanceRequests.add(VAdminProto.InitiateRebalanceNodeRequest.newBuilder()
                                                                          .setAttempt(plan.getAttempt())
                                                                          .setDonorId(plan.getDonorId())
                                                                          .setStealerId(plan.getStealerId())
                                                                          .addAllPartitions(plan.getPartitionList())
                                                                          .addAllUnbalancedStore(plan.getUnbalancedStoreList())
                                                                          .addAllDeletePartitions(plan.getDeletePartitionsList())
                                                                          .addAllStealMasterPartitions(plan.getStealMasterPartitions())
                                                                          .addAllStealerRoStoreToDir(decodeROStoreVersionDirMap(plan.getStealerNodeROStoreToDir()))
                                                                          .addAllDonorRoStoreToDir(decodeROStoreVersionDirMap(plan.getDonorNodeROStoreToDir()))
                                                                          .build());
        }

        UpdateGrandfatherMetadataRequest metadataRequest = VAdminProto.UpdateGrandfatherMetadataRequest.newBuilder()
                                                                                                       .addAllPlan(rebalanceRequests)
                                                                                                       .build();
        VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                     .setType(VAdminProto.AdminRequestType.UPDATE_GRANDFATHER_METADATA)
                                                                                     .setUpdateGrandfatherMetadata(metadataRequest)
                                                                                     .build();

        VAdminProto.UpdateGrandfatherMetadataResponse.Builder response = sendAndReceive(donorNodeId,
                                                                                        request,
                                                                                        VAdminProto.UpdateGrandfatherMetadataResponse.newBuilder());

        if(response.hasError()) {
            throwException(response.getError());
        }

        Versioned<byte[]> value = ProtoUtils.decodeVersioned(response.getVersion());
        return new Versioned<String>(ByteUtils.getString(value.getValue(), "UTF-8"),
                                     value.getVersion());
    }
}
