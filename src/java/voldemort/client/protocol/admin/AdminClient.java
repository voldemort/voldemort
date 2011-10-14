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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.Socket;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VAdminProto.RebalancePartitionInfoMap;
import voldemort.client.protocol.pb.VProto;
import voldemort.client.protocol.pb.VProto.RequestType;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.server.rebalance.VoldemortRebalancingException;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.mysql.MysqlStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
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
import com.google.common.collect.Sets;
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
                                      HashMap<Integer, List<Integer>> replicaToPartitionList,
                                      VoldemortFilter filter,
                                      boolean fetchValues,
                                      boolean fetchMasterEntries,
                                      Cluster initialCluster,
                                      long skipRecords) throws IOException {
        HashMap<Integer, List<Integer>> filteredReplicaToPartitionList = Maps.newHashMap();
        if(fetchMasterEntries) {
            if(!replicaToPartitionList.containsKey(0)) {
                throw new VoldemortException("Could not find any partitions for primary replica type");
            } else {
                filteredReplicaToPartitionList.put(0, replicaToPartitionList.get(0));
            }
        } else {
            filteredReplicaToPartitionList.putAll(replicaToPartitionList);
        }
        VAdminProto.FetchPartitionEntriesRequest.Builder fetchRequest = VAdminProto.FetchPartitionEntriesRequest.newBuilder()
                                                                                                                .setFetchValues(fetchValues)
                                                                                                                .addAllReplicaToPartition(ProtoUtils.encodePartitionTuple(filteredReplicaToPartitionList))
                                                                                                                .setStore(storeName)
                                                                                                                .setSkipRecords(skipRecords);

        try {
            if(filter != null) {
                fetchRequest.setFilter(encodeFilter(filter));
            }
        } catch(IOException e) {
            throw new VoldemortException(e);
        }

        if(initialCluster != null) {
            fetchRequest.setInitialCluster(new ClusterMapper().writeCluster(initialCluster));
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
     * Legacy interface for fetching entries. See
     * {@link AdminClient#fetchEntries(int, String, HashMap, VoldemortFilter, boolean, Cluster, long)}
     * for more information.
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
     */
    public Iterator<Pair<ByteArray, Versioned<byte[]>>> fetchEntries(int nodeId,
                                                                     String storeName,
                                                                     List<Integer> partitionList,
                                                                     VoldemortFilter filter,
                                                                     boolean fetchMasterEntries,
                                                                     long skipRecords) {
        return fetchEntries(nodeId,
                            storeName,
                            getReplicaToPartitionMap(nodeId, storeName, partitionList),
                            filter,
                            fetchMasterEntries,
                            null,
                            skipRecords);
    }

    /**
     * Legacy interface for fetching entries. See
     * {@link AdminClient#fetchEntries(int, String, HashMap, VoldemortFilter, boolean, Cluster, long)}
     * for more information.
     *
     * @param nodeId Id of the node to fetch from
     * @param storeName Name of the store
     * @param partitionList List of the partitions
     * @param filter Custom filter implementation to filter out entries which
     *        should not be fetched.
     * @param fetchMasterEntries Fetch an entry only if master replica
     * @return An iterator which allows entries to be streamed as they're being
     *         iterated over.
     */
    public Iterator<Pair<ByteArray, Versioned<byte[]>>> fetchEntries(int nodeId,
                                                                     String storeName,
                                                                     List<Integer> partitionList,
                                                                     VoldemortFilter filter,
                                                                     boolean fetchMasterEntries) {
        return fetchEntries(nodeId, storeName, partitionList, filter, fetchMasterEntries, 0);
    }

    /**
     * Fetch key/value tuples belonging to this map of replica type to partition
     * list
     * <p>
     *
     * <b>Streaming API</b> - The server keeps sending the messages as it's
     * iterating over the data. Once iteration has finished, the server sends an
     * "end of stream" marker and flushes its buffer. A response indicating a
     * {@link VoldemortException} may be sent at any time during the process.
     * <br>
     *
     * <p>
     * Entries are being streamed <em>as the iteration happens</em> i.e. the
     * whole result set is <b>not</b> buffered in memory.
     *
     * @param nodeId Id of the node to fetch from
     * @param storeName Name of the store
     * @param replicaToPartitionList Mapping of replica type to partition list
     * @param filter Custom filter implementation to filter out entries which
     *        should not be fetched.
     * @param fetchMasterEntries Fetch an entry only if master replica
     * @param initialCluster The cluster metadata to use while making the
     *        decision to fetch entries. This is important during rebalancing
     *        where-in we want to fetch keys using an older metadata compared to
     *        the new one.
     * @param skipRecords Number of records to skip
     * @return An iterator which allows entries to be streamed as they're being
     *         iterated over.
     */
    public Iterator<Pair<ByteArray, Versioned<byte[]>>> fetchEntries(int nodeId,
                                                                     String storeName,
                                                                     HashMap<Integer, List<Integer>> replicaToPartitionList,
                                                                     VoldemortFilter filter,
                                                                     boolean fetchMasterEntries,
                                                                     Cluster initialCluster,
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
                                 replicaToPartitionList,
                                 filter,
                                 true,
                                 fetchMasterEntries,
                                 initialCluster,
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
     * Legacy interface for fetching entries. See
     * {@link AdminClient#fetchKeys(int, String, HashMap, VoldemortFilter, boolean, Cluster, long)}
     * for more information.
     *
     * @param nodeId Id of the node to fetch from
     * @param storeName Name of the store
     * @param partitionList List of the partitions to retrieve
     * @param filter Custom filter implementation to filter out entries which
     *        should not be fetched.
     * @param fetchMasterEntries Fetch a key only if master replica
     * @param skipRecords Number of keys to skip
     * @return An iterator which allows keys to be streamed as they're being
     *         iterated over.
     */
    public Iterator<ByteArray> fetchKeys(int nodeId,
                                         String storeName,
                                         List<Integer> partitionList,
                                         VoldemortFilter filter,
                                         boolean fetchMasterEntries,
                                         long skipRecords) {
        return fetchKeys(nodeId,
                         storeName,
                         getReplicaToPartitionMap(nodeId, storeName, partitionList),
                         filter,
                         fetchMasterEntries,
                         null,
                         skipRecords);
    }

    /**
     * Legacy interface for fetching entries. See
     * {@link AdminClient#fetchKeys(int, String, HashMap, VoldemortFilter, boolean, Cluster, long)}
     * for more information.
     *
     * @param nodeId Id of the node to fetch from
     * @param storeName Name of the store
     * @param partitionList List of the partitions to retrieve
     * @param filter Custom filter implementation to filter out entries which
     *        should not be fetched.
     * @param fetchMasterEntries Fetch a key only if master replica
     * @return An iterator which allows keys to be streamed as they're being
     *         iterated over.
     */
    public Iterator<ByteArray> fetchKeys(int nodeId,
                                         String storeName,
                                         List<Integer> partitionList,
                                         VoldemortFilter filter,
                                         boolean fetchMasterEntries) {
        return fetchKeys(nodeId, storeName, partitionList, filter, fetchMasterEntries, 0);
    }

    /**
     * Fetch all keys belonging to the map of replica type to partition list.
     * Identical to {@link AdminClient#fetchEntries} but
     * <em>only fetches the keys</em>
     *
     * @param nodeId The node id from where to fetch the keys
     * @param storeName The store name whose keys we want to retrieve
     * @param replicaToPartitionList Map of replica type to corresponding
     *        partition list
     * @param filter Custom filter
     * @param initialCluster Cluster to use for selecting a key. If null, use
     *        the default metadata from the metadata store
     * @param skipRecords Number of records to skip [ Used for sampling ]
     * @return Returns an iterator of the keys
     */
    public Iterator<ByteArray> fetchKeys(int nodeId,
                                         String storeName,
                                         HashMap<Integer, List<Integer>> replicaToPartitionList,
                                         VoldemortFilter filter,
                                         boolean fetchMasterEntries,
                                         Cluster initialCluster,
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
                                 replicaToPartitionList,
                                 filter,
                                 false,
                                 fetchMasterEntries,
                                 initialCluster,
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
                if(def.isView()) {
                    logger.info("Ignoring store " + def.getName() + " since it is a view");
                } else if(restoreStoreEngineBlackList.contains(def.getType())) {
                    logger.info("Ignoring store " + def.getName()
                                + " since we don't support restoring for " + def.getType()
                                + " storage engine");
                } else if(def.getReplicationFactor() == 1) {
                    logger.info("Ignoring store " + def.getName()
                                + " since replication factor is set to 1");
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
                executors.awaitTermination(adminClientConfig.getRestoreDataTimeoutSec(),
                                           TimeUnit.SECONDS);
            } catch(InterruptedException e) {
                logger.error("Interrupted while waiting restore operation to finish.");
            }
            logger.info("Finished restoring data.");
        }
    }

    /**
     * For a particular node, finds out all the [replica, partition] tuples it
     * needs to steal in order to be brought back to normal state
     *
     * @param restoringNode The id of the node which needs to be restored
     * @param cluster The cluster definition
     * @param storeDef The store definition to use
     * @return Map of node id to map of replica type and corresponding partition
     *         list
     */
    public Map<Integer, HashMap<Integer, List<Integer>>> getReplicationMapping(int restoringNode,
                                                                               Cluster cluster,
                                                                               StoreDefinition storeDef) {

        Map<Integer, Integer> partitionToNodeId = RebalanceUtils.getCurrentPartitionMapping(cluster);
        Map<Integer, HashMap<Integer, List<Integer>>> returnMap = Maps.newHashMap();

        RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                      cluster);
        List<Integer> restoringNodePartition = cluster.getNodeById(restoringNode).getPartitionIds();

        // Go over every partition. As long as one of them belongs to the
        // current node list, find its replica
        for(Node node: cluster.getNodes()) {
            for(int partitionId: node.getPartitionIds()) {
                List<Integer> replicatingPartitions = strategy.getReplicatingPartitionList(partitionId);
                List<Integer> extraCopyReplicatingPartitions = Lists.newArrayList(replicatingPartitions);

                if(replicatingPartitions.size() <= 1) {
                    throw new VoldemortException("Store "
                                                 + storeDef.getName()
                                                 + " cannot be restored from replica because replication factor = 1");
                }

                if(replicatingPartitions.removeAll(restoringNodePartition)) {
                    if(replicatingPartitions.size() == 0) {
                        throw new VoldemortException("Found a case where-in the overlap of "
                                                     + "the node partition list results in no replicas "
                                                     + "being left in replicating list");
                    }

                    // Pick the first element and find its position in the
                    // origin replicating list
                    int replicaType = extraCopyReplicatingPartitions.indexOf(replicatingPartitions.get(0));
                    int partition = extraCopyReplicatingPartitions.get(0);
                    int nodeId = partitionToNodeId.get(replicatingPartitions.get(0));

                    HashMap<Integer, List<Integer>> replicaToPartitionList = null;
                    if(returnMap.containsKey(nodeId)) {
                        replicaToPartitionList = returnMap.get(nodeId);
                    } else {
                        replicaToPartitionList = Maps.newHashMap();
                        returnMap.put(nodeId, replicaToPartitionList);
                    }

                    List<Integer> partitions = null;
                    if(replicaToPartitionList.containsKey(replicaType)) {
                        partitions = replicaToPartitionList.get(replicaType);
                    } else {
                        partitions = Lists.newArrayList();
                        replicaToPartitionList.put(replicaType, partitions);
                    }
                    partitions.add(partition);
                }

            }
        }
        return returnMap;
    }

    /**
     * For a particular store and node, runs the replication job. This works
     * only for read-write stores
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

        Map<Integer, HashMap<Integer, List<Integer>>> restoreMapping = getReplicationMapping(restoringNodeId,
                                                                                             cluster,
                                                                                             storeDef);
        // migrate partition
        for(final Entry<Integer, HashMap<Integer, List<Integer>>> replicationEntry: restoreMapping.entrySet()) {
            final int donorNodeId = replicationEntry.getKey();
            executorService.submit(new Runnable() {

                public void run() {
                    try {
                        logger.info("Restoring data for store " + storeDef.getName() + " at node "
                                    + restoringNodeId + " from node " + replicationEntry.getKey()
                                    + " partitions:" + replicationEntry.getValue());

                        int migrateAsyncId = migratePartitions(donorNodeId,
                                                               restoringNodeId,
                                                               storeDef.getName(),
                                                               replicationEntry.getValue(),
                                                               null,
                                                               null,
                                                               false);
                        waitForCompletion(restoringNodeId,
                                          migrateAsyncId,
                                          adminClientConfig.getRestoreDataTimeoutSec(),
                                          TimeUnit.SECONDS);

                        logger.info("Restoring data for store:" + storeDef.getName()
                                    + " from node " + donorNodeId + " completed.");
                    } catch(Exception e) {
                        logger.error("Restore operation for store " + storeDef.getName()
                                     + "from node " + donorNodeId + " failed.", e);
                    }
                }
            });
        }
    }

    /**
     * Rebalance a stealer-donor node pair for a set of stores
     *
     * @param stealInfo Partition steal information
     * @return The request id of the async operation
     */
    public int rebalanceNode(RebalancePartitionsInfo stealInfo) {
        VAdminProto.RebalancePartitionInfoMap rebalancePartitionInfoMap = ProtoUtils.encodeRebalancePartitionInfoMap(stealInfo);
        VAdminProto.InitiateRebalanceNodeRequest rebalanceNodeRequest = VAdminProto.InitiateRebalanceNodeRequest.newBuilder()
                                                                                                                .setRebalancePartitionInfo(rebalancePartitionInfoMap)
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
     * Converts list of partitions to map of replica type to partition list.
     *
     * @param nodeId Node which is donating data
     * @param storeName Name of store
     * @param partitions List of partitions ( primary OR replicas ) to move
     * @return Map of replica type to partitions
     */
    private HashMap<Integer, List<Integer>> getReplicaToPartitionMap(int nodeId,
                                                                     String storeName,
                                                                     List<Integer> partitions) {

        StoreDefinition def = RebalanceUtils.getStoreDefinitionWithName(getRemoteStoreDefList(nodeId).getValue(),
                                                                        storeName);
        HashMap<Integer, List<Integer>> replicaToPartitionList = Maps.newHashMap();
        for(int replicaNum = 0; replicaNum < def.getReplicationFactor(); replicaNum++) {
            replicaToPartitionList.put(replicaNum, partitions);
        }

        return replicaToPartitionList;
    }

    /**
     * Migrate keys/values belonging to stealPartitionList ( can be primary or
     * replica ) from donor node to stealer node. <b>Does not delete the
     * partitions from donorNode, merely copies them. </b>
     * <p>
     * See
     * {@link AdminClient#migratePartitions(int, int, String, HashMap, VoldemortFilter, Cluster, boolean)}
     * for more details.
     *
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
        return migratePartitions(donorNodeId,
                                 stealerNodeId,
                                 storeName,
                                 getReplicaToPartitionMap(donorNodeId,
                                                          storeName,
                                                          stealPartitionList),
                                 filter,
                                 null,
                                 false);
    }

    /**
     * Migrate keys/values belonging to a map of replica type to partition list
     * from donor node to stealer node. <b>Does not delete the partitions from
     * donorNode, merely copies them. </b>
     * <p>
     * This is a background operation (see
     * {@link voldemort.server.protocol.admin.AsyncOperation} that runs on the
     * stealer node where updates are performed.
     * <p>
     *
     * @param donorNodeId Node <em>from</em> which the partitions are to be
     *        streamed.
     * @param stealerNodeId Node <em>to</em> which the partitions are to be
     *        streamed.
     * @param storeName Name of the store to stream.
     * @param replicaToPartitionList Mapping from replica type to partition to
     *        be stolen
     * @param filter Voldemort post-filter
     * @param initialCluster The cluster metadata to use for making the decision
     *        if the key belongs to these partitions. If not specified, falls
     *        back to the metadata stored on the box
     * @param optimize We can run an optimization at this level where-in we try
     *        avoid copying of data which already exists ( in the form of a
     *        replica ). We do need to disable this when we're trying to recover
     *        a node which was completely damaged ( restore from replica ).
     * @return The value of the
     *         {@link voldemort.server.protocol.admin.AsyncOperation} created on
     *         stealer node which is performing the operation.
     */
    public int migratePartitions(int donorNodeId,
                                 int stealerNodeId,
                                 String storeName,
                                 HashMap<Integer, List<Integer>> replicaToPartitionList,
                                 VoldemortFilter filter,
                                 Cluster initialCluster,
                                 boolean optimize) {
        VAdminProto.InitiateFetchAndUpdateRequest.Builder initiateFetchAndUpdateRequest = VAdminProto.InitiateFetchAndUpdateRequest.newBuilder()
                                                                                                                                   .setNodeId(donorNodeId)
                                                                                                                                   .addAllReplicaToPartition(ProtoUtils.encodePartitionTuple(replicaToPartitionList))
                                                                                                                                   .setStore(storeName);

        try {
            if(filter != null) {
                initiateFetchAndUpdateRequest.setFilter(encodeFilter(filter));
            }
        } catch(IOException e) {
            throw new VoldemortException(e);
        }

        if(initialCluster != null) {
            initiateFetchAndUpdateRequest.setInitialCluster(new ClusterMapper().writeCluster(initialCluster));
        }
        initiateFetchAndUpdateRequest.setOptimize(optimize);

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
     * Delete the store completely (<b>Deletes all data</b>) from the remote
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
     * Delete all entries belonging to a list of partitions
     *
     * @param nodeId Node on which the entries to be deleted
     * @param storeName Name of the store holding the entries
     * @param partitionList List of partitions to delete.
     * @param filter Custom filter implementation to filter out entries which
     *        should not be deleted.
     * @return Number of entries deleted
     */
    public long deletePartitions(int nodeId,
                                 String storeName,
                                 List<Integer> partitionList,
                                 VoldemortFilter filter) {
        return deletePartitions(nodeId,
                                storeName,
                                getReplicaToPartitionMap(nodeId, storeName, partitionList),
                                null,
                                filter);
    }

    /**
     * Delete all entries belonging to all the partitions passed as a map of
     * replica_type to partition list. Works only for RW stores.
     *
     * @param nodeId Node on which the entries to be deleted
     * @param storeName Name of the store holding the entries
     * @param replicaToPartitionList Map of replica type to partition list
     * @param filter Custom filter implementation to filter out entries which
     *        should not be deleted.
     * @return Number of entries deleted
     */
    public long deletePartitions(int nodeId,
                                 String storeName,
                                 HashMap<Integer, List<Integer>> replicaToPartitionList,
                                 Cluster initialCluster,
                                 VoldemortFilter filter) {
        VAdminProto.DeletePartitionEntriesRequest.Builder deleteRequest = VAdminProto.DeletePartitionEntriesRequest.newBuilder()
                                                                                                                   .addAllReplicaToPartition(ProtoUtils.encodePartitionTuple(replicaToPartitionList))
                                                                                                                   .setStore(storeName);

        try {
            if(filter != null) {
                deleteRequest.setFilter(encodeFilter(filter));
            }
        } catch(IOException e) {
            throw new VoldemortException(e);
        }

        if(initialCluster != null) {
            deleteRequest.setInitialCluster(new ClusterMapper().writeCluster(initialCluster));
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
     * @param higherStatus A higher level async operation object. If this
     *        waiting is being run another async operation this helps us
     *        propagate the status all the way up.
     * @return description The final description attached with the response
     * @throws VoldemortException if task failed to finish in specified maxWait
     *         time.
     */
    public String waitForCompletion(int nodeId,
                                    int requestId,
                                    long maxWait,
                                    TimeUnit timeUnit,
                                    AsyncOperationStatus higherStatus) {
        long delay = INITIAL_DELAY;
        long waitUntil = System.currentTimeMillis() + timeUnit.toMillis(maxWait);

        String description = null;
        while(System.currentTimeMillis() < waitUntil) {
            try {
                AsyncOperationStatus status = getAsyncRequestStatus(nodeId, requestId);
                logger.info("Status from node " + nodeId + " (" + status.getDescription() + ") - "
                            + status.getStatus());
                if(higherStatus != null) {
                    higherStatus.setStatus("Status from node " + nodeId + " ("
                                           + status.getDescription() + ") - " + status.getStatus());
                }
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
                throw new VoldemortException("Failed while waiting for async task (" + description
                                             + ") at node " + nodeId + " to finish", e);
            }
        }
        throw new VoldemortException("Failed to finish task requestId: " + requestId
                                     + " in maxWait " + maxWait + " " + timeUnit.toString());
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
     * @return description The final description attached with the response
     * @throws VoldemortException if task failed to finish in specified maxWait
     *         time.
     */
    public String waitForCompletion(int nodeId, int requestId, long maxWait, TimeUnit timeUnit) {
        return waitForCompletion(nodeId, requestId, maxWait, timeUnit, null);
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
        for(Node node: currentCluster.getNodes()) {
            addStore(def, node.getId());
        }
    }

    /**
     * Add a new store definition to a particular node
     * <p>
     *
     * @param def the definition of the store to add
     * @param nodeId Node on which to add the store
     */
    public void addStore(StoreDefinition def, int nodeId) {
        String value = storeMapper.writeStore(def);

        VAdminProto.AddStoreRequest.Builder addStoreRequest = VAdminProto.AddStoreRequest.newBuilder()
                                                                                         .setStoreDefinition(value);
        VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                     .setType(VAdminProto.AdminRequestType.ADD_STORE)
                                                                                     .setAddStore(addStoreRequest)
                                                                                     .build();

        Node node = currentCluster.getNodeById(nodeId);
        if(null == node)
            throw new VoldemortException("Invalid node id (" + nodeId + ") specified");

        logger.info("Adding store " + def.getName() + " on node " + node.getHost() + ":"
                    + node.getId());
        VAdminProto.AddStoreResponse.Builder response = sendAndReceive(nodeId,
                                                                       request,
                                                                       VAdminProto.AddStoreResponse.newBuilder());
        if(response.hasError())
            throwException(response.getError());
        logger.info("Succesfully added " + def.getName() + " on node " + node.getHost() + ":"
                    + node.getId());
    }

    /**
     * Delete a store from all active nodes in the cluster
     * <p>
     *
     * @param storeName name of the store to delete
     */
    public void deleteStore(String storeName) {
        for(Node node: currentCluster.getNodes()) {
            deleteStore(storeName, node.getId());
        }
    }

    /**
     * Delete a store from a particular node
     * <p>
     *
     * @param storeName name of the store to delete
     * @param nodeId Node on which we want to delete a store
     */
    public void deleteStore(String storeName, int nodeId) {
        VAdminProto.DeleteStoreRequest.Builder deleteStoreRequest = VAdminProto.DeleteStoreRequest.newBuilder()
                                                                                                  .setStoreName(storeName);
        VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                     .setType(VAdminProto.AdminRequestType.DELETE_STORE)
                                                                                     .setDeleteStore(deleteStoreRequest)
                                                                                     .build();
        Node node = currentCluster.getNodeById(nodeId);
        if(null == node)
            throw new VoldemortException("Invalid node id (" + nodeId + ") specified");

        logger.info("Deleting " + storeName + " on node " + node.getHost() + ":" + node.getId());
        VAdminProto.DeleteStoreResponse.Builder response = sendAndReceive(node.getId(),
                                                                          request,
                                                                          VAdminProto.DeleteStoreResponse.newBuilder());
        if(response.hasError())
            throwException(response.getError());
        logger.info("Successfully deleted " + storeName + " on node " + node.getHost() + ":"
                    + node.getId());
    }

    /**
     * Set cluster info for AdminClient to use.
     *
     * @param cluster Set the current cluster
     */
    public void setAdminClientCluster(Cluster cluster) {
        this.currentCluster = cluster;
    }

    /**
     * Get the cluster info AdminClient is using.
     *
     * @return Returns the current cluster being used by the admin client
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
     * Repair the stores on a rebalanced node 'nodeId'
     * <p>
     *
     * @param nodeId The id of the node on which to do the repair
     */
    public void repairJob(int nodeId) {
        VAdminProto.RepairJobRequest.Builder repairJobRequest = VAdminProto.RepairJobRequest.newBuilder();

        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                          .setRepairJob(repairJobRequest)
                                                                                          .setType(VAdminProto.AdminRequestType.REPAIR_JOB)
                                                                                          .build();
        VAdminProto.AsyncOperationStatusResponse.Builder response = sendAndReceive(nodeId,
                                                                                   adminRequest,
                                                                                   VAdminProto.AsyncOperationStatusResponse.newBuilder());

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
     * Returns the read-only storage format - {@link ReadOnlyStorageFormat} for
     * a list of stores
     *
     * @param nodeId The id of the node on which the stores are present
     * @param storeNames List of all the store names
     * @return Returns a map of store name to its corresponding RO storage
     *         format
     */
    public Map<String, String> getROStorageFormat(int nodeId, List<String> storeNames) {
        VAdminProto.GetROStorageFormatRequest.Builder getRORequest = VAdminProto.GetROStorageFormatRequest.newBuilder()
                                                                                                          .addAllStoreName(storeNames);
        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                          .setGetRoStorageFormat(getRORequest)
                                                                                          .setType(VAdminProto.AdminRequestType.GET_RO_STORAGE_FORMAT)
                                                                                          .build();
        VAdminProto.GetROStorageFormatResponse.Builder response = sendAndReceive(nodeId,
                                                                                 adminRequest,
                                                                                 VAdminProto.GetROStorageFormatResponse.newBuilder());
        if(response.hasError()) {
            throwException(response.getError());
        }

        Map<String, String> storeToValues = ProtoUtils.encodeROMap(response.getRoStoreVersionsList());

        if(storeToValues.size() != storeNames.size()) {
            storeNames.removeAll(storeToValues.keySet());
            throw new VoldemortException("Did not retrieve values for " + storeNames);
        }
        return storeToValues;
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
        VAdminProto.GetROMaxVersionDirRequest.Builder getRORequest = VAdminProto.GetROMaxVersionDirRequest.newBuilder()
                                                                                                          .addAllStoreName(storeNames);
        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                          .setGetRoMaxVersionDir(getRORequest)
                                                                                          .setType(VAdminProto.AdminRequestType.GET_RO_MAX_VERSION_DIR)
                                                                                          .build();
        VAdminProto.GetROMaxVersionDirResponse.Builder response = sendAndReceive(nodeId,
                                                                                 adminRequest,
                                                                                 VAdminProto.GetROMaxVersionDirResponse.newBuilder());
        if(response.hasError()) {
            throwException(response.getError());
        }

        Map<String, String> storeToValues = ProtoUtils.encodeROMap(response.getRoStoreVersionsList());

        if(storeToValues.size() != storeNames.size()) {
            storeNames.removeAll(storeToValues.keySet());
            throw new VoldemortException("Did not retrieve values for " + storeNames);
        }
        return storeToValues;
    }

    /**
     * Returns the 'current' versions of all RO stores provided
     *
     * @param nodeId The id of the node on which the store is present
     * @param storeNames List of all the RO stores
     * @return Returns a map of store name to the respective max version
     *         directory
     */
    public Map<String, String> getROCurrentVersionDir(int nodeId, List<String> storeNames) {
        VAdminProto.GetROCurrentVersionDirRequest.Builder getRORequest = VAdminProto.GetROCurrentVersionDirRequest.newBuilder()
                                                                                                                  .addAllStoreName(storeNames);
        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                          .setGetRoCurrentVersionDir(getRORequest)
                                                                                          .setType(VAdminProto.AdminRequestType.GET_RO_CURRENT_VERSION_DIR)
                                                                                          .build();
        VAdminProto.GetROCurrentVersionDirResponse.Builder response = sendAndReceive(nodeId,
                                                                                     adminRequest,
                                                                                     VAdminProto.GetROCurrentVersionDirResponse.newBuilder());
        if(response.hasError()) {
            throwException(response.getError());
        }

        Map<String, String> storeToValues = ProtoUtils.encodeROMap(response.getRoStoreVersionsList());

        if(storeToValues.size() != storeNames.size()) {
            storeNames.removeAll(storeToValues.keySet());
            throw new VoldemortException("Did not retrieve values for " + storeNames);
        }
        return storeToValues;
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
     * This is a wrapper around
     * {@link voldemort.client.protocol.admin.AdminClient#getROMaxVersion(int, List)}
     * where-in we find the max versions on each machine and then return the max
     * of all of them
     *
     * @param storeNames List of all read-only stores
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
     * Fetch read-only store files to a specified directory. This is run on the
     * stealer node side
     *
     * @param nodeId The node id from where to copy
     * @param storeName The name of the read-only store
     * @param replicaToPartitionList Map of replica type to partition list
     * @param destinationDirPath The destination path
     * @param notAcceptedBuckets These are Pair< partition, replica > which we
     *        cannot copy AT all. This is because these are current mmap-ed and
     *        are serving traffic.
     * @param running A boolean which will control when we want to stop the
     *        copying of files. As long this is true, we will continue copying.
     *        Once this is changed to false we'll disable the copying
     */
    public void fetchPartitionFiles(int nodeId,
                                    String storeName,
                                    HashMap<Integer, List<Integer>> replicaToPartitionList,
                                    String destinationDirPath,
                                    Set<Object> notAcceptedBuckets,
                                    AtomicBoolean running) {
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

            // Add the metadata file if it doesn't exist - We do this because
            // for new nodes the stores don't start with any metadata file

            File metadataFile = new File(destinationDirPath, ".metadata");
            if(!metadataFile.exists()) {
                ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
                metadata.add(ReadOnlyStorageMetadata.FORMAT,
                             ReadOnlyStorageFormat.READONLY_V2.getCode());
                FileUtils.writeStringToFile(metadataFile, metadata.toJsonString());
            }

            VAdminProto.FetchPartitionFilesRequest fetchPartitionFileRequest = VAdminProto.FetchPartitionFilesRequest.newBuilder()
                                                                                                                     .addAllReplicaToPartition(ProtoUtils.encodePartitionTuple(replicaToPartitionList))
                                                                                                                     .setStore(storeName)
                                                                                                                     .build();
            VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                         .setFetchPartitionFiles(fetchPartitionFileRequest)
                                                                                         .setType(VAdminProto.AdminRequestType.FETCH_PARTITION_FILES)
                                                                                         .build();
            ProtoUtils.writeMessage(outputStream, request);
            outputStream.flush();

            while(true && running.get()) {
                int size = 0;

                try {
                    size = inputStream.readInt();
                } catch(IOException e) {
                    logger.error("Received IOException while fetching files", e);
                    throw e;
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

                if(notAcceptedBuckets != null) {
                    Pair<Integer, Integer> partitionReplicaTuple = ReadOnlyUtils.getPartitionReplicaTuple(fileEntry.getFileName());
                    if(notAcceptedBuckets.contains(partitionReplicaTuple)) {
                        throw new VoldemortException("Cannot copy file " + fileEntry.getFileName()
                                                     + " since it is one of the mmap-ed files");
                    }
                }
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
     * Used in rebalancing to indicate change in states. Groups the partition
     * plans on the basis of stealer nodes and sends them over.
     *
     * The various combinations and their order of execution is given below
     *
     * <pre>
     * | swapRO | changeClusterMetadata | changeRebalanceState | Order |
     * | f | t | t | cluster -> rebalance |
     * | f | f | t | rebalance |
     * | t | t | f | cluster -> swap |
     * | t | t | t | cluster -> swap -> rebalance |
     * </pre>
     *
     *
     * Similarly for rollback:
     *
     * <pre>
     * | swapRO | changeClusterMetadata | changeRebalanceState | Order |
     * | f | t | t | remove from rebalance -> cluster  |
     * | f | f | t | remove from rebalance |
     * | t | t | f | cluster -> swap |
     * | t | t | t | remove from rebalance -> cluster -> swap  |
     * </pre>
     *
     *
     * @param existingCluster Current cluster
     * @param transitionCluster Transition cluster
     * @param rebalancePartitionPlanList The list of rebalance partition info
     *        plans
     * @param swapRO Boolean indicating if we need to swap RO stores
     * @param changeClusterMetadata Boolean indicating if we need to change
     *        cluster metadata
     * @param changeRebalanceState Boolean indicating if we need to change
     *        rebalancing state
     * @param rollback Do we want to do a rollback step in case of failures?
     * @param failEarly Do we want to fail early while doing state change?
     */
    public void rebalanceStateChange(Cluster existingCluster,
                                     Cluster transitionCluster,
                                     List<RebalancePartitionsInfo> rebalancePartitionPlanList,
                                     boolean swapRO,
                                     boolean changeClusterMetadata,
                                     boolean changeRebalanceState,
                                     boolean rollback,
                                     boolean failEarly) {
        HashMap<Integer, List<RebalancePartitionsInfo>> stealerNodeToPlan = groupPartitionsInfoByStealerNode(rebalancePartitionPlanList);
        Set<Integer> completedNodeIds = Sets.newHashSet();

        int nodeId = 0;
        HashMap<Integer, Exception> exceptions = Maps.newHashMap();

        try {
            while(nodeId < transitionCluster.getNumberOfNodes()) {

                try {
                    individualStateChange(nodeId,
                                          transitionCluster,
                                          stealerNodeToPlan.get(nodeId),
                                          swapRO,
                                          changeClusterMetadata,
                                          changeRebalanceState,
                                          false);
                    completedNodeIds.add(nodeId);
                } catch(Exception e) {
                    exceptions.put(nodeId, e);
                    if(failEarly) {
                        throw e;
                    }
                }
                nodeId++;

            }

            if(exceptions.size() > 0) {
                throw new VoldemortRebalancingException("Got exceptions from nodes "
                                                        + exceptions.keySet());
            }
        } catch(Exception e) {

            if(rollback) {
                logger.error("Got exceptions from nodes " + exceptions.keySet()
                             + " while changing state. Rolling back state on " + completedNodeIds);

                // Rollback changes on completed nodes
                for(int completedNodeId: completedNodeIds) {
                    try {
                        individualStateChange(completedNodeId,
                                              existingCluster,
                                              stealerNodeToPlan.get(completedNodeId),
                                              swapRO,
                                              changeClusterMetadata,
                                              changeRebalanceState,
                                              true);
                    } catch(Exception exception) {
                        logger.error("Error while reverting back state change for completed node "
                                     + completedNodeId, exception);
                    }
                }
            } else {
                logger.error("Got exceptions from nodes " + exceptions.keySet()
                             + " while changing state");
            }
            throw new VoldemortRebalancingException("Got exceptions from nodes "
                                                            + exceptions.keySet()
                                                            + " while changing state",
                                                    Lists.newArrayList(exceptions.values()));
        }

    }

    /**
     * Single node rebalance state change
     *
     * @param nodeId Stealer node id
     * @param cluster Cluster information which we need to update
     * @param rebalancePartitionPlanList The list of rebalance partition info
     *        plans
     * @param swapRO Boolean indicating if we need to swap RO stores
     * @param changeClusterMetadata Boolean indicating if we need to change
     *        cluster metadata
     * @param changeRebalanceState Boolean indicating if we need to change
     *        rebalancing state
     * @param rollback Are we doing a rollback or a normal state?
     */
    private void individualStateChange(int nodeId,
                                       Cluster cluster,
                                       List<RebalancePartitionsInfo> rebalancePartitionPlanList,
                                       boolean swapRO,
                                       boolean changeClusterMetadata,
                                       boolean changeRebalanceState,
                                       boolean rollback) {

        // If we do not want to change the metadata and are not one of the
        // stealer nodes, nothing to do
        if(!changeClusterMetadata && rebalancePartitionPlanList == null) {
            return;
        }

        logger.info("Node "
                    + nodeId
                    + "] Performing "
                    + (rollback ? "rollback" : "normal")
                    + " rebalance state change "
                    + (swapRO ? "<swap RO>" : "")
                    + (changeClusterMetadata ? "<change cluster - " + cluster + ">" : "")
                    + (changeRebalanceState ? "<change rebalance state - "
                                              + rebalancePartitionPlanList + ">" : ""));

        VAdminProto.RebalanceStateChangeRequest.Builder getRebalanceStateChangeRequestBuilder = VAdminProto.RebalanceStateChangeRequest.newBuilder();

        if(rebalancePartitionPlanList != null) {
            List<RebalancePartitionInfoMap> map = Lists.newArrayList();
            for(RebalancePartitionsInfo stealInfo: rebalancePartitionPlanList) {
                RebalancePartitionInfoMap infoMap = ProtoUtils.encodeRebalancePartitionInfoMap(stealInfo);
                map.add(infoMap);
            }
            getRebalanceStateChangeRequestBuilder.addAllRebalancePartitionInfoList(map);
        }

        VAdminProto.RebalanceStateChangeRequest getRebalanceStateChangeRequest = getRebalanceStateChangeRequestBuilder.setSwapRo(swapRO)
                                                                                                                      .setChangeClusterMetadata(changeClusterMetadata)
                                                                                                                      .setChangeRebalanceState(changeRebalanceState)
                                                                                                                      .setClusterString(clusterMapper.writeCluster(cluster))
                                                                                                                      .setRollback(rollback)
                                                                                                                      .build();
        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                          .setRebalanceStateChange(getRebalanceStateChangeRequest)
                                                                                          .setType(VAdminProto.AdminRequestType.REBALANCE_STATE_CHANGE)
                                                                                          .build();
        VAdminProto.RebalanceStateChangeResponse.Builder response = sendAndReceive(nodeId,
                                                                                   adminRequest,
                                                                                   VAdminProto.RebalanceStateChangeResponse.newBuilder());
        if(response.hasError()) {
            throwException(response.getError());
        }
    }

    /**
     * Given a list of partition infos, generates a map of stealer node to list
     * of partition infos
     *
     * @param rebalancePartitionPlanList Complete list of partition plans
     * @return Flattens it into a map on a per stealer node basis
     */
    private HashMap<Integer, List<RebalancePartitionsInfo>> groupPartitionsInfoByStealerNode(List<RebalancePartitionsInfo> rebalancePartitionPlanList) {
        HashMap<Integer, List<RebalancePartitionsInfo>> stealerNodeToPlan = Maps.newHashMap();
        if(rebalancePartitionPlanList != null) {
            for(RebalancePartitionsInfo partitionInfo: rebalancePartitionPlanList) {
                List<RebalancePartitionsInfo> partitionInfos = stealerNodeToPlan.get(partitionInfo.getStealerId());
                if(partitionInfos == null) {
                    partitionInfos = Lists.newArrayList();
                    stealerNodeToPlan.put(partitionInfo.getStealerId(), partitionInfos);
                }
                partitionInfos.add(partitionInfo);
            }
        }
        return stealerNodeToPlan;
    }

    /**
     * Native backup a store
     *
     * @param nodeId The node id to backup
     * @param storeName The name of the store to backup
     * @param destinationDirPath The destination path
     */
    public void nativeBackup(int nodeId,
                             String storeName,
                             String destinationDirPath) {
        Node node = this.getAdminClientCluster().getNodeById(nodeId);

        VAdminProto.NativeBackupRequest nativeBackupRequest = VAdminProto.NativeBackupRequest.newBuilder()
                .setStoreName(storeName)
                .setBackupDir(destinationDirPath)
                .build();
        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                .setNativeBackup(nativeBackupRequest)
                .setType(VAdminProto.AdminRequestType.NATIVE_BACKUP)
                .build();
        VAdminProto.AsyncOperationStatusResponse.Builder response = sendAndReceive(nodeId,
                adminRequest,
                VAdminProto.AsyncOperationStatusResponse.newBuilder());

        if (response.hasError()) {
            throwException(response.getError());
        }

        int asyncId = response.getRequestId();
        waitForCompletion(nodeId, asyncId, 3, TimeUnit.MINUTES);
    }
}
