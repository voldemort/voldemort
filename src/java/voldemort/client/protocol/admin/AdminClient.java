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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
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
import voldemort.client.SystemStoreClient;
import voldemort.client.SystemStoreClientFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VAdminProto.RebalanceTaskInfoMap;
import voldemort.client.protocol.pb.VProto;
import voldemort.client.protocol.pb.VProto.RequestType;
import voldemort.client.rebalance.RebalanceTaskInfo;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.server.RequestRoutingType;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.server.rebalance.VoldemortRebalancingException;
import voldemort.server.storage.prunejob.VersionedPutPruneJob;
import voldemort.server.storage.repairjob.RepairJob;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.quota.QuotaType;
import voldemort.store.quota.QuotaUtils;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.store.routed.NodeValue;
import voldemort.store.slop.Slop;
import voldemort.store.slop.Slop.Operation;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketStore;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.store.system.SystemStoreConstants;
import voldemort.store.views.ViewStorageConfiguration;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.MetadataVersionStoreUtils;
import voldemort.utils.NetworkClassLoader;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.VectorClockUtils;
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
    private static final ClusterMapper clusterMapper = new ClusterMapper();
    private static final StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();

    // Parameters for exponential back off
    private static final long INITIAL_DELAY = 250; // Initial delay
    private static final long PRINT_STATS_THRESHOLD = 10000;
    private static final long PRINT_STATS_INTERVAL = 5 * 60 * 1000; // 5 minutes

    private static final String CLUSTER_VERSION_KEY = "cluster.xml";
    private static final String STORES_VERSION_KEY = "stores.xml";

    public final static List<String> restoreStoreEngineBlackList = Arrays.asList(ReadOnlyStorageConfiguration.TYPE_NAME,
                                                                                 ViewStorageConfiguration.TYPE_NAME);

    private final ErrorCodeMapper errorMapper;
    private final SocketPool socketPool;
    private final AdminStoreClient adminStoreClient;
    private final NetworkClassLoader networkClassLoader;
    private final AdminClientConfig adminClientConfig;
    private Cluster currentCluster;
    private SystemStoreClient<String, String> metadataVersionSysStoreClient = null;
    private SystemStoreClient<String, String> quotaSysStoreClient = null;
    private SystemStoreClientFactory<String, String> systemStoreFactory = null;
    private String mainBootstrapUrl = null;

    final public AdminClient.HelperOperations helperOps;
    final public AdminClient.ReplicationOperations replicaOps;
    final public AdminClient.RPCOperations rpcOps;
    final public AdminClient.MetadataManagementOperations metadataMgmtOps;
    final public AdminClient.StoreManagementOperations storeMgmtOps;
    final public AdminClient.StoreMaintenanceOperations storeMntOps;
    final public AdminClient.BulkStreamingFetchOperations bulkFetchOps;
    final public AdminClient.StreamingOperations streamingOps;
    final public AdminClient.StoreOperations storeOps;
    final public AdminClient.RestoreOperations restoreOps;
    final public AdminClient.RebalancingOperations rebalanceOps;
    final public AdminClient.ReadOnlySpecificOperations readonlyOps;
    final public AdminClient.QuotaManagementOperations quotaMgmtOps;

    /**
     * Common initialization of AdminClient.
     * 
     * @param adminClientConfig Client configuration for SocketPool-based
     *        operations.
     * @param clientConfig Client configurations for
     *        ClientRequestExecutorPool-based operations via the (private)
     *        AdminStoreClient.
     */
    private AdminClient(AdminClientConfig adminClientConfig, ClientConfig clientConfig) {
        this.helperOps = this.new HelperOperations();
        this.replicaOps = this.new ReplicationOperations();
        this.rpcOps = this.new RPCOperations();
        this.metadataMgmtOps = this.new MetadataManagementOperations();
        this.storeMgmtOps = this.new StoreManagementOperations();
        this.storeMntOps = this.new StoreMaintenanceOperations();
        this.bulkFetchOps = this.new BulkStreamingFetchOperations();
        this.streamingOps = this.new StreamingOperations();
        this.storeOps = this.new StoreOperations();
        this.restoreOps = this.new RestoreOperations();
        this.rebalanceOps = this.new RebalancingOperations();
        this.readonlyOps = this.new ReadOnlySpecificOperations();
        this.quotaMgmtOps = this.new QuotaManagementOperations();

        this.errorMapper = new ErrorCodeMapper();
        this.networkClassLoader = new NetworkClassLoader(Thread.currentThread()
                                                               .getContextClassLoader());

        this.adminClientConfig = adminClientConfig;
        this.socketPool = helperOps.createSocketPool(adminClientConfig);
        this.adminStoreClient = new AdminStoreClient(clientConfig);
        try {
            if(clientConfig.getBootstrapUrls().length > 0) {
                this.mainBootstrapUrl = clientConfig.getBootstrapUrls()[0];
            }
        } catch(IllegalStateException e) {}
    }

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
    public AdminClient(String bootstrapURL,
                       AdminClientConfig adminClientConfig,
                       ClientConfig clientConfig) {
        this(adminClientConfig, clientConfig);
        this.mainBootstrapUrl = bootstrapURL;
        this.currentCluster = helperOps.getClusterFromBootstrapURL(bootstrapURL);
        helperOps.initSystemStoreClient(this.mainBootstrapUrl, Zone.UNSET_ZONE_ID);
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
    public AdminClient(Cluster cluster,
                       AdminClientConfig adminClientConfig,
                       ClientConfig clientConfig) {
        this(adminClientConfig, clientConfig);
        this.mainBootstrapUrl = cluster.getNodes().iterator().next().getSocketUrl().toString();
        this.currentCluster = cluster;
        helperOps.initSystemStoreClient(mainBootstrapUrl, Zone.UNSET_ZONE_ID);
    }

    /**
     * Wrapper for the actual AdminClient constructor given the URL of a node in
     * the cluster.
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
     * @param zoneID The primary Zone ID for the purpose of the SystemStore
     */
    public AdminClient(String bootstrapURL,
                       AdminClientConfig adminClientConfig,
                       ClientConfig clientConfig,
                       int zoneID) {
        this(bootstrapURL, adminClientConfig, clientConfig);
        helperOps.initSystemStoreClient(bootstrapURL, zoneID);
    }

    /**
     * Stop the AdminClient cleanly freeing all resources.
     */
    public void close() {
        this.socketPool.close();
        this.adminStoreClient.close();

        if(systemStoreFactory != null) {
            systemStoreFactory.close();
        }
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
     * Helper method to construct an AdminClient with "good" default settings
     * based upon a VoldemortConfig. This helper is intended for use by
     * server-side processes such as rebalancing, restore, and so on.
     * 
     * @param voldemortConfig
     * @param cluster
     * @param numConnPerNode
     * @return newly constructed AdminClient
     */
    public static AdminClient createTempAdminClient(VoldemortConfig voldemortConfig,
                                                    Cluster cluster,
                                                    int numConnPerNode) {
        AdminClientConfig config = new AdminClientConfig().setMaxConnectionsPerNode(numConnPerNode)
                                                          .setAdminConnectionTimeoutSec(voldemortConfig.getAdminConnectionTimeout())
                                                          .setAdminSocketTimeoutSec(voldemortConfig.getAdminSocketTimeout())
                                                          .setAdminSocketBufferSize(voldemortConfig.getAdminSocketBufferSize());

        return new AdminClient(cluster, config, new ClientConfig());
    }

    /**
     * Encapsulates helper methods used across the admin client
     * 
     */
    public class HelperOperations {

        /**
         * Create a system store client based on the cached bootstrap URLs and
         * Zone ID
         * 
         * Two system store clients are created currently.
         * 
         * 1) metadata version store
         * 
         * 2) quota store
         */
        private void initSystemStoreClient(String bootstrapURL, int zoneId) {

            String[] bootstrapUrls = new String[1];
            bootstrapUrls[0] = bootstrapURL;

            try {
                if(systemStoreFactory == null) {
                    ClientConfig clientConfig = new ClientConfig();
                    clientConfig.setBootstrapUrls(bootstrapUrls);
                    clientConfig.setClientZoneId(zoneId);
                    systemStoreFactory = new SystemStoreClientFactory<String, String>(clientConfig);
                }
                String metadataVersionStoreName = SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name();
                String quotaStoreName = SystemStoreConstants.SystemStoreName.voldsys$_store_quotas.name();
                metadataVersionSysStoreClient = systemStoreFactory.createSystemStore(metadataVersionStoreName,
                                                                                     null,
                                                                                     null);
                quotaSysStoreClient = systemStoreFactory.createSystemStore(quotaStoreName,
                                                                           null,
                                                                           null);
            } catch(Exception e) {
                logger.debug("Error while creating a system store client for metadata version store/quota store.");
            }

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

        private void close(Socket socket) {
            try {
                socket.close();
            } catch(IOException e) {
                logger.warn("Failed to close socket");
            }
        }

        // TODO move as many messages to use this helper.
        private void sendAdminRequest(VAdminProto.VoldemortAdminRequest adminRequest,
                                      int destinationNodeId) {
            // TODO probably need a helper to do all this, at some point.. all
            // of this file has repeated code
            Node node = AdminClient.this.getAdminClientCluster().getNodeById(destinationNodeId);
            SocketDestination destination = new SocketDestination(node.getHost(),
                                                                  node.getAdminPort(),
                                                                  RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
            SocketAndStreams sands = socketPool.checkout(destination);

            try {
                DataOutputStream outputStream = sands.getOutputStream();
                ProtoUtils.writeMessage(outputStream, adminRequest);
                outputStream.flush();
            } catch(IOException e) {
                helperOps.close(sands.getSocket());
                throw new VoldemortException(e);
            } finally {
                socketPool.checkin(destination, sands);
            }
        }

        public void throwException(VProto.Error error) {
            throw AdminClient.this.errorMapper.getError((short) error.getErrorCode(),
                                                        error.getErrorMessage());
        }

        private VAdminProto.VoldemortFilter encodeFilter(VoldemortFilter filter) throws IOException {
            Class<?> cl = filter.getClass();
            byte[] classBytes = networkClassLoader.dumpClass(cl);
            return VAdminProto.VoldemortFilter.newBuilder()
                                              .setName(cl.getName())
                                              .setData(ProtoUtils.encodeBytes(new ByteArray(classBytes)))
                                              .build();
        }

        public List<StoreDefinition> getStoresInCluster() {
            int firstRemoteNodeId = currentCluster.getNodeIds().iterator().next();
            return metadataMgmtOps.getRemoteStoreDefList(firstRemoteNodeId).getValue();
        }

        public boolean checkStoreExistsInCluster(String storeName) {
            List<String> storesInCluster = StoreDefinitionUtils.getStoreNames(getStoresInCluster());
            if(storesInCluster.contains(storeName)) {
                return true;
            } else {
                return false;
            }
        }
    }

    public class ReplicationOperations {

        /**
         * For a particular node, finds out all the [replica, partition] tuples
         * it needs to steal in order to be brought back to normal state
         * 
         * @param restoringNode The id of the node which needs to be restored
         * @param cluster The cluster definition
         * @param storeDef The store definition to use
         * @return Map of node id to corresponding partition list
         */
        public Map<Integer, List<Integer>> getReplicationMapping(int restoringNode,
                                                                 Cluster cluster,
                                                                 StoreDefinition storeDef) {
            return getReplicationMapping(restoringNode, cluster, storeDef, -1);
        }

        /**
         * For a particular node, finds out all the [node, partition] it needs
         * to steal in order to be brought back to normal state
         * 
         * @param restoringNode The id of the node which needs to be restored
         * @param cluster The cluster definition
         * @param storeDef The store definition to use
         * @param zoneId zone from which nodes are chosen, -1 means no zone
         *        preference
         * @return Map of node id to corresponding partition list
         */
        public Map<Integer, List<Integer>> getReplicationMapping(int restoringNode,
                                                                 Cluster cluster,
                                                                 StoreDefinition storeDef,
                                                                 int zoneId) {

            Map<Integer, List<Integer>> returnMap = Maps.newHashMap();

            RoutingStrategy strategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                          cluster);
            List<Integer> restoringNodePartition = cluster.getNodeById(restoringNode)
                                                          .getPartitionIds();

            // Go over every partition. As long as one of them belongs to the
            // current node list, find its replicating partitions
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

                        addDonorWithZonePreference(replicatingPartitions,
                                                   extraCopyReplicatingPartitions,
                                                   returnMap,
                                                   zoneId,
                                                   cluster,
                                                   storeDef);
                    }

                }
            }
            return returnMap;
        }

        /**
         * For each partition that need to be restored, find a donor node that
         * owns the partition AND has the same zone ID as requested. -1 means no
         * zone preference required when finding a donor node needs to steal in
         * order to
         * 
         * @param remainderPartitions The replicating partitions without the one
         *        needed by the restore node
         * @param originalPartitions The entire replicating partition list
         *        (including the one needed by the restore node)
         * @param donorMap All donor nodes that will be fetched from
         * @param zondId The zone from which donor nodes will be chosen from; -1
         *        means all zones are fine
         * @param cluster The cluster metadata
         * @param storeDef The store to be restored
         * @return
         */
        private void addDonorWithZonePreference(List<Integer> remainderPartitions,
                                                List<Integer> originalPartitions,
                                                Map<Integer, List<Integer>> donorMap,
                                                int zoneId,
                                                Cluster cluster,
                                                StoreDefinition storeDef) {

            Map<Integer, Integer> partitionToNodeId = cluster.getPartitionIdToNodeIdMap();
            int nodeId = -1;
            int partitionId = -1;
            boolean found = false;
            int index = 0;

            while(!found && index < remainderPartitions.size()) {
                nodeId = partitionToNodeId.get(remainderPartitions.get(index));
                if(-1 == zoneId || cluster.getNodeById(nodeId).getZoneId() == zoneId) {
                    found = true;
                } else {
                    index++;
                }
            }
            if(!found) {
                throw new VoldemortException("unable to find a node to fetch partition "
                                             + partitionId + " for store " + storeDef.getName());
            }
            partitionId = originalPartitions.get(0);
            List<Integer> partitionIds = null;
            if(donorMap.containsKey(nodeId)) {
                partitionIds = donorMap.get(nodeId);
            } else {
                partitionIds = new ArrayList<Integer>();
                donorMap.put(nodeId, partitionIds);
            }
            partitionIds.add(partitionId);
        }
    }

    /**
     * Encapsulates all the RPC helper methods
     * 
     */
    public class RPCOperations {

        private <T extends Message.Builder> T innerSendAndReceive(SocketAndStreams sands,
                                                                  Message message,
                                                                  T builder) throws IOException {
            DataOutputStream outputStream = sands.getOutputStream();
            DataInputStream inputStream = sands.getInputStream();
            ProtoUtils.writeMessage(outputStream, message);
            outputStream.flush();
            return ProtoUtils.readToBuilder(inputStream, builder);
        }

        /**
         * tests socket connection by sending a get metadata request
         * 
         * @throws IOException
         */
        private SocketAndStreams getSocketAndStreams(SocketDestination destination)
                throws IOException {
            ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(MetadataStore.SERVER_STATE_KEY,
                                                                  "UTF-8"));
            VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                         .setType(VAdminProto.AdminRequestType.GET_METADATA)
                                                                                         .setGetMetadata(VAdminProto.GetMetadataRequest.newBuilder()
                                                                                                                                       .setKey(ByteString.copyFrom(keyBytes.get())))
                                                                                         .build();
            SocketAndStreams sands = socketPool.checkout(destination);
            try {
                rpcOps.innerSendAndReceive(sands,
                                           request,
                                           VAdminProto.GetMetadataResponse.newBuilder());
            } catch(EOFException eofe) {
                helperOps.close(sands.getSocket());
                socketPool.checkin(destination, sands);
                socketPool.close(destination);
                sands = socketPool.checkout(destination);
                System.out.println("Socket connection to " + destination.getHost() + ":"
                                   + destination.getPort() + " was stale and it's now refreshed.");
            }
            return sands;
        }

        private <T extends Message.Builder> T sendAndReceive(int nodeId, Message message, T builder) {
            Node node = currentCluster.getNodeById(nodeId);
            SocketDestination destination = new SocketDestination(node.getHost(),
                                                                  node.getAdminPort(),
                                                                  RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
            SocketAndStreams sands = null;
            try {
                sands = getSocketAndStreams(destination);
                return innerSendAndReceive(sands, message, builder);
            } catch(IOException e) {
                helperOps.close(sands.getSocket());
                throw new VoldemortException(e);
            } finally {
                if(sands != null) {
                    socketPool.checkin(destination, sands);
                }
            }
        }

        /**
         * Get the status of an Async Operation running at (remote) node.
         * 
         * <b>If The operation is complete, then the operation will be removed
         * from a list of currently running operations.</b>
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
            VAdminProto.AsyncOperationStatusResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                              adminRequest,
                                                                                              VAdminProto.AsyncOperationStatusResponse.newBuilder());

            if(response.hasError())
                helperOps.throwException(response.getError());

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
         * Retrieves a list of asynchronous request ids on the server. Depending
         * on the boolean passed also retrieves the completed requests
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
            VAdminProto.AsyncOperationListResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                            adminRequest,
                                                                                            VAdminProto.AsyncOperationListResponse.newBuilder());
            if(response.hasError())
                helperOps.throwException(response.getError());

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
            VAdminProto.AsyncOperationStopResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                            adminRequest,
                                                                                            VAdminProto.AsyncOperationStopResponse.newBuilder());

            if(response.hasError())
                helperOps.throwException(response.getError());
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
         * @param maxWait Maximum time we'll keep checking a request until we
         *        give up. Pass in 0 or less to wait "forever".
         * @param timeUnit Unit in which maxWait is expressed.
         * @param higherStatus A higher level async operation object. If this
         *        waiting is being run another async operation this helps us
         *        propagate the status all the way up.
         * @return description The final description attached with the response
         * @throws VoldemortException if task failed to finish in specified
         *         maxWait time.
         */
        public String waitForCompletion(int nodeId,
                                        int requestId,
                                        long maxWait,
                                        TimeUnit timeUnit,
                                        AsyncOperationStatus higherStatus) {
            long delay = INITIAL_DELAY;
            long waitUntil = Long.MAX_VALUE;
            if(maxWait > 0) {
                waitUntil = System.currentTimeMillis() + timeUnit.toMillis(maxWait);
            }

            String description = null;
            String oldStatus = "";
            while(System.currentTimeMillis() < waitUntil) {
                try {
                    AsyncOperationStatus status = getAsyncRequestStatus(nodeId, requestId);
                    if(!status.getStatus().equalsIgnoreCase(oldStatus)) {
                        logger.info("Status from node " + nodeId + " (" + status.getDescription()
                                    + ") - " + status.getStatus());
                    }
                    oldStatus = status.getStatus();

                    if(higherStatus != null) {
                        higherStatus.setStatus("Status from node " + nodeId + " ("
                                               + status.getDescription() + ") - "
                                               + status.getStatus());
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
                    throw new VoldemortException("Failed while waiting for async task ("
                                                 + description + ") at node " + nodeId
                                                 + " to finish", e);
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
         * @param maxWait Maximum time we'll keep checking a request until we
         *        give up
         * @param timeUnit Unit in which maxWait is expressed.
         * @return description The final description attached with the response
         * @throws VoldemortException if task failed to finish in specified
         *         maxWait time.
         */
        public String waitForCompletion(int nodeId, int requestId, long maxWait, TimeUnit timeUnit) {
            return waitForCompletion(nodeId, requestId, maxWait, timeUnit, null);
        }

        /**
         * Wait for async task at (remote) nodeId to finish completion, using
         * exponential backoff to poll the task completion status. Effectively
         * waits forever.
         * <p>
         * 
         * <i>Logs the status at each status check if debug is enabled.</i>
         * 
         * @param nodeId Id of the node to poll
         * @param requestId Id of the request to check
         * @return description The final description attached with the response
         * @throws VoldemortException if task failed to finish in specified
         *         maxWait time.
         */
        public String waitForCompletion(int nodeId, int requestId) {
            return waitForCompletion(nodeId, requestId, 0, TimeUnit.SECONDS, null);
        }

        /**
         * Wait till the passed value matches with the metadata value returned
         * by the remote node for the passed key.
         * <p>
         * 
         * <i>Logs the status at each status check if debug is enabled.</i>
         * 
         * @param nodeId Id of the node to poll
         * @param key metadata key to keep checking for current value
         * @param value metadata value should match for exit criteria.
         * @param maxWait Maximum time we'll keep checking a request until we
         *        give up. Pass in 0 or less to wait "forever".
         * @param timeUnit Unit in which maxWait is expressed.
         */
        public void waitForCompletion(int nodeId,
                                      String key,
                                      String value,
                                      long maxWait,
                                      TimeUnit timeUnit) {
            long delay = INITIAL_DELAY;
            long waitUntil = Long.MAX_VALUE;
            if(maxWait > 0) {
                waitUntil = System.currentTimeMillis() + timeUnit.toMillis(maxWait);
            }

            while(System.currentTimeMillis() < waitUntil) {
                String currentValue = metadataMgmtOps.getRemoteMetadata(nodeId, key).getValue();
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
            throw new VoldemortException("Failed to get matching value " + value + " for key "
                                         + key + " at remote node " + nodeId + " in maximum wait"
                                         + maxWait + " " + timeUnit.toString() + " time.");
        }

        /**
         * Wait till the passed value matches with the metadata value returned
         * by the remote node for the passed key. Effectively waits forever.
         * <p>
         * 
         * <i>Logs the status at each status check if debug is enabled.</i>
         * 
         * @param nodeId Id of the node to poll
         * @param key metadata key to keep checking for current value
         * @param value metadata value should match for exit criteria.
         */
        public void waitForCompletion(int nodeId, String key, String value) {
            waitForCompletion(nodeId, key, value, 0, TimeUnit.SECONDS);
        }

    }

    /**
     * Encapsulates all operations that deal with cluster.xml and stores.xml
     * 
     */
    public class MetadataManagementOperations {

        /**
         * Update the metadata version for the given key (cluster or store). The
         * new value set is the current timestamp.
         * 
         * @param versionKey The metadata key for which Version should be
         *        incremented
         */
        public void updateMetadataversion(String versionKey) {
            updateMetadataversion(Arrays.asList(new String[] { versionKey }));
        }

        /**
         * Update the metadata versions for the given keys (cluster or store).
         * The new value set is the current timestamp.
         * 
         * @param versionKeys The metadata keys for which Version should be
         *        incremented
         */
        public void updateMetadataversion(Collection<String> versionKeys) {
            Properties props = MetadataVersionStoreUtils.getProperties(AdminClient.this.metadataVersionSysStoreClient);
            for(String versionKey: versionKeys) {
                long newValue = 0;
                if(props != null && props.getProperty(versionKey) != null) {
                    logger.debug("Version obtained = " + props.getProperty(versionKey));
                    newValue = System.currentTimeMillis();
                } else {
                    logger.debug("Current version is null. Assuming version 0.");
                    if(props == null) {
                        props = new Properties();
                    }
                }
                props.setProperty(versionKey, Long.toString(newValue));
            }
            MetadataVersionStoreUtils.setProperties(AdminClient.this.metadataVersionSysStoreClient,
                                                    props);
        }

        /**
         * Set the metadata versions to the given set
         * 
         * @param newProperties The new metadata versions to be set across all
         *        the nodes in the cluster
         */
        public void setMetadataversion(Properties newProperties) {
            MetadataVersionStoreUtils.setProperties(AdminClient.this.metadataVersionSysStoreClient,
                                                    newProperties);
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
         * See {@link voldemort.store.metadata.MetadataStore} for more
         * information.
         * 
         * @param remoteNodeId Id of the node
         * @param key Metadata key to update
         * @param value Value for the metadata key
         */

        public void updateRemoteMetadata(int remoteNodeId, String key, Versioned<String> value) {

            if(key.equals(STORES_VERSION_KEY)) {
                List<StoreDefinition> storeDefs = storeMapper.readStoreList(new StringReader(value.getValue()));
                // Check for backwards compatibility
                StoreDefinitionUtils.validateSchemasAsNeeded(storeDefs);
            }

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
            VAdminProto.UpdateMetadataResponse.Builder response = rpcOps.sendAndReceive(remoteNodeId,
                                                                                        request,
                                                                                        VAdminProto.UpdateMetadataResponse.newBuilder());
            if(response.hasError())
                helperOps.throwException(response.getError());
        }

        /**
         * Wrapper for updateRemoteMetadata function used against a single Node
         * It basically loops over the entire list of Nodes that we need to
         * execute the required operation against. It also increments the
         * version of the corresponding metadata in the system store.
         * <p>
         * 
         * Metadata keys can be one of {@link MetadataStore#METADATA_KEYS}<br>
         * eg.<br>
         * <li>cluster metadata (cluster.xml as string)
         * <li>stores definitions (stores.xml as string)
         * <li>Server states <br <br>
         * See {@link voldemort.store.metadata.MetadataStore} for more
         * information.
         * 
         * @param remoteNodeIds Ids of the nodes
         * @param key Metadata key to update
         * @param value Value for the metadata key
         * 
         * */
        public void updateRemoteMetadata(List<Integer> remoteNodeIds,
                                         String key,
                                         Versioned<String> value) {
            /*
             * Assume everything will be fine, increment the metadata version
             * for the key Would not harm even if the operation fails
             */
            if(key.equals(CLUSTER_VERSION_KEY) || key.equals(STORES_VERSION_KEY)) {
                metadataMgmtOps.updateMetadataversion(key);
            }
            for(Integer currentNodeId: remoteNodeIds) {
                logger.info("Setting " + key + " for "
                            + getAdminClientCluster().getNodeById(currentNodeId).getHost() + ":"
                            + getAdminClientCluster().getNodeById(currentNodeId).getId());
                updateRemoteMetadata(currentNodeId, key, value);
            }
        }

        /**
         * Update metadata pair <cluster,stores> at the given remoteNodeId.
         * 
         * @param remoteNodeId Id of the node
         * @param clusterKey cluster key to update
         * @param clusterValue value of the cluster metadata key
         * @param storesKey stores key to update
         * @param storesValue value of the stores metadata key
         * 
         */
        public void updateRemoteMetadataPair(int remoteNodeId,
                                             String clusterKey,
                                             Versioned<String> clusterValue,
                                             String storesKey,
                                             Versioned<String> storesValue) {
            ByteArray clusterKeyBytes = new ByteArray(ByteUtils.getBytes(clusterKey, "UTF-8"));
            Versioned<byte[]> clusterValueBytes = new Versioned<byte[]>(ByteUtils.getBytes(clusterValue.getValue(),
                                                                                           "UTF-8"),
                                                                        clusterValue.getVersion());

            List<StoreDefinition> storeDefs = storeMapper.readStoreList(new StringReader(storesValue.getValue()));
            // Check for backwards compatibility
            StoreDefinitionUtils.validateSchemasAsNeeded(storeDefs);

            ByteArray storesKeyBytes = new ByteArray(ByteUtils.getBytes(storesKey, "UTF-8"));
            Versioned<byte[]> storesValueBytes = new Versioned<byte[]>(ByteUtils.getBytes(storesValue.getValue(),
                                                                                          "UTF-8"),
                                                                       storesValue.getVersion());

            VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                         .setType(VAdminProto.AdminRequestType.UPDATE_METADATA_PAIR)
                                                                                         .setUpdateMetadataPair(VAdminProto.UpdateMetadataPairRequest.newBuilder()
                                                                                                                                                     .setClusterKey(ByteString.copyFrom(clusterKeyBytes.get()))
                                                                                                                                                     .setClusterValue(ProtoUtils.encodeVersioned(clusterValueBytes))
                                                                                                                                                     .setStoresKey(ByteString.copyFrom(storesKeyBytes.get()))
                                                                                                                                                     .setStoresValue((ProtoUtils.encodeVersioned(storesValueBytes)))
                                                                                                                                                     .build())
                                                                                         .build();
            VAdminProto.UpdateMetadataPairResponse.Builder response = rpcOps.sendAndReceive(remoteNodeId,
                                                                                            request,
                                                                                            VAdminProto.UpdateMetadataPairResponse.newBuilder());
            if(response.hasError())
                helperOps.throwException(response.getError());
        }

        /**
         * Wrapper for updateRemoteMetadataPair function used against a single
         * Node It basically loops over the entire list of Nodes that we need to
         * execute the required operation against. It also increments the
         * version of the corresponding metadata in the system store.
         * 
         * @param remoteNodeIds Ids of the nodes
         * @param clusterKey cluster key to update
         * @param clusterValue value of the cluster metadata key
         * @param storesKey stores key to update
         * @param storesValue value of the stores metadata key
         * 
         * */
        public void updateRemoteMetadataPair(List<Integer> remoteNodeIds,
                                             String clusterKey,
                                             Versioned<String> clusterValue,
                                             String storesKey,
                                             Versioned<String> storesValue) {

            /*
             * We first increment the metadata version for the cluster and the
             * stores which does not harm even if the operation fail
             */
            if(clusterKey.equals(CLUSTER_VERSION_KEY)) {
                metadataMgmtOps.updateMetadataversion(clusterKey);
            }
            if(storesKey.equals(STORES_VERSION_KEY)) {
                StoreDefinitionsMapper storeDefsMapper = new StoreDefinitionsMapper();
                List<StoreDefinition> storeDefs = storeDefsMapper.readStoreList(new StringReader(storesValue.getValue()));
                if(storeDefs != null) {
                    try {
                        for(StoreDefinition storeDef: storeDefs) {
                            logger.info("Updating metadata version for stores: "
                                        + storeDef.getName());
                            metadataMgmtOps.updateMetadataversion(storeDef.getName());
                        }
                    } catch(Exception e) {
                        System.err.println("Error while updating metadata version for the specified store.");
                    }
                }
            }
            for(Integer currentNodeId: remoteNodeIds) {
                logger.info("Setting " + clusterKey + " and " + storesKey + " for "
                            + getAdminClientCluster().getNodeById(currentNodeId).getHost() + ":"
                            + getAdminClientCluster().getNodeById(currentNodeId).getId());
                updateRemoteMetadataPair(currentNodeId,
                                         clusterKey,
                                         clusterValue,
                                         storesKey,
                                         storesValue);
            }
        }

        /**
         * Helper method to fetch the current stores xml list and update the
         * specified stores
         * 
         * @param nodeId ID of the node for which the stores list has to be
         *        updated
         * @param updatedStores New version of the stores to be updated
         */
        public synchronized void fetchAndUpdateRemoteStore(int nodeId,
                                                           List<StoreDefinition> updatedStores) {

            // Check for backwards compatibility
            StoreDefinitionUtils.validateSchemasAsNeeded(updatedStores);

            Map<String, StoreDefinition> updatedStoresMap = new HashMap<String, StoreDefinition>();

            // Fetch the original store definition list
            Versioned<List<StoreDefinition>> originalStoreDefinitions = getRemoteStoreDefList(nodeId);
            if(originalStoreDefinitions == null) {
                throw new VoldemortException("No stores found at this node ID : " + nodeId);
            }

            List<StoreDefinition> originalstoreDefList = originalStoreDefinitions.getValue();
            List<StoreDefinition> finalStoreDefList = new ArrayList<StoreDefinition>();
            VectorClock oldClock = (VectorClock) originalStoreDefinitions.getVersion();

            // Build a map of store name to the new store definitions
            for(StoreDefinition def: updatedStores) {
                updatedStoresMap.put(def.getName(), def);
            }

            // Iterate through the original store definitions. Replace the old
            // ones with the ones specified in 'updatedStores'
            for(StoreDefinition def: originalstoreDefList) {
                StoreDefinition updatedDef = updatedStoresMap.get(def.getName());
                if(updatedDef == null) {
                    finalStoreDefList.add(def);
                } else {
                    finalStoreDefList.add(updatedDef);
                }
            }

            // Set the new store definition on the given nodeId
            updateRemoteMetadata(nodeId,
                                 MetadataStore.STORES_KEY,
                                 new Versioned<String>(storeMapper.writeStoreList(finalStoreDefList),
                                                       oldClock.incremented(nodeId, 1)));

        }

        /**
         * Helper method to fetch the current stores xml list and update the
         * specified stores. This is done for all the nodes in the current
         * cluster.
         * 
         * @param updatedStores New version of the stores to be updated
         */
        public synchronized void fetchAndUpdateRemoteStores(List<StoreDefinition> updatedStores) {
            for(Integer nodeId: currentCluster.getNodeIds()) {
                fetchAndUpdateRemoteStore(nodeId, updatedStores);
            }
        }

        /**
         * Get the metadata on a remote node.
         * <p>
         * Metadata keys can be one of {@link MetadataStore#METADATA_KEYS}<br>
         * eg.<br>
         * <li>cluster metadata (cluster.xml as string)
         * <li>stores definitions (stores.xml as string)
         * <li>Server states <br <br>
         * See {@link voldemort.store.metadata.MetadataStore} for more
         * information.
         * 
         * @param remoteNodeId Id of the node
         * @param key Metadata key to update
         * @return Metadata with its associated
         *         {@link voldemort.versioning.Version}
         */
        public Versioned<String> getRemoteMetadata(int remoteNodeId, String key) {
            ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(key, "UTF-8"));
            VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                         .setType(VAdminProto.AdminRequestType.GET_METADATA)
                                                                                         .setGetMetadata(VAdminProto.GetMetadataRequest.newBuilder()
                                                                                                                                       .setKey(ByteString.copyFrom(keyBytes.get())))
                                                                                         .build();
            VAdminProto.GetMetadataResponse.Builder response = rpcOps.sendAndReceive(remoteNodeId,
                                                                                     request,
                                                                                     VAdminProto.GetMetadataResponse.newBuilder());

            if(response.hasError())
                helperOps.throwException(response.getError());

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
         * @return A cluster object with its
         *         {@link voldemort.versioning.Version}
         * @throws VoldemortException
         */
        public Versioned<Cluster> getRemoteCluster(int nodeId) throws VoldemortException {
            Versioned<String> value = metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                        MetadataStore.CLUSTER_KEY);
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
            // Check for backwards compatibility
            StoreDefinitionUtils.validateSchemasAsNeeded(storesList);

            // get current version.
            VectorClock oldClock = (VectorClock) metadataMgmtOps.getRemoteStoreDefList(nodeId)
                                                                .getVersion();

            Versioned<String> value = new Versioned<String>(storeMapper.writeStoreList(storesList),
                                                            oldClock.incremented(nodeId, 1));

            ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(MetadataStore.STORES_KEY, "UTF-8"));
            Versioned<byte[]> valueBytes = new Versioned<byte[]>(ByteUtils.getBytes(value.getValue(),
                                                                                    "UTF-8"),
                                                                 value.getVersion());

            VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                         .setType(VAdminProto.AdminRequestType.UPDATE_STORE_DEFINITIONS)
                                                                                         .setUpdateMetadata(VAdminProto.UpdateMetadataRequest.newBuilder()
                                                                                                                                             .setKey(ByteString.copyFrom(keyBytes.get()))
                                                                                                                                             .setVersioned(ProtoUtils.encodeVersioned(valueBytes))
                                                                                                                                             .build())
                                                                                         .build();
            VAdminProto.UpdateMetadataResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                        request,
                                                                                        VAdminProto.UpdateMetadataResponse.newBuilder());
            if(response.hasError())
                helperOps.throwException(response.getError());
        }

        /**
         * Wrapper for updateRemoteStoreDefList : update this for all the nodes
         * <p>
         * 
         * @param storesList The new store list
         * @throws VoldemortException
         */
        public void updateRemoteStoreDefList(List<StoreDefinition> storesList)
                throws VoldemortException {
            for(Node node: currentCluster.getNodes()) {
                logger.info("Updating stores.xml for " + node.getHost() + ":" + node.getId());

                updateRemoteStoreDefList(node.getId(), storesList);
            }
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
            Versioned<String> value = metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                        MetadataStore.STORES_KEY);
            List<StoreDefinition> storeList = storeMapper.readStoreList(new StringReader(value.getValue()),
                                                                        false);
            return new Versioned<List<StoreDefinition>>(storeList, value.getVersion());
        }
    }

    /**
     * Encapsulates all operations related to store management (addition,
     * deletion)
     * 
     */
    public class StoreManagementOperations {

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
            // Check for backwards compatibility
            StoreDefinitionUtils.validateSchemasAsNeeded(Arrays.asList(def));

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
            VAdminProto.AddStoreResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                  request,
                                                                                  VAdminProto.AddStoreResponse.newBuilder());
            if(response.hasError())
                helperOps.throwException(response.getError());
            logger.info("Succesfully added " + def.getName() + " on node " + node.getHost() + ":"
                        + node.getId());
        }

        public void addStore(StoreDefinition def, Collection<Integer> nodeIds) {
            // Check for backwards compatibility
            StoreDefinitionUtils.validateSchemasAsNeeded(Arrays.asList(def));

            String value = storeMapper.writeStore(def);
            VAdminProto.AddStoreRequest.Builder addStoreRequest = VAdminProto.AddStoreRequest.newBuilder()
                                                                                             .setStoreDefinition(value);
            VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                         .setType(VAdminProto.AdminRequestType.ADD_STORE)
                                                                                         .setAddStore(addStoreRequest)
                                                                                         .build();
            for(Integer nodeId: nodeIds) {
                Node node = currentCluster.getNodeById(nodeId);
                if(null == node) {
                    throw new VoldemortException("Invalid node id (" + nodeId + ") specified");
                }

                logger.info("Adding store " + def.getName() + " on node " + node.getHost() + ":"
                            + nodeId);
                VAdminProto.AddStoreResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                      request,
                                                                                      VAdminProto.AddStoreResponse.newBuilder());
                if(response.hasError()) {
                    helperOps.throwException(response.getError());
                }
                logger.info("Succesfully added " + def.getName() + " on node " + node.getHost()
                            + ":" + nodeId);
            }
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
            VAdminProto.DeleteStoreResponse.Builder response = rpcOps.sendAndReceive(node.getId(),
                                                                                     request,
                                                                                     VAdminProto.DeleteStoreResponse.newBuilder());
            if(response.hasError())
                helperOps.throwException(response.getError());
            logger.info("Successfully deleted " + storeName + " on node " + node.getHost() + ":"
                        + node.getId());
        }

        public void deleteStore(String storeName, List<Integer> nodeIds) {
            VAdminProto.DeleteStoreRequest.Builder deleteStoreRequest = VAdminProto.DeleteStoreRequest.newBuilder()
                                                                                                      .setStoreName(storeName);
            VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                         .setType(VAdminProto.AdminRequestType.DELETE_STORE)
                                                                                         .setDeleteStore(deleteStoreRequest)
                                                                                         .build();
            for(Integer nodeId: nodeIds) {
                Node node = currentCluster.getNodeById(nodeId);
                if(node == null) {
                    throw new VoldemortException("Invalid node id (" + nodeId + ") specified");
                }

                logger.info("Deleting " + storeName + " on node " + node.getHost() + ":" + nodeId);
                VAdminProto.DeleteStoreResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                         request,
                                                                                         VAdminProto.DeleteStoreResponse.newBuilder());
                if(response.hasError()) {
                    helperOps.throwException(response.getError());
                }
                logger.info("Successfully deleted " + storeName + " on node " + node.getHost()
                            + ":" + nodeId);
            }
        }
    }

    /**
     * Encapsulates all operations that aid in performing maintenance on the
     * actual store's data
     * 
     */
    public class StoreMaintenanceOperations {

        /**
         * Migrate keys/values belonging to stealPartitionList ( can be primary
         * or replica ) from donor node to stealer node. <b>Does not delete the
         * partitions from donorNode, merely copies them. </b>
         * <p>
         * See
         * {@link #migratePartitions(int, int, String, List, VoldemortFilter, Cluster)}
         * for more details.
         * 
         * 
         * @param donorNodeId Node <em>from</em> which the partitions are to be
         *        streamed.
         * @param stealerNodeId Node <em>to</em> which the partitions are to be
         *        streamed.
         * @param storeName Name of the store to stream.
         * @param stealPartitionList List of partitions to stream.
         * @param filter Custom filter implementation to filter out entries
         *        which should not be deleted.
         * @return The value of the
         *         {@link voldemort.server.protocol.admin.AsyncOperation}
         *         created on stealerNodeId which is performing the operation.
         */
        public int migratePartitions(int donorNodeId,
                                     int stealerNodeId,
                                     String storeName,
                                     List<Integer> stealPartitionList,
                                     VoldemortFilter filter) {
            return migratePartitions(donorNodeId,
                                     stealerNodeId,
                                     storeName,
                                     stealPartitionList,
                                     filter,
                                     null);
        }

        /**
         * Migrate keys/values belonging to a list of partition ids from donor
         * node to stealer node. <b>Does not delete the partitions from
         * donorNode, merely copies them. </b>
         * <p>
         * This is a background operation (see
         * {@link voldemort.server.protocol.admin.AsyncOperation} that runs on
         * the stealer node where updates are performed.
         * <p>
         * 
         * @param donorNodeId Node <em>from</em> which the partitions are to be
         *        streamed.
         * @param stealerNodeId Node <em>to</em> which the partitions are to be
         *        streamed.
         * @param storeName Name of the store to stream.
         * @param partitionIds List of partition ids
         * @param filter Voldemort post-filter
         * @param initialCluster The cluster metadata to use for making the
         *        decision if the key belongs to these partitions. If not
         *        specified, falls back to the metadata stored on the box
         * @return The value of the
         *         {@link voldemort.server.protocol.admin.AsyncOperation}
         *         created on stealer node which is performing the operation.
         */
        public int migratePartitions(int donorNodeId,
                                     int stealerNodeId,
                                     String storeName,
                                     List<Integer> partitionIds,
                                     VoldemortFilter filter,
                                     Cluster initialCluster) {
            VAdminProto.InitiateFetchAndUpdateRequest.Builder initiateFetchAndUpdateRequest = VAdminProto.InitiateFetchAndUpdateRequest.newBuilder()
                                                                                                                                       .setNodeId(donorNodeId)
                                                                                                                                       .addAllPartitionIds(partitionIds)
                                                                                                                                       .setStore(storeName);

            try {
                if(filter != null) {
                    initiateFetchAndUpdateRequest.setFilter(helperOps.encodeFilter(filter));
                }
            } catch(IOException e) {
                throw new VoldemortException(e);
            }

            if(initialCluster != null) {
                initiateFetchAndUpdateRequest.setInitialCluster(new ClusterMapper().writeCluster(initialCluster));
            }

            VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                              .setInitiateFetchAndUpdate(initiateFetchAndUpdateRequest)
                                                                                              .setType(VAdminProto.AdminRequestType.INITIATE_FETCH_AND_UPDATE)
                                                                                              .build();
            VAdminProto.AsyncOperationStatusResponse.Builder response = rpcOps.sendAndReceive(stealerNodeId,
                                                                                              adminRequest,
                                                                                              VAdminProto.AsyncOperationStatusResponse.newBuilder());

            if(response.hasError()) {
                helperOps.throwException(response.getError());
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
            VAdminProto.TruncateEntriesResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                         request,
                                                                                         VAdminProto.TruncateEntriesResponse.newBuilder());

            if(response.hasError()) {
                helperOps.throwException(response.getError());
            }
        }

        public void truncate(List<Integer> nodeIds, String storeName) {
            VAdminProto.TruncateEntriesRequest.Builder truncateRequest = VAdminProto.TruncateEntriesRequest.newBuilder()
                                                                                                           .setStore(storeName);

            VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                         .setType(VAdminProto.AdminRequestType.TRUNCATE_ENTRIES)
                                                                                         .setTruncateEntries(truncateRequest)
                                                                                         .build();
            for(Integer nodeId: nodeIds) {
                VAdminProto.TruncateEntriesResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                             request,
                                                                                             VAdminProto.TruncateEntriesResponse.newBuilder());

                if(response.hasError()) {
                    helperOps.throwException(response.getError());
                }
            }
        }

        /**
         * Delete all entries belonging to a list of partitions
         * 
         * @param nodeId Node on which the entries to be deleted
         * @param storeName Name of the store holding the entries
         * @param partitionList List of partitions to delete.
         * @param filter Custom filter implementation to filter out entries
         *        which should not be deleted.
         * @return Number of entries deleted
         */
        public long deletePartitions(int nodeId,
                                     String storeName,
                                     List<Integer> partitionList,
                                     VoldemortFilter filter) {
            return deletePartitions(nodeId, storeName, partitionList, null, filter);
        }

        /**
         * Delete all entries belonging to all the partitions passed as a map of
         * replica_type to partition list. Works only for RW stores.
         * 
         * @param nodeId Node on which the entries to be deleted
         * @param storeName Name of the store holding the entries
         * @param partitionIds List of partition Ids
         * @param filter Custom filter implementation to filter out entries
         *        which should not be deleted.
         * @return Number of entries deleted
         */
        public long deletePartitions(int nodeId,
                                     String storeName,
                                     List<Integer> partitionIds,
                                     Cluster initialCluster,
                                     VoldemortFilter filter) {
            VAdminProto.DeletePartitionEntriesRequest.Builder deleteRequest = VAdminProto.DeletePartitionEntriesRequest.newBuilder()
                                                                                                                       .addAllPartitionIds(partitionIds)
                                                                                                                       .setStore(storeName);

            try {
                if(filter != null) {
                    deleteRequest.setFilter(helperOps.encodeFilter(filter));
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
            VAdminProto.DeletePartitionEntriesResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                                request,
                                                                                                VAdminProto.DeletePartitionEntriesResponse.newBuilder());

            if(response.hasError())
                helperOps.throwException(response.getError());

            return response.getCount();
        }

        /**
         * See {@link RepairJob}
         * 
         * @param nodeId The id of the node on which to do the repair
         */
        public void repairJob(int nodeId) {
            VAdminProto.RepairJobRequest.Builder repairJobRequest = VAdminProto.RepairJobRequest.newBuilder();

            VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                              .setRepairJob(repairJobRequest)
                                                                                              .setType(VAdminProto.AdminRequestType.REPAIR_JOB)
                                                                                              .build();
            Node node = AdminClient.this.getAdminClientCluster().getNodeById(nodeId);
            SocketDestination destination = new SocketDestination(node.getHost(),
                                                                  node.getAdminPort(),
                                                                  RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
            SocketAndStreams sands = socketPool.checkout(destination);

            try {
                DataOutputStream outputStream = sands.getOutputStream();
                ProtoUtils.writeMessage(outputStream, adminRequest);
                outputStream.flush();
            } catch(IOException e) {
                helperOps.close(sands.getSocket());
                throw new VoldemortException(e);
            } finally {
                socketPool.checkin(destination, sands);
            }
            return;
        }

        /**
         * See {@link VersionedPutPruneJob}
         * 
         * @param nodeId server on which to prune
         * @param store store to prune
         */
        public void pruneJob(int nodeId, String store) {
            logger.info("Kicking off prune job on Node " + nodeId + " for store " + store);
            VAdminProto.PruneJobRequest.Builder jobRequest = VAdminProto.PruneJobRequest.newBuilder()
                                                                                        .setStoreName(store);

            VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                              .setPruneJob(jobRequest)
                                                                                              .setType(VAdminProto.AdminRequestType.PRUNE_JOB)
                                                                                              .build();
            // TODO probably need a helper to do all this, at some point.. all
            // of this file has repeated code
            Node node = AdminClient.this.getAdminClientCluster().getNodeById(nodeId);
            SocketDestination destination = new SocketDestination(node.getHost(),
                                                                  node.getAdminPort(),
                                                                  RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
            SocketAndStreams sands = socketPool.checkout(destination);

            try {
                DataOutputStream outputStream = sands.getOutputStream();
                ProtoUtils.writeMessage(outputStream, adminRequest);
                outputStream.flush();
            } catch(IOException e) {
                helperOps.close(sands.getSocket());
                throw new VoldemortException(e);
            } finally {
                socketPool.checkin(destination, sands);
            }
        }

        /**
         * See {@link VersionedPutPruneJob}
         * 
         * 
         * @param nodeId The id of the node on which to do the pruning
         * @param stores the list of stores to prune
         */
        public void pruneJob(int nodeId, List<String> stores) {
            for(String store: stores) {
                pruneJob(nodeId, store);
            }
        }

        public void slopPurgeJob(int destinationNodeId,
                                 List<Integer> nodeList,
                                 int zoneId,
                                 List<String> storeNames) {
            VAdminProto.SlopPurgeJobRequest.Builder jobRequest = VAdminProto.SlopPurgeJobRequest.newBuilder();
            if(nodeList != null) {
                jobRequest.addAllFilterNodeIds(nodeList);
            }
            if(zoneId != Zone.UNSET_ZONE_ID) {
                jobRequest.setFilterZoneId(zoneId);
            }
            if(storeNames != null) {
                jobRequest.addAllFilterStoreNames(storeNames);
            }

            VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                              .setSlopPurgeJob(jobRequest)
                                                                                              .setType(VAdminProto.AdminRequestType.SLOP_PURGE_JOB)
                                                                                              .build();
            helperOps.sendAdminRequest(adminRequest, destinationNodeId);
        }

        public void slopPurgeJob(List<Integer> nodesToPurge,
                                 int zoneToPurge,
                                 List<String> storesToPurge) {
            // Run this on all the nodes in the cluster
            for(Node node: currentCluster.getNodes()) {
                logger.info("Submitting SlopPurgeJob on node " + node.getId());
                slopPurgeJob(node.getId(), nodesToPurge, zoneToPurge, storesToPurge);
            }
        }

        /**
         * Native backup a store
         * 
         * @param nodeId The node id to backup
         * @param storeName The name of the store to backup
         * @param destinationDirPath The destination path
         * @param timeOut minutes to wait for operation to complete
         * @param verify should the file checksums be verified
         * @param isIncremental is the backup incremental
         */
        public void nativeBackup(int nodeId,
                                 String storeName,
                                 String destinationDirPath,
                                 int timeOut,
                                 boolean verify,
                                 boolean isIncremental) {

            VAdminProto.NativeBackupRequest nativeBackupRequest = VAdminProto.NativeBackupRequest.newBuilder()
                                                                                                 .setStoreName(storeName)
                                                                                                 .setBackupDir(destinationDirPath)
                                                                                                 .setIncremental(isIncremental)
                                                                                                 .setVerifyFiles(verify)
                                                                                                 .build();
            VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                              .setNativeBackup(nativeBackupRequest)
                                                                                              .setType(VAdminProto.AdminRequestType.NATIVE_BACKUP)
                                                                                              .build();
            VAdminProto.AsyncOperationStatusResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                              adminRequest,
                                                                                              VAdminProto.AsyncOperationStatusResponse.newBuilder());

            if(response.hasError()) {
                helperOps.throwException(response.getError());
            }

            int asyncId = response.getRequestId();
            rpcOps.waitForCompletion(nodeId, asyncId, timeOut, TimeUnit.MINUTES);
        }
    }

    /**
     * Encapsulates all the operations to forklift data from the cluster
     * 
     */
    public class BulkStreamingFetchOperations {

        private void initiateFetchRequest(DataOutputStream outputStream,
                                          String storeName,
                                          List<Integer> partitionIds,
                                          VoldemortFilter filter,
                                          boolean fetchValues,
                                          boolean fetchMasterEntries,
                                          Cluster initialCluster,
                                          long recordsPerPartition) throws IOException {
            VAdminProto.FetchPartitionEntriesRequest.Builder fetchRequest = VAdminProto.FetchPartitionEntriesRequest.newBuilder()
                                                                                                                    .setFetchValues(fetchValues)
                                                                                                                    .addAllPartitionIds(partitionIds)
                                                                                                                    .setStore(storeName)
                                                                                                                    .setRecordsPerPartition(recordsPerPartition);

            try {
                if(filter != null) {
                    fetchRequest.setFilter(helperOps.encodeFilter(filter));
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
         * Fetches entries that don't belong to the node, based on current
         * metadata and yet persisted on the node
         * 
         * @param nodeId Id of the node to fetch from
         * @param storeName Name of the store
         * @return An iterator which allows entries to be streamed as they're
         *         being iterated over.
         */
        public Iterator<Pair<ByteArray, Versioned<byte[]>>> fetchOrphanedEntries(int nodeId,
                                                                                 String storeName) {

            Node node = AdminClient.this.getAdminClientCluster().getNodeById(nodeId);
            final SocketDestination destination = new SocketDestination(node.getHost(),
                                                                        node.getAdminPort(),
                                                                        RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
            final SocketAndStreams sands = socketPool.checkout(destination);
            DataOutputStream outputStream = sands.getOutputStream();
            final DataInputStream inputStream = sands.getInputStream();

            try {
                VAdminProto.FetchPartitionEntriesRequest.Builder fetchOrphanedRequest = VAdminProto.FetchPartitionEntriesRequest.newBuilder()
                                                                                                                                .setFetchValues(true)
                                                                                                                                .setStore(storeName)
                                                                                                                                .setFetchOrphaned(true);

                VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                             .setType(VAdminProto.AdminRequestType.FETCH_PARTITION_ENTRIES)
                                                                                             .setFetchPartitionEntries(fetchOrphanedRequest)
                                                                                             .build();
                ProtoUtils.writeMessage(outputStream, request);
                outputStream.flush();
            } catch(IOException e) {
                helperOps.close(sands.getSocket());
                socketPool.checkin(destination, sands);
                throw new VoldemortException(e);
            }

            return new AbstractIterator<Pair<ByteArray, Versioned<byte[]>>>() {

                @Override
                public Pair<ByteArray, Versioned<byte[]>> computeNext() {
                    try {
                        int size = inputStream.readInt();
                        if(size == -1) {
                            socketPool.checkin(destination, sands);
                            return endOfData();
                        }

                        VAdminProto.FetchPartitionEntriesResponse response = responseFromStream(inputStream,
                                                                                                size);

                        if(response.hasError()) {
                            socketPool.checkin(destination, sands);
                            helperOps.throwException(response.getError());
                        }

                        VAdminProto.PartitionEntry partitionEntry = response.getPartitionEntry();

                        return Pair.create(ProtoUtils.decodeBytes(partitionEntry.getKey()),
                                           ProtoUtils.decodeVersioned(partitionEntry.getVersioned()));
                    } catch(IOException e) {
                        helperOps.close(sands.getSocket());
                        socketPool.checkin(destination, sands);
                        throw new VoldemortException(e);
                    }
                }
            };
        }

        /**
         * Legacy interface for fetching entries. See
         * {@link #fetchEntries(int, String, List, VoldemortFilter, boolean, Cluster, long)}
         * for more information.
         * 
         * @param nodeId Id of the node to fetch from
         * @param storeName Name of the store
         * @param partitionIds List of the partitions
         * @param filter Custom filter implementation to filter out entries
         *        which should not be fetched.
         * @param fetchMasterEntries Fetch an entry only if master replica
         * @return An iterator which allows entries to be streamed as they're
         *         being iterated over.
         */
        public Iterator<Pair<ByteArray, Versioned<byte[]>>> fetchEntries(int nodeId,
                                                                         String storeName,
                                                                         List<Integer> partitionIds,
                                                                         VoldemortFilter filter,
                                                                         boolean fetchMasterEntries,
                                                                         long recordsPerPartition) {
            return fetchEntries(nodeId,
                                storeName,
                                partitionIds,
                                filter,
                                fetchMasterEntries,
                                null,
                                recordsPerPartition);
        }

        /**
         * Legacy interface for fetching entries. See
         * {@link #fetchEntries(int, String, List, VoldemortFilter, boolean, Cluster, long)}
         * for more information.
         * 
         * @param nodeId Id of the node to fetch from
         * @param storeName Name of the store
         * @param partitionList List of the partitions
         * @param filter Custom filter implementation to filter out entries
         *        which should not be fetched.
         * @param fetchMasterEntries Fetch an entry only if master replica
         * @return An iterator which allows entries to be streamed as they're
         *         being iterated over.
         */
        public Iterator<Pair<ByteArray, Versioned<byte[]>>> fetchEntries(int nodeId,
                                                                         String storeName,
                                                                         List<Integer> partitionList,
                                                                         VoldemortFilter filter,
                                                                         boolean fetchMasterEntries) {
            return fetchEntries(nodeId, storeName, partitionList, filter, fetchMasterEntries, 0);
        }

        // TODO: The use of "Pair" in the return for a fundamental type is
        // awkward. We should have a core KeyValue type that effectively wraps
        // up a ByteArray and a Versioned<byte[]>.
        /**
         * Fetch key/value tuples belonging to this list of partition ids
         * <p>
         * 
         * <b>Streaming API</b> - The server keeps sending the messages as it's
         * iterating over the data. Once iteration has finished, the server
         * sends an "end of stream" marker and flushes its buffer. A response
         * indicating a {@link VoldemortException} may be sent at any time
         * during the process. <br>
         * 
         * <p>
         * Entries are being streamed <em>as the iteration happens</em> i.e. the
         * whole result set is <b>not</b> buffered in memory.
         * 
         * @param nodeId Id of the node to fetch from
         * @param storeName Name of the store
         * @param partitionIds List of partition ids
         * @param filter Custom filter implementation to filter out entries
         *        which should not be fetched.
         * @param fetchMasterEntries Fetch an entry only if master replica
         * @param initialCluster The cluster metadata to use while making the
         *        decision to fetch entries. This is important during
         *        rebalancing where-in we want to fetch keys using an older
         *        metadata compared to the new one.
         * @return An iterator which allows entries to be streamed as they're
         *         being iterated over.
         */
        public Iterator<Pair<ByteArray, Versioned<byte[]>>> fetchEntries(int nodeId,
                                                                         String storeName,
                                                                         List<Integer> partitionIds,
                                                                         VoldemortFilter filter,
                                                                         boolean fetchMasterEntries,
                                                                         Cluster initialCluster,
                                                                         long recordsPerPartition) {

            Node node = AdminClient.this.getAdminClientCluster().getNodeById(nodeId);
            final SocketDestination destination = new SocketDestination(node.getHost(),
                                                                        node.getAdminPort(),
                                                                        RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
            final SocketAndStreams sands = socketPool.checkout(destination);
            DataOutputStream outputStream = sands.getOutputStream();
            final DataInputStream inputStream = sands.getInputStream();

            try {
                initiateFetchRequest(outputStream,
                                     storeName,
                                     partitionIds,
                                     filter,
                                     true,
                                     fetchMasterEntries,
                                     initialCluster,
                                     recordsPerPartition);
            } catch(IOException e) {
                helperOps.close(sands.getSocket());
                socketPool.checkin(destination, sands);
                throw new VoldemortException(e);
            }

            return new AbstractIterator<Pair<ByteArray, Versioned<byte[]>>>() {

                @Override
                public Pair<ByteArray, Versioned<byte[]>> computeNext() {
                    try {
                        int size = inputStream.readInt();
                        if(size == -1) {
                            socketPool.checkin(destination, sands);
                            return endOfData();
                        }

                        VAdminProto.FetchPartitionEntriesResponse response = responseFromStream(inputStream,
                                                                                                size);

                        if(response.hasError()) {
                            socketPool.checkin(destination, sands);
                            helperOps.throwException(response.getError());
                        }

                        VAdminProto.PartitionEntry partitionEntry = response.getPartitionEntry();

                        return Pair.create(ProtoUtils.decodeBytes(partitionEntry.getKey()),
                                           ProtoUtils.decodeVersioned(partitionEntry.getVersioned()));
                    } catch(IOException e) {
                        helperOps.close(sands.getSocket());
                        socketPool.checkin(destination, sands);
                        throw new VoldemortException(e);
                    }
                }
            };

        }

        /**
         * Fetch all the keys on the node that don't belong to it, based on its
         * current metadata and yet stored on the node. i.e all keys orphaned on
         * the node due to say not running the repair job after a rebalance
         * 
         * @param nodeId Id of the node to fetch from
         * @param storeName Name of the store
         * @return An iterator which allows keys to be streamed as they're being
         *         iterated over.
         */
        public Iterator<ByteArray> fetchOrphanedKeys(int nodeId, String storeName) {
            Node node = AdminClient.this.getAdminClientCluster().getNodeById(nodeId);
            final SocketDestination destination = new SocketDestination(node.getHost(),
                                                                        node.getAdminPort(),
                                                                        RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
            final SocketAndStreams sands = socketPool.checkout(destination);
            DataOutputStream outputStream = sands.getOutputStream();
            final DataInputStream inputStream = sands.getInputStream();

            try {
                VAdminProto.FetchPartitionEntriesRequest.Builder fetchOrphanedRequest = VAdminProto.FetchPartitionEntriesRequest.newBuilder()
                                                                                                                                .setFetchValues(false)
                                                                                                                                .setStore(storeName)
                                                                                                                                .setFetchOrphaned(true);

                VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                             .setType(VAdminProto.AdminRequestType.FETCH_PARTITION_ENTRIES)
                                                                                             .setFetchPartitionEntries(fetchOrphanedRequest)
                                                                                             .build();
                ProtoUtils.writeMessage(outputStream, request);
                outputStream.flush();
            } catch(IOException e) {
                helperOps.close(sands.getSocket());
                socketPool.checkin(destination, sands);
                throw new VoldemortException(e);
            }

            return new AbstractIterator<ByteArray>() {

                @Override
                public ByteArray computeNext() {
                    try {
                        int size = inputStream.readInt();
                        if(size == -1) {
                            socketPool.checkin(destination, sands);
                            return endOfData();
                        }

                        VAdminProto.FetchPartitionEntriesResponse response = responseFromStream(inputStream,
                                                                                                size);

                        if(response.hasError()) {
                            socketPool.checkin(destination, sands);
                            helperOps.throwException(response.getError());
                        }

                        return ProtoUtils.decodeBytes(response.getKey());
                    } catch(IOException e) {
                        helperOps.close(sands.getSocket());
                        socketPool.checkin(destination, sands);
                        throw new VoldemortException(e);
                    }

                }
            };
        }

        /**
         * Legacy interface for fetching entries. See
         * {@link #fetchKeys(int, String, List, VoldemortFilter, boolean, Cluster, long)}
         * for more information.
         * 
         * @param nodeId Id of the node to fetch from
         * @param storeName Name of the store
         * @param partitionIds List of the partitions to retrieve
         * @param filter Custom filter implementation to filter out entries
         *        which should not be fetched.
         * @param fetchMasterEntries Fetch a key only if master replica
         * @return An iterator which allows keys to be streamed as they're being
         *         iterated over.
         */
        public Iterator<ByteArray> fetchKeys(int nodeId,
                                             String storeName,
                                             List<Integer> partitionIds,
                                             VoldemortFilter filter,
                                             boolean fetchMasterEntries,
                                             long recordsPerPartition) {
            return fetchKeys(nodeId,
                             storeName,
                             partitionIds,
                             filter,
                             fetchMasterEntries,
                             null,
                             recordsPerPartition);
        }

        /**
         * Legacy interface for fetching entries. See
         * {@link #fetchKeys(int, String, List, VoldemortFilter, boolean, Cluster, long)}
         * for more information.
         * 
         * @param nodeId Id of the node to fetch from
         * @param storeName Name of the store
         * @param partitionList List of the partitions to retrieve
         * @param filter Custom filter implementation to filter out entries
         *        which should not be fetched.
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
         * Fetch all keys belonging to the list of partition ids. Identical to
         * {@link #fetchEntries} but <em>only fetches the keys</em>
         * 
         * @param nodeId The node id from where to fetch the keys
         * @param storeName The store name whose keys we want to retrieve
         * @param partitionIds List of partitionIds
         * @param filter Custom filter
         * @param initialCluster Cluster to use for selecting a key. If null,
         *        use the default metadata from the metadata store
         * @return Returns an iterator of the keys
         */
        public Iterator<ByteArray> fetchKeys(int nodeId,
                                             String storeName,
                                             List<Integer> partitionIds,
                                             VoldemortFilter filter,
                                             boolean fetchMasterEntries,
                                             Cluster initialCluster,
                                             long recordsPerPartition) {
            Node node = AdminClient.this.getAdminClientCluster().getNodeById(nodeId);
            final SocketDestination destination = new SocketDestination(node.getHost(),
                                                                        node.getAdminPort(),
                                                                        RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
            final SocketAndStreams sands = socketPool.checkout(destination);
            DataOutputStream outputStream = sands.getOutputStream();
            final DataInputStream inputStream = sands.getInputStream();

            try {
                initiateFetchRequest(outputStream,
                                     storeName,
                                     partitionIds,
                                     filter,
                                     false,
                                     fetchMasterEntries,
                                     initialCluster,
                                     recordsPerPartition);
            } catch(IOException e) {
                helperOps.close(sands.getSocket());
                socketPool.checkin(destination, sands);
                throw new VoldemortException(e);
            }

            return new AbstractIterator<ByteArray>() {

                @Override
                public ByteArray computeNext() {
                    try {
                        int size = inputStream.readInt();
                        if(size == -1) {
                            socketPool.checkin(destination, sands);
                            return endOfData();
                        }

                        VAdminProto.FetchPartitionEntriesResponse response = responseFromStream(inputStream,
                                                                                                size);

                        if(response.hasError()) {
                            socketPool.checkin(destination, sands);
                            helperOps.throwException(response.getError());
                        }

                        return ProtoUtils.decodeBytes(response.getKey());
                    } catch(IOException e) {
                        helperOps.close(sands.getSocket());
                        socketPool.checkin(destination, sands);
                        throw new VoldemortException(e);
                    }

                }
            };
        }
    }

    private class AdminStoreClient {

        private class NodeStore {

            final public Integer nodeId;
            final public String storeName;

            NodeStore(int nodeId, String storeName) {
                this.nodeId = new Integer(nodeId);
                this.storeName = storeName;
            }

            @Override
            public boolean equals(Object obj) {
                if(this == obj)
                    return true;
                if(!(obj instanceof NodeStore))
                    return false;
                NodeStore other = (NodeStore) obj;
                return nodeId.equals(other.nodeId) && storeName.equals(other.storeName);
            }

            @Override
            public int hashCode() {
                return nodeId.hashCode() + storeName.hashCode();
            }
        }

        final private ClientConfig clientConfig;
        final private ClientRequestExecutorPool clientPool;

        private final ConcurrentMap<NodeStore, SocketStore> nodeStoreSocketCache;

        AdminStoreClient(ClientConfig clientConfig) {
            this.clientConfig = clientConfig;
            clientPool = new ClientRequestExecutorPool(clientConfig.getSelectors(),
                                                       clientConfig.getMaxConnectionsPerNode(),
                                                       clientConfig.getConnectionTimeout(TimeUnit.MILLISECONDS),
                                                       clientConfig.getSocketTimeout(TimeUnit.MILLISECONDS),
                                                       clientConfig.getSocketBufferSize(),
                                                       clientConfig.getSocketKeepAlive());
            nodeStoreSocketCache = new ConcurrentHashMap<NodeStore, SocketStore>();
        }

        public SocketStore getSocketStore(int nodeId, String storeName) {
            NodeStore nodeStore = new NodeStore(nodeId, storeName);

            SocketStore socketStore = nodeStoreSocketCache.get(nodeStore);
            if(socketStore == null) {
                Node node = getAdminClientCluster().getNodeById(nodeId);

                SocketStore newSocketStore = null;
                try {
                    // Unless request format is protobuf, IGNORE_CHECKS
                    // will not work otherwise
                    newSocketStore = clientPool.create(storeName,
                                                       node.getHost(),
                                                       node.getSocketPort(),
                                                       clientConfig.getRequestFormatType(),
                                                       RequestRoutingType.IGNORE_CHECKS);
                } catch(Exception e) {
                    clientPool.close();
                    throw new VoldemortException(e);
                }

                socketStore = nodeStoreSocketCache.putIfAbsent(nodeStore, newSocketStore);
                if(socketStore == null) {
                    socketStore = newSocketStore;
                } else {
                    newSocketStore.close();
                }
            }

            return socketStore;
        }

        public void close() {
            clientPool.close();
        }
    }

    public class StoreOperations {

        /**
         * This method updates exactly one key/value for a specific store on a
         * specific node.
         * 
         * @param storeName Name of the store
         * @param nodeKeyValue A specific key/value to update on a specific
         *        node.
         */
        public void putNodeKeyValue(String storeName, NodeValue<ByteArray, byte[]> nodeKeyValue) {
            SocketStore socketStore = adminStoreClient.getSocketStore(nodeKeyValue.getNodeId(),
                                                                      storeName);

            socketStore.put(nodeKeyValue.getKey(), nodeKeyValue.getVersioned(), null);
        }

        /**
         * Fetch key/value tuple for given key for a specific store on specified
         * node.
         * 
         * @param storeName Name of the store
         * @param nodeId Id of the node to query from
         * @param key for which to query
         * @return List<Versioned<byte[]>> of values for the specified NodeKey.
         */
        public List<Versioned<byte[]>> getNodeKey(String storeName, int nodeId, ByteArray key) {
            SocketStore socketStore = adminStoreClient.getSocketStore(nodeId, storeName);
            return socketStore.get(key, null);
        }

        // As needed, add 'getall', 'delete', and so on interfaces...
    }

    /**
     * Encapsulates all steaming operations that actually read and write
     * key-value pairs into the cluster
     * 
     */
    public class StreamingOperations {

        /**
         * Update a stream of key/value entries at the given node. The iterator
         * entries are <em>streamed</em> from the client to the server:
         * <ol>
         * <li>Client performs a handshake with the server (sending in the
         * update entries request with a store name and a
         * {@link VoldemortFilter} instance.</li>
         * <li>While entryIterator has entries, the client will keep sending the
         * updates one after another to the server, buffering the data, without
         * waiting for a response from the server.</li>
         * <li>After iteration is complete, send an end of stream message, force
         * a flush of the buffer, check the response on the server to check if a
         * {@link VoldemortException} has occured.</li>
         * </ol>
         * 
         * @param nodeId Id of the remote node (where we wish to update the
         *        entries)
         * @param storeName Store name for the entries
         * @param entryIterator Iterator of key-value pairs for the entries
         * @param filter Custom filter implementation to filter out entries
         *        which should not be updated.
         * 
         * @throws VoldemortException
         */
        public void updateEntries(int nodeId,
                                  String storeName,
                                  Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator,
                                  VoldemortFilter filter) {
            streamingUpdateEntries(nodeId, storeName, entryIterator, filter, false);
        }

        /**
         * Update a stream of key/value entries at the given node in the same
         * way as
         * {@link StreamingOperations#updateEntries(int, String, Iterator, VoldemortFilter)}
         * 
         * The only difference being the resolving on the server will happen
         * based on timestamp and not the vector clock.
         * 
         * @param nodeId Id of the remote node (where we wish to update the
         *        entries)
         * @param storeName Store name for the entries
         * @param entryIterator Iterator of key-value pairs for the entries
         * @param filter Custom filter implementation to filter out entries
         *        which should not be updated.
         * 
         * @throws VoldemortException
         */
        public void updateEntriesTimeBased(int nodeId,
                                           String storeName,
                                           Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator,
                                           VoldemortFilter filter) {
            streamingUpdateEntries(nodeId, storeName, entryIterator, filter, true);
        }

        /**
         * Update a stream of key/value entries at the given node. The iterator
         * entries are <em>streamed</em> from the client to the server:
         * <ol>
         * <li>Client performs a handshake with the server (sending in the
         * update entries request with a store name and a
         * {@link VoldemortFilter} instance.</li>
         * <li>While entryIterator has entries, the client will keep sending the
         * updates one after another to the server, buffering the data, without
         * waiting for a response from the server.</li>
         * <li>After iteration is complete, send an end of stream message, force
         * a flush of the buffer, check the response on the server to check if a
         * {@link VoldemortException} has occured.</li>
         * </ol>
         * 
         * @param nodeId Id of the remote node (where we wish to update the
         *        entries)
         * @param storeName Store name for the entries
         * @param entryIterator Iterator of key-value pairs for the entries
         * @param filter Custom filter implementation to filter out entries
         *        which should not be updated.
         * @param overWriteIfLatestTs if true overwrite the existing value if
         *        the supplied version has greater timestamp; else use vector
         *        clocks
         * @throws VoldemortException
         */
        private void streamingUpdateEntries(int nodeId,
                                            String storeName,
                                            Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator,
                                            VoldemortFilter filter,
                                            boolean overWriteIfLatestTs) {
            Node node = AdminClient.this.getAdminClientCluster().getNodeById(nodeId);
            SocketDestination destination = new SocketDestination(node.getHost(),
                                                                  node.getAdminPort(),
                                                                  RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
            SocketAndStreams sands = socketPool.checkout(destination);
            DataOutputStream outputStream = sands.getOutputStream();
            DataInputStream inputStream = sands.getInputStream();
            boolean firstMessage = true;
            long printStatsTimer = System.currentTimeMillis() + PRINT_STATS_INTERVAL;
            long entryCount = 0;

            try {
                if(entryIterator.hasNext()) {
                    while(entryIterator.hasNext()) {
                        Pair<ByteArray, Versioned<byte[]>> entry = entryIterator.next();
                        VAdminProto.PartitionEntry partitionEntry = VAdminProto.PartitionEntry.newBuilder()
                                                                                              .setKey(ProtoUtils.encodeBytes(entry.getFirst()))
                                                                                              .setVersioned(ProtoUtils.encodeVersioned(entry.getSecond()))
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
                        entryCount++;
                        if(firstMessage) {
                            if(filter != null) {
                                updateRequest.setFilter(helperOps.encodeFilter(filter));
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
                            if(printStatsTimer <= System.currentTimeMillis()
                               || 0 == entryCount % PRINT_STATS_THRESHOLD) {
                                logger.info("UpdatePartitionEntries: fetched " + entryCount
                                            + " to node " + nodeId + " for store " + storeName);
                                printStatsTimer = System.currentTimeMillis() + PRINT_STATS_INTERVAL;
                            }
                        }
                    }
                    ProtoUtils.writeEndOfStream(outputStream);
                    outputStream.flush();
                    VAdminProto.UpdatePartitionEntriesResponse.Builder updateResponse = ProtoUtils.readToBuilder(inputStream,
                                                                                                                 VAdminProto.UpdatePartitionEntriesResponse.newBuilder());
                    if(updateResponse.hasError()) {
                        helperOps.throwException(updateResponse.getError());
                    }
                }
            } catch(IOException e) {
                helperOps.close(sands.getSocket());
                throw new VoldemortException(e);
            } finally {
                socketPool.checkin(destination, sands);
            }
        }

        /**
         * Fetch key/value tuples from a given server, directly from storage
         * engine
         * 
         * <p>
         * Entries are being queried synchronously
         * <em>as the iteration happens</em> i.e. the whole result set is
         * <b>not</b> buffered in memory.
         * 
         * @param nodeId Id of the node to fetch from
         * @param storeName Name of the store
         * @param keys An Iterable of keys
         * @return An iterator which allows entries to be streamed as they're
         *         being iterated over.
         */
        public Iterator<QueryKeyResult> queryKeys(int nodeId,
                                                  String storeName,
                                                  final Iterator<ByteArray> keys) {

            final Store<ByteArray, byte[], byte[]> store;

            try {
                store = adminStoreClient.getSocketStore(nodeId, storeName);
            } catch(Exception e) {
                throw new VoldemortException(e);
            }

            return new AbstractIterator<QueryKeyResult>() {

                @Override
                public QueryKeyResult computeNext() {
                    ByteArray key;
                    List<Versioned<byte[]>> value = null;
                    if(!keys.hasNext()) {
                        return endOfData();
                    } else {
                        key = keys.next();
                    }
                    try {
                        value = store.get(key, null);
                        return new QueryKeyResult(key, value);
                    } catch(Exception e) {
                        return new QueryKeyResult(key, e);
                    }
                }
            };
        }

        /**
         * Update slops which may be meant for multiple stores
         * 
         * @param nodeId The id of the node
         * @param entryIterator An iterator over all the slops for this
         *        particular node
         */
        public void updateSlopEntries(int nodeId, Iterator<Versioned<Slop>> entryIterator) {
            Node node = AdminClient.this.getAdminClientCluster().getNodeById(nodeId);
            SocketDestination destination = new SocketDestination(node.getHost(),
                                                                  node.getAdminPort(),
                                                                  RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
            SocketAndStreams sands = socketPool.checkout(destination);
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
                        helperOps.throwException(updateResponse.getError());
                    }
                }
            } catch(IOException e) {
                helperOps.close(sands.getSocket());
                throw new VoldemortException(e);
            } finally {
                socketPool.checkin(destination, sands);
            }

        }
    }

    /**
     * Encapsulates all operations concerning cluster expansion
     * 
     */
    public class RebalancingOperations {

        /**
         * Rebalance a stealer-donor node pair for a set of stores. This is run
         * on the stealer node.
         * 
         * @param stealInfo Partition steal information
         * @return The request id of the async operation
         */
        public int rebalanceNode(RebalanceTaskInfo stealInfo) {
            VAdminProto.RebalanceTaskInfoMap rebalanceTaskInfoMap = ProtoUtils.encodeRebalanceTaskInfoMap(stealInfo);
            VAdminProto.InitiateRebalanceNodeRequest rebalanceNodeRequest = VAdminProto.InitiateRebalanceNodeRequest.newBuilder()
                                                                                                                    .setRebalanceTaskInfo(rebalanceTaskInfoMap)
                                                                                                                    .build();

            VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                              .setType(VAdminProto.AdminRequestType.INITIATE_REBALANCE_NODE)
                                                                                              .setInitiateRebalanceNode(rebalanceNodeRequest)
                                                                                              .build();

            VAdminProto.AsyncOperationStatusResponse.Builder response = rpcOps.sendAndReceive(stealInfo.getStealerId(),
                                                                                              adminRequest,
                                                                                              VAdminProto.AsyncOperationStatusResponse.newBuilder());

            if(response.hasError())
                helperOps.throwException(response.getError());

            return response.getRequestId();
        }

        /**
         * Delete the rebalancing metadata related to the store on the stealer
         * node
         * 
         * @param donorNodeId The donor node id
         * @param stealerNodeId The stealer node id
         * @param storeName The name of the store
         */
        public void deleteStoreRebalanceState(int donorNodeId, int stealerNodeId, String storeName) {

            VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                         .setType(VAdminProto.AdminRequestType.DELETE_STORE_REBALANCE_STATE)
                                                                                         .setDeleteStoreRebalanceState(VAdminProto.DeleteStoreRebalanceStateRequest.newBuilder()
                                                                                                                                                                   .setNodeId(donorNodeId)
                                                                                                                                                                   .setStoreName(storeName)
                                                                                                                                                                   .build())
                                                                                         .build();
            VAdminProto.DeleteStoreRebalanceStateResponse.Builder response = rpcOps.sendAndReceive(stealerNodeId,
                                                                                                   request,
                                                                                                   VAdminProto.DeleteStoreRebalanceStateResponse.newBuilder());
            if(response.hasError())
                helperOps.throwException(response.getError());

        }

        /**
         * Retrieve the server
         * {@link voldemort.store.metadata.MetadataStore.VoldemortState} from a
         * remote node.
         * 
         * @param nodeId The node from which we want to retrieve the state
         * @return The server state
         */
        public Versioned<VoldemortState> getRemoteServerState(int nodeId) {
            Versioned<String> value = metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                        MetadataStore.SERVER_STATE_KEY);
            return new Versioned<VoldemortState>(VoldemortState.valueOf(value.getValue()),
                                                 value.getVersion());
        }

        /**
         * Used in rebalancing to indicate change in states. Groups the
         * partition plans on the basis of stealer nodes and sends them over.
         * 
         * The various combinations and their order of execution is given below
         * where:
         * <ul>
         * <li>'cluster' means cluster state is updated
         * <li>'rebalance' means rebalance flag is set.
         * <li>'swap' means stores are swapped.
         * </ul>
         * 
         * <pre>
         * | swapRO | changeClusterMetadata | changeRebalanceState | Order                        |
         * |   f    |         t             |          t           | cluster -> rebalance         |
         * |   f    |         f             |          t           | rebalance                    |
         * |   t    |         t             |          f           | cluster -> swap              |
         * |   t    |         t             |          t           | cluster -> swap -> rebalance |
         * </pre>
         * 
         * 
         * Similarly for rollback, order means the following:
         * <ul>
         * <li>'remove from rebalance' means set rebalance flag false
         * <li>'cluster' means cluster is rolled back
         * <li>'swap' means stores are swapped
         * </ul>
         * 
         * <pre>
         * | swapRO | changeClusterMetadata | changeRebalanceState | Order                                    |
         * |   f    |         t             |          t           | remove from rebalance -> cluster         |
         * |   f    |         f             |          t           | remove from rebalance                    |
         * |   t    |         t             |          f           | cluster -> swap                          |
         * |   t    |         t             |          t           | remove from rebalance -> cluster -> swap |
         * </pre>
         * 
         * 
         * @param existingCluster Current cluster
         * @param transitionCluster Transition cluster
         * @param existingStoreDefs current store defs
         * @param targetStoreDefs transition store defs
         * @param rebalanceTaskPlanList The list of rebalance partition info
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
                                         List<StoreDefinition> existingStoreDefs,
                                         List<StoreDefinition> targetStoreDefs,
                                         List<RebalanceTaskInfo> rebalanceTaskPlanList,
                                         boolean swapRO,
                                         boolean changeClusterMetadata,
                                         boolean changeRebalanceState,
                                         boolean rollback,
                                         boolean failEarly) {
            HashMap<Integer, List<RebalanceTaskInfo>> stealerNodeToRebalanceTasks = RebalanceUtils.groupPartitionsTaskByNode(rebalanceTaskPlanList,
                                                                                                                             true);
            Set<Integer> completedNodeIds = Sets.newHashSet();

            HashMap<Integer, Exception> exceptions = Maps.newHashMap();

            try {
                for(Node node: transitionCluster.getNodes()) {
                    try {
                        individualStateChange(node.getId(),
                                              transitionCluster,
                                              targetStoreDefs,
                                              stealerNodeToRebalanceTasks.get(node.getId()),
                                              swapRO,
                                              changeClusterMetadata,
                                              changeRebalanceState,
                                              false);
                        completedNodeIds.add(node.getId());
                    } catch(Exception e) {
                        exceptions.put(node.getId(), e);
                        if(failEarly) {
                            throw e;
                        }
                    }
                }

                if(exceptions.size() > 0) {
                    throw new VoldemortRebalancingException("Got exceptions from nodes "
                                                            + exceptions.keySet());
                }

                /*
                 * If everything went smoothly, update the version of the
                 * cluster metadata
                 */
                if(changeClusterMetadata) {
                    try {
                        metadataMgmtOps.updateMetadataversion(CLUSTER_VERSION_KEY);
                    } catch(Exception e) {
                        logger.info("Exception occurred while setting cluster metadata version during Rebalance state change !!!");
                    }
                }
            } catch(Exception e) {

                if(rollback) {
                    logger.error("Got exceptions from nodes " + exceptions.keySet()
                                 + " while changing state. Rolling back state on "
                                 + completedNodeIds);

                    // Rollback changes on completed nodes
                    for(int completedNodeId: completedNodeIds) {
                        try {
                            individualStateChange(completedNodeId,
                                                  existingCluster,
                                                  existingStoreDefs,
                                                  stealerNodeToRebalanceTasks.get(completedNodeId),
                                                  swapRO,
                                                  changeClusterMetadata,
                                                  changeRebalanceState,
                                                  true);
                        } catch(Exception exception) {
                            logger.error("Error while reverting back state change for completed node "
                                                 + completedNodeId,
                                         exception);
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
         * @param rebalanceTaskPlanList The list of rebalance partition info
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
                                           List<StoreDefinition> storeDefs,
                                           List<RebalanceTaskInfo> rebalanceTaskPlanList,
                                           boolean swapRO,
                                           boolean changeClusterMetadata,
                                           boolean changeRebalanceState,
                                           boolean rollback) {

            // If we do not want to change the metadata and are not one of the
            // stealer nodes, nothing to do
            if(!changeClusterMetadata && rebalanceTaskPlanList == null) {
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
                                                  + rebalanceTaskPlanList + ">" : ""));

            VAdminProto.RebalanceStateChangeRequest.Builder getRebalanceStateChangeRequestBuilder = VAdminProto.RebalanceStateChangeRequest.newBuilder();

            if(rebalanceTaskPlanList != null) {
                List<RebalanceTaskInfoMap> map = Lists.newArrayList();
                for(RebalanceTaskInfo stealInfo: rebalanceTaskPlanList) {
                    RebalanceTaskInfoMap infoMap = ProtoUtils.encodeRebalanceTaskInfoMap(stealInfo);
                    map.add(infoMap);
                }
                getRebalanceStateChangeRequestBuilder.addAllRebalanceTaskList(map);
            }

            VAdminProto.RebalanceStateChangeRequest getRebalanceStateChangeRequest = getRebalanceStateChangeRequestBuilder.setSwapRo(swapRO)
                                                                                                                          .setChangeClusterMetadata(changeClusterMetadata)
                                                                                                                          .setChangeRebalanceState(changeRebalanceState)
                                                                                                                          .setClusterString(clusterMapper.writeCluster(cluster))
                                                                                                                          .setRollback(rollback)
                                                                                                                          .setStoresString(new StoreDefinitionsMapper().writeStoreList(storeDefs))
                                                                                                                          .build();
            VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                              .setRebalanceStateChange(getRebalanceStateChangeRequest)
                                                                                              .setType(VAdminProto.AdminRequestType.REBALANCE_STATE_CHANGE)
                                                                                              .build();
            VAdminProto.RebalanceStateChangeResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                              adminRequest,
                                                                                              VAdminProto.RebalanceStateChangeResponse.newBuilder());
            if(response.hasError()) {
                helperOps.throwException(response.getError());
            }
        }

        /**
         * Get the latest cluster from all available nodes in the cluster<br>
         * 
         * Throws exception if:<br>
         * A) Any node in the required nodes list fails to respond.<br>
         * B) Cluster is in inconsistent state with concurrent versions for
         * cluster metadata on any two nodes.<br>
         * 
         * @param requiredNodes List of nodes from which we definitely need an
         *        answer
         * @return Returns the latest cluster metadata
         */
        public Versioned<Cluster> getLatestCluster(List<Integer> requiredNodes) {
            Versioned<Cluster> latestCluster = new Versioned<Cluster>(getAdminClientCluster());
            Cluster cluster = latestCluster.getValue();
            for(Node node: cluster.getNodes()) {
                try {
                    Cluster nodesCluster = metadataMgmtOps.getRemoteCluster(node.getId())
                                                          .getValue();
                    if(!nodesCluster.equals(cluster)) {
                        throw new VoldemortException("Cluster is in inconsistent state because cluster xml on node "
                                                     + node.getId()
                                                     + " does not match cluster xml of adminClient.");
                    }
                } catch(Exception e) {
                    if(null != requiredNodes && requiredNodes.contains(node.getId()))
                        throw new VoldemortException("Failed on node " + node.getId(), e);
                    else
                        logger.info("Failed on node " + node.getId(), e);
                }
            }
            return latestCluster;
        }

        /**
         * Check the execution state of the server by checking the state of
         * {@link VoldemortState} <br>
         * 
         * This function checks if the nodes are all in normal state (
         * {@link VoldemortState#NORMAL_SERVER}).
         * 
         * @param cluster Cluster metadata whose nodes we are checking
         * @throws VoldemortRebalancingException if any node is not in normal
         *         state
         */
        public void checkEachServerInNormalState(final Cluster cluster) {
            for(Node node: cluster.getNodes()) {
                Versioned<VoldemortState> versioned = rebalanceOps.getRemoteServerState(node.getId());

                if(!VoldemortState.NORMAL_SERVER.equals(versioned.getValue())) {
                    throw new VoldemortRebalancingException("Cannot rebalance since node "
                                                            + node.getId() + " (" + node.getHost()
                                                            + ") is not in normal state, but in "
                                                            + versioned.getValue());
                } else {
                    if(logger.isInfoEnabled()) {
                        logger.info("Node " + node.getId() + " (" + node.getHost()
                                    + ") is ready for rebalance.");
                    }
                }
            }
        }

        /**
         * Given the cluster metadata, retrieves the list of store definitions.
         * 
         * <br>
         * 
         * It also checks if the store definitions are consistent across the
         * cluster
         * 
         * @param cluster The cluster metadata
         * @return List of store definitions
         */
        public List<StoreDefinition> getCurrentStoreDefinitions(Cluster cluster) {
            List<StoreDefinition> storeDefs = null;
            for(Node node: cluster.getNodes()) {
                List<StoreDefinition> storeDefList = metadataMgmtOps.getRemoteStoreDefList(node.getId())
                                                                    .getValue();
                if(storeDefs == null) {
                    storeDefs = storeDefList;
                } else {

                    // Compare against the previous store definitions
                    if(!Utils.compareList(storeDefs, storeDefList)) {
                        throw new VoldemortException("Store definitions on node " + node.getId()
                                                     + " does not match those on other nodes");
                    }
                }
            }

            if(storeDefs == null) {
                throw new VoldemortException("Could not retrieve list of store definitions correctly");
            } else {
                return storeDefs;
            }
        }

        /**
         * Given a list of store definitions, cluster and admin client returns a
         * boolean indicating if all RO stores are in the correct format.
         * 
         * <br>
         * 
         * This function also takes into consideration nodes which are being
         * bootstrapped for the first time, in which case we can safely ignore
         * checking them ( as they will have default to ro0 )
         * 
         * @param cluster Cluster metadata
         * @param storeDefs Complete list of store definitions
         */
        public void validateReadOnlyStores(Cluster cluster, List<StoreDefinition> storeDefs) {
            List<StoreDefinition> readOnlyStores = StoreDefinitionUtils.filterStores(storeDefs,
                                                                                     true);

            if(readOnlyStores.size() == 0) {
                // No read-only stores
                return;
            }

            List<String> storeNames = StoreDefinitionUtils.getStoreNames(readOnlyStores);
            for(Node node: cluster.getNodes()) {
                if(node.getNumberOfPartitions() != 0) {
                    for(Entry<String, String> storeToStorageFormat: readonlyOps.getROStorageFormat(node.getId(),
                                                                                                   storeNames)
                                                                               .entrySet()) {
                        if(storeToStorageFormat.getValue()
                                               .compareTo(ReadOnlyStorageFormat.READONLY_V2.getCode()) != 0) {
                            throw new VoldemortRebalancingException("Cannot rebalance since node "
                                                                    + node.getId()
                                                                    + " has store "
                                                                    + storeToStorageFormat.getKey()
                                                                    + " not using format "
                                                                    + ReadOnlyStorageFormat.READONLY_V2);
                        }
                    }
                }
            }
        }

    }

    /**
     * Encapsulates all operations to restore data in the cluster
     * 
     */
    public class RestoreOperations {

        /**
         * RestoreData from copies on other machines for the given nodeId
         * <p>
         * Recovery mechanism to recover and restore data actively from
         * replicated copies in the cluster.<br>
         * 
         * @param nodeId Id of the node to restoreData
         * @param parallelTransfers number of transfers
         * @throws InterruptedException
         */
        public void restoreDataFromReplications(int nodeId, int parallelTransfers) {
            restoreDataFromReplications(nodeId, parallelTransfers, -1);
        }

        /**
         * RestoreData from copies on other machines for the given nodeId
         * <p>
         * Recovery mechanism to recover and restore data actively from
         * replicated copies in the cluster.<br>
         * 
         * @param nodeId Id of the node to restoreData
         * @param parallelTransfers number of transfers
         * @param zoneId zone from which the nodes are chosen from, -1 means no
         *        zone preference
         * @throws InterruptedException
         */
        public void restoreDataFromReplications(int nodeId, int parallelTransfers, int zoneId) {
            ExecutorService executors = Executors.newFixedThreadPool(parallelTransfers,
                                                                     new ThreadFactory() {

                                                                         @Override
                                                                         public Thread newThread(Runnable r) {
                                                                             Thread thread = new Thread(r);
                                                                             thread.setName("restore-data-thread");
                                                                             return thread;
                                                                         }
                                                                     });
            try {
                List<StoreDefinition> storeDefList = metadataMgmtOps.getRemoteStoreDefList(nodeId)
                                                                    .getValue();
                Cluster cluster = metadataMgmtOps.getRemoteCluster(nodeId).getValue();

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
                    restoreStoreFromReplication(nodeId, cluster, def, executors, zoneId);
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
         * For a particular store and node, runs the replication job. This works
         * only for read-write stores
         * 
         * @param restoringNodeId The node which we want to restore
         * @param cluster The cluster metadata
         * @param storeDef The definition of the store which we want to restore
         * @param executorService An executor to allow us to run the replication
         *        job
         */
        private void restoreStoreFromReplication(final int restoringNodeId,
                                                 final Cluster cluster,
                                                 final StoreDefinition storeDef,
                                                 final ExecutorService executorService,
                                                 final int zoneId) {
            logger.info("Restoring data for store " + storeDef.getName() + " on node "
                        + restoringNodeId);

            Map<Integer, List<Integer>> restoreMapping = replicaOps.getReplicationMapping(restoringNodeId,
                                                                                          cluster,
                                                                                          storeDef,
                                                                                          zoneId);

            // migrate partition
            for(final Entry<Integer, List<Integer>> replicationEntry: restoreMapping.entrySet()) {
                final int donorNodeId = replicationEntry.getKey();
                executorService.submit(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            logger.info("Restoring data for store " + storeDef.getName()
                                        + " at node " + restoringNodeId + " from node "
                                        + replicationEntry.getKey() + " partitions:"
                                        + replicationEntry.getValue());

                            int migrateAsyncId = storeMntOps.migratePartitions(donorNodeId,
                                                                               restoringNodeId,
                                                                               storeDef.getName(),
                                                                               replicationEntry.getValue(),
                                                                               null,
                                                                               null);

                            rpcOps.waitForCompletion(restoringNodeId,
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
         * Mirror data from another voldemort server
         * 
         * @param nodeId node in the current cluster to mirror to
         * @param nodeIdToMirrorFrom node from which to mirror data
         * @param urlToMirrorFrom cluster bootstrap url to mirror from
         * @param stores set of stores to be mirrored
         * 
         */
        public void mirrorData(final int nodeId,
                               final int nodeIdToMirrorFrom,
                               final String urlToMirrorFrom,
                               List<String> stores) {
            final AdminClient mirrorAdminClient = new AdminClient(urlToMirrorFrom,
                                                                  new AdminClientConfig(),
                                                                  new ClientConfig());
            final AdminClient currentAdminClient = AdminClient.this;

            // determine the partitions residing on the mirror node
            Node mirrorNode = mirrorAdminClient.getAdminClientCluster()
                                               .getNodeById(nodeIdToMirrorFrom);
            Node currentNode = currentAdminClient.getAdminClientCluster().getNodeById(nodeId);

            if(mirrorNode == null) {
                logger.error("Mirror node specified does not exist in the mirror cluster");
                return;
            }

            if(currentNode == null) {
                logger.error("node specified does not exist in the current cluster");
                return;
            }

            // compare the mirror-from and mirrored-to nodes have same set of
            // stores
            List<String> currentStoreList = StoreUtils.getStoreNames(currentAdminClient.metadataMgmtOps.getRemoteStoreDefList(nodeId)
                                                                                                       .getValue(),
                                                                     true);
            List<String> mirrorStoreList = StoreUtils.getStoreNames(mirrorAdminClient.metadataMgmtOps.getRemoteStoreDefList(nodeIdToMirrorFrom)
                                                                                                     .getValue(),
                                                                    true);
            if(stores == null)
                stores = currentStoreList;

            if(!currentStoreList.containsAll(stores) || !mirrorStoreList.containsAll(stores)) {
                logger.error("Make sure the set of stores match on both sides");
                return;
            }

            // check if the partitions are same on both the nodes
            if(!currentNode.getPartitionIds().equals(mirrorNode.getPartitionIds())) {
                logger.error("Make sure the same set of partitions exist on both sides");
                return;
            }

            ExecutorService executors = Executors.newFixedThreadPool(stores.size(),
                                                                     new ThreadFactory() {

                                                                         @Override
                                                                         public Thread newThread(Runnable r) {
                                                                             Thread thread = new Thread(r);
                                                                             thread.setName("mirror-data-thread");
                                                                             return thread;
                                                                         }
                                                                     });

            final List<Integer> partitionIdList = mirrorNode.getPartitionIds();
            final CountDownLatch waitLatch = new CountDownLatch(stores.size());
            try {
                for(final String storeName: stores)
                    executors.submit(new Runnable() {

                        @Override
                        public void run() {
                            try {
                                logger.info("Mirroring data for store " + storeName + " from node "
                                            + nodeIdToMirrorFrom + "(" + urlToMirrorFrom
                                            + ") to node " + nodeId + " partitions:"
                                            + partitionIdList);

                                Iterator<Pair<ByteArray, Versioned<byte[]>>> iterator = mirrorAdminClient.bulkFetchOps.fetchEntries(nodeIdToMirrorFrom,
                                                                                                                                    storeName,
                                                                                                                                    partitionIdList,
                                                                                                                                    null,
                                                                                                                                    false);
                                currentAdminClient.streamingOps.updateEntries(nodeId,
                                                                              storeName,
                                                                              iterator,
                                                                              null);

                                logger.info("Mirroring data for store:" + storeName + " from node "
                                            + nodeIdToMirrorFrom + " completed.");
                            } catch(Exception e) {
                                logger.error("Mirroring operation for store " + storeName
                                             + "from node " + nodeIdToMirrorFrom + " failed.", e);
                            } finally {
                                waitLatch.countDown();
                            }
                        }
                    });
                waitLatch.await();
            } catch(Exception e) {
                logger.error("Mirroring operation failed.", e);
            } finally {
                executors.shutdown();
                logger.info("Finished mirroring data.");
            }
        }
    }

    /**
     * Encapsulates all operations specific to read-only stores alone
     * 
     */
    public class ReadOnlySpecificOperations {

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
            VAdminProto.RollbackStoreResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                       adminRequest,
                                                                                       VAdminProto.RollbackStoreResponse.newBuilder());
            if(response.hasError()) {
                helperOps.throwException(response.getError());
            }
            return;
        }

        public void rollbackStore(List<Integer> nodeIds, String storeName, long pushVersion) {
            VAdminProto.RollbackStoreRequest.Builder rollbackStoreRequest = VAdminProto.RollbackStoreRequest.newBuilder()
                                                                                                            .setStoreName(storeName)
                                                                                                            .setPushVersion(pushVersion);

            VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                              .setRollbackStore(rollbackStoreRequest)
                                                                                              .setType(VAdminProto.AdminRequestType.ROLLBACK_STORE)
                                                                                              .build();
            for(Integer nodeId: nodeIds) {
                VAdminProto.RollbackStoreResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                           adminRequest,
                                                                                           VAdminProto.RollbackStoreResponse.newBuilder());
                if(response.hasError()) {
                    helperOps.throwException(response.getError());
                }
            }
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
            VAdminProto.AsyncOperationStatusResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                              adminRequest,
                                                                                              VAdminProto.AsyncOperationStatusResponse.newBuilder());

            if(response.hasError()) {
                helperOps.throwException(response.getError());
            }

            int asyncId = response.getRequestId();
            return rpcOps.waitForCompletion(nodeId, asyncId, timeoutMs, TimeUnit.MILLISECONDS);
        }

        /**
         * When a fetch store fails, we don't need to keep the pushed data
         * around. This function deletes its...
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
            VAdminProto.FailedFetchStoreResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                          adminRequest,
                                                                                          VAdminProto.FailedFetchStoreResponse.newBuilder());
            if(response.hasError()) {
                helperOps.throwException(response.getError());
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
            VAdminProto.SwapStoreResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                   adminRequest,
                                                                                   VAdminProto.SwapStoreResponse.newBuilder());
            if(response.hasError()) {
                helperOps.throwException(response.getError());
            }
            return response.getPreviousStoreDir();
        }

        /**
         * Returns the read-only storage format - {@link ReadOnlyStorageFormat}
         * for a list of stores
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
            VAdminProto.GetROStorageFormatResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                            adminRequest,
                                                                                            VAdminProto.GetROStorageFormatResponse.newBuilder());
            if(response.hasError()) {
                helperOps.throwException(response.getError());
            }

            Map<String, String> storeToValues = ProtoUtils.encodeROMap(response.getRoStoreVersionsList());

            if(storeToValues.size() != storeNames.size()) {
                storeNames.removeAll(storeToValues.keySet());
                throw new VoldemortException("Did not retrieve values for " + storeNames);
            }
            return storeToValues;
        }

        /**
         * Returns the max version of push currently being used by read-only
         * store. Important to remember that this may not be the 'current'
         * version since multiple pushes (with greater version numbers) may be
         * in progress currently
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
            VAdminProto.GetROMaxVersionDirResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                            adminRequest,
                                                                                            VAdminProto.GetROMaxVersionDirResponse.newBuilder());
            if(response.hasError()) {
                helperOps.throwException(response.getError());
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
            VAdminProto.GetROCurrentVersionDirResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                                adminRequest,
                                                                                                VAdminProto.GetROCurrentVersionDirResponse.newBuilder());
            if(response.hasError()) {
                helperOps.throwException(response.getError());
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
         * @return Returns a map of store name to the respective max version
         *         number
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
         * Returns the max version of push currently being used by read-only
         * store. Important to remember that this may not be the 'current'
         * version since multiple pushes (with greater version numbers) may be
         * in progress currently
         * 
         * @param nodeId The id of the node on which the store is present
         * @param storeNames List of all the stores
         * @return Returns a map of store name to the respective max version
         *         number
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
         * This is a wrapper around {@link #getROMaxVersion(int, List)} where-in
         * we find the max versions on each machine and then return the max of
         * all of them
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
         * Fetch read-only store files to a specified directory. This is run on
         * the stealer node side
         * 
         * @param nodeId The node id from where to copy
         * @param storeName The name of the read-only store
         * @param partitionIds List of partitionIds
         * @param destinationDirPath The destination path
         * @param notAcceptedBuckets These are Pair< partition, replica > which
         *        we cannot copy AT all. This is because these are current
         *        mmap-ed and are serving traffic.
         * @param running A boolean which will control when we want to stop the
         *        copying of files. As long this is true, we will continue
         *        copying. Once this is changed to false we'll disable the
         *        copying
         */
        public void fetchPartitionFiles(int nodeId,
                                        String storeName,
                                        List<Integer> partitionIds,
                                        String destinationDirPath,
                                        Set<Object> notAcceptedBuckets,
                                        AtomicBoolean running) {
            if(!Utils.isReadableDir(destinationDirPath)) {
                throw new VoldemortException("The destination path (" + destinationDirPath
                                             + ") to store " + storeName + " does not exist");
            }

            Node node = AdminClient.this.getAdminClientCluster().getNodeById(nodeId);
            final SocketDestination destination = new SocketDestination(node.getHost(),
                                                                        node.getAdminPort(),
                                                                        RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
            final SocketAndStreams sands = socketPool.checkout(destination);
            DataOutputStream outputStream = sands.getOutputStream();
            final DataInputStream inputStream = sands.getInputStream();

            try {

                // Add the metadata file if it doesn't exist - We do this
                // because
                // for new nodes the stores don't start with any metadata file

                File metadataFile = new File(destinationDirPath, ".metadata");
                if(!metadataFile.exists()) {
                    ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();
                    metadata.add(ReadOnlyStorageMetadata.FORMAT,
                                 ReadOnlyStorageFormat.READONLY_V2.getCode());
                    FileUtils.writeStringToFile(metadataFile, metadata.toJsonString());
                }

                VAdminProto.FetchPartitionFilesRequest fetchPartitionFileRequest = VAdminProto.FetchPartitionFilesRequest.newBuilder()
                                                                                                                         .setStoreName(storeName)
                                                                                                                         .addAllPartitionIds(partitionIds)
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
                        helperOps.close(sands.getSocket());
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
                            throw new VoldemortException("Cannot copy file "
                                                         + fileEntry.getFileName()
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
                helperOps.close(sands.getSocket());
                throw new VoldemortException(e);
            } finally {
                socketPool.checkin(destination, sands);
            }

        }
    }

    public class QuotaManagementOperations {

        public void setQuota(String storeName, String quotaTypeStr, String quotaValue) {
            QuotaType quotaType = QuotaType.valueOf(quotaTypeStr);
            if(quotaType != QuotaType.GET_THROUGHPUT && quotaType != QuotaType.PUT_THROUGHPUT) {
                // TODO : Convert this to warning to exception once existing
                // clients are upgraded. Probably around End of 2014.
                logger.warn(" Quota only supports GET (Read) / PUT (Write) throughputs. Other throughput types are deprecated for easier use"
                            + "StoreName: " + storeName + " QuotaType: " + quotaType);
            }
            // FIXME This is a temporary workaround for System store client not
            // being able to do a second insert. We simply generate a super
            // clock that will trump what is on storage
            VectorClock denseClock = VectorClockUtils.makeClockWithCurrentTime(currentCluster.getNodeIds());
            String quotaKey = QuotaUtils.makeQuotaKey(storeName, quotaType);
            quotaSysStoreClient.putSysStore(quotaKey, new Versioned<String>(quotaValue, denseClock));
            logger.info("Set quota " + quotaTypeStr + " to " + quotaValue + " for store "
                        + storeName);
        }

        public void unsetQuota(String storeName, String quotaType) {
            quotaSysStoreClient.deleteSysStore(QuotaUtils.makeQuotaKey(storeName,
                                                                       QuotaType.valueOf(quotaType)));
            logger.info("Unset quota " + quotaType + " for store " + storeName);
        }

        public Versioned<String> getQuota(String storeName, String quotaType) {
            return quotaSysStoreClient.getSysStore(QuotaUtils.makeQuotaKey(storeName,
                                                                           QuotaType.valueOf(quotaType)));
        }

        /**
         * Reserve memory for the stores
         * 
         * TODO this should also now use the voldsys$_quotas system store
         * 
         * @param nodeId The node id to reserve, -1 for entire cluster
         * @param stores list of stores for which to reserve
         * @param sizeInMB size of reservation
         */
        public void reserveMemory(int nodeId, List<String> stores, long sizeInMB) {

            List<Integer> reserveNodes = new ArrayList<Integer>();
            if(nodeId == -1) {
                // if no node is specified send it to the entire cluster
                for(Node node: currentCluster.getNodes())
                    reserveNodes.add(node.getId());
            } else {
                reserveNodes.add(nodeId);
            }
            for(String storeName: stores) {
                for(Integer reserveNodeId: reserveNodes) {

                    VAdminProto.ReserveMemoryRequest reserveRequest = VAdminProto.ReserveMemoryRequest.newBuilder()
                                                                                                      .setStoreName(storeName)
                                                                                                      .setSizeInMb(sizeInMB)
                                                                                                      .build();
                    VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                                      .setReserveMemory(reserveRequest)
                                                                                                      .setType(VAdminProto.AdminRequestType.RESERVE_MEMORY)
                                                                                                      .build();
                    VAdminProto.ReserveMemoryResponse.Builder response = rpcOps.sendAndReceive(reserveNodeId,
                                                                                               adminRequest,
                                                                                               VAdminProto.ReserveMemoryResponse.newBuilder());
                    if(response.hasError())
                        helperOps.throwException(response.getError());
                }
                logger.info("Finished reserving memory for store : " + storeName);
            }
        }

        /**
         * Reserve memory for the stores
         * 
         * TODO this should also now use the voldsys$_quotas system store
         * 
         * @param nodeIds The node ids to reserve, -1 for entire cluster
         * @param storeNames list of stores for which to reserve
         * @param sizeInMB size of reservation
         */
        public void reserveMemory(List<Integer> nodeIds, List<String> storeNames, long sizeInMB) {
            for(String storeName: storeNames) {
                for(Integer nodeId: nodeIds) {

                    VAdminProto.ReserveMemoryRequest reserveRequest = VAdminProto.ReserveMemoryRequest.newBuilder()
                                                                                                      .setStoreName(storeName)
                                                                                                      .setSizeInMb(sizeInMB)
                                                                                                      .build();
                    VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                                      .setReserveMemory(reserveRequest)
                                                                                                      .setType(VAdminProto.AdminRequestType.RESERVE_MEMORY)
                                                                                                      .build();
                    VAdminProto.ReserveMemoryResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                               adminRequest,
                                                                                               VAdminProto.ReserveMemoryResponse.newBuilder());
                    if(response.hasError())
                        helperOps.throwException(response.getError());
                }
                logger.info("Finished reserving memory for store : " + storeName);
            }
        }
    }
}
