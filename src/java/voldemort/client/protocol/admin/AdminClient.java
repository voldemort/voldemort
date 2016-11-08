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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.SocketException;
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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import voldemort.VoldemortApplicationException;
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
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.json.JsonTypeDefinition;
import voldemort.server.RequestRoutingType;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.server.rebalance.VoldemortRebalancingException;
import voldemort.server.storage.prunejob.VersionedPutPruneJob;
import voldemort.server.storage.repairjob.RepairJob;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreNotFoundException;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.quota.QuotaType;
import voldemort.store.quota.QuotaUtils;
import voldemort.store.readonly.FileType;
import voldemort.store.readonly.ReadOnlyFileEntry;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.store.routed.NodeValue;
import voldemort.store.slop.Slop;
import voldemort.store.slop.Slop.Operation;
import voldemort.store.slop.SlopStreamingDisabledException;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketStore;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.store.system.SystemStoreConstants;
import voldemort.store.views.ViewStorageConfiguration;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ExceptionUtils;
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

import com.google.common.base.Objects;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.UninitializedMessageException;

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
public class AdminClient implements Closeable {

    private static final Logger logger = Logger.getLogger(AdminClient.class);
    private static final ClusterMapper clusterMapper = new ClusterMapper();
    private static final StoreDefinitionsMapper storeMapper = new StoreDefinitionsMapper();

    // Parameters for exponential back off
    private static final long INITIAL_DELAY = 250; // Initial delay
    private static final long PRINT_STATS_THRESHOLD = 10000;
    private static final long PRINT_STATS_INTERVAL = 5 * 60 * 1000; // 5 minutes

    public final static List<String> restoreStoreEngineBlackList = Arrays.asList(ReadOnlyStorageConfiguration.TYPE_NAME,
                                                                                 ViewStorageConfiguration.TYPE_NAME);

    private final ErrorCodeMapper errorMapper;
    private final SocketPool socketPool;
    private final AdminStoreClient adminStoreClient;
    private final NetworkClassLoader networkClassLoader;
    private final AdminClientConfig adminClientConfig;
    private final ClientConfig clientConfig;
    private final boolean fetchSingleStore;
    private final String debugInfo;
    private Long clusterVersion;
    private Cluster currentCluster;
    private SystemStoreClient<String, String> metadataVersionSysStoreClient = null;
    private SystemStoreClientFactory<String, String> systemStoreFactory = null;

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
    private AdminClient(AdminClientConfig adminClientConfig,
                        ClientConfig clientConfig,
                        Cluster cluster) {
        Utils.notNull(adminClientConfig);
        Utils.notNull(clientConfig);

        if(clientConfig.getBootstrapUrls().length == 0) {
            throw new IllegalArgumentException("Client config does not have valid bootstrapUrls");
        }
        debugInfo = "BootStrapUrls: "
                    + Arrays.toString(clientConfig.getBootstrapUrls());
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
        this.clientConfig = clientConfig;
        this.socketPool = helperOps.createSocketPool(adminClientConfig);
        this.adminStoreClient = new AdminStoreClient(clientConfig);
        this.fetchSingleStore = !clientConfig.isFetchAllStoresXmlInBootstrap();

        if(cluster != null) {
            // We don't know at which time the Cluster's state was generated, so we can't know the cluster version.
            this.clusterVersion = null;
            this.currentCluster = cluster;
        } else {
            // Important: this time must be recorded before we bootstrap from the remote state
            this.clusterVersion = System.currentTimeMillis();
            this.currentCluster = getClusterFromBootstrapURL(clientConfig);
        }

        Utils.notNull(this.currentCluster);
        helperOps.initSystemStoreClient(clientConfig);
    }

    public AdminClient(String bootstrapURL) {
        this(new ClientConfig().setBootstrapUrls(bootstrapURL));
    }

    public AdminClient(Cluster cluster) {
        this(cluster, new AdminClientConfig());
    }

    public AdminClient(ClientConfig clientConfig) {
        this(new AdminClientConfig(), clientConfig);
    }

    public AdminClient(Cluster cluster, AdminClientConfig config) {
        this(config, new ClientConfig().setBootstrapUrls(cluster.getBootStrapUrls()), cluster);
    }

    public AdminClient(AdminClientConfig adminConfig, ClientConfig clientConfig) {
        this(adminConfig, clientConfig, null);
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
        this(adminClientConfig, clientConfig, cluster);
    }

    /**
     * This function can be useful to rebootstrap a new {@link AdminClient} when
     * {@link #isClusterModified()} returns true.
     *
     * @return a freshly re-bootstrapped {@link AdminClient} based on the same configs.
     */
    public AdminClient getFreshClient() {
        return new AdminClient(adminClientConfig, clientConfig);
    }

    @Override
    public String toString() {
        return "AdminClient with " + debugInfo;
    }

    /**
     * Stop the AdminClient cleanly freeing all resources.
     */
    @Override
    public void close() {
        this.socketPool.close();
        this.adminStoreClient.close();

        if(systemStoreFactory != null) {
            systemStoreFactory.close();
        }
    }

    /**
     * If Cluster is modified ( nodes are added/removed), using the old
     * AdminClient will cause inconsistent operations. This method returns true
     * under such cases, so that caller can discard the current AdminClient and
     * create new AdminClient.
     * 
     * @return true if cluster is modified since the AdminClient is create,
     *         false otherwise
     */
    public boolean isClusterModified() {
        try {
            if (null == clusterVersion) {
                // Then we have no choice to do a full cluster.xml equality check in order to know if the cluster is the same.

                // Important: this time must be recorded before we bootstrap from the remote state
                long currentTime = System.currentTimeMillis();

                Cluster remoteCluster = getClusterFromBootstrapURL(clientConfig);

                boolean remoteClusterConsistentWithLocalOne = currentCluster.equals(remoteCluster);
                if (remoteClusterConsistentWithLocalOne) {
                    // Then we can record the time we had prior to bootstrapping, as an optimization
                    clusterVersion = currentTime;
                }

                return !remoteClusterConsistentWithLocalOne;
            } else {
                // Then we can rely on the timestamp we recorded before the bootstrap phase.
                Properties props = MetadataVersionStoreUtils.getProperties(metadataVersionSysStoreClient);
                long retrievedVersion = MetadataVersionStoreUtils.getVersion(props, SystemStoreConstants.CLUSTER_VERSION_KEY);

                return retrievedVersion > this.clusterVersion;
            }
        } catch(Exception e) {
            logger.info("Error retrieving cluster metadata version");
            return true;
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

        return new AdminClient(cluster, config);
    }

    private static Cluster getClusterFromBootstrapURL(ClientConfig config) {
        SocketStoreClientFactory factory = new SocketStoreClientFactory(config);
        // get Cluster from bootStrapUrl
        String clusterXml = factory.bootstrapMetadataWithRetries(MetadataStore.CLUSTER_KEY,
                                                                 factory.validateUrls(config.getBootstrapUrls()));
        // release all threads/sockets hold by the factory.
        factory.close();

        return clusterMapper.readCluster(new StringReader(clusterXml), false);
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
        private void initSystemStoreClient(ClientConfig clientConfig) {

            String originalIdString = null;
            try {
                if(systemStoreFactory == null) {
                    originalIdString = clientConfig.getIdentifierString();
                    clientConfig.setIdentifierString("admin");
                    systemStoreFactory = new SystemStoreClientFactory<String, String>(clientConfig);
                }
                String metadataVersionStoreName = SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name();
                metadataVersionSysStoreClient = systemStoreFactory.createSystemStore(metadataVersionStoreName,
                                                                                     null,
                                                                                     null);
            } catch(Exception e) {
                logger.info("Error while creating a system store client for metadata version store/quota store.",
                            e);
            } finally {
                if(originalIdString != null) {
                    clientConfig.setIdentifierString(originalIdString);
                }
            }

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
         * @param zoneId The zone from which donor nodes will be chosen from; -1
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
                if (sands != null) {
                    helperOps.close(sands.getSocket());
                }
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
         * Retrieves a list of scheduled job ids on the server.
         * 
         * @param nodeId The id of the node whose job ids we want
         * @return List of job ids
         */
        public List<String> getScheduledJobsList(int nodeId) {
            VAdminProto.ListScheduledJobsRequest listScheduledJobsRequest = VAdminProto.ListScheduledJobsRequest.newBuilder()
                                                                                                                   .build();
            VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                              .setType(VAdminProto.AdminRequestType.LIST_SCHEDULED_JOBS)
                                                                                              .setListScheduledJobs(listScheduledJobsRequest)
                                                                                              .build();
            VAdminProto.ListScheduledJobsResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                           adminRequest,
                                                                                           VAdminProto.ListScheduledJobsResponse.newBuilder());
            if(response.hasError()) {
                helperOps.throwException(response.getError());
            }

            return response.getJobIdsList();
        }

        /**
         * Retrieves status of a scheduled job on the particular node
         * 
         * @param nodeId The id of the node on which the request is running
         * @param jobId The id of the job to terminate
         * @returns true if job is enabled
         */
        public boolean getScheduledJobStatus(int nodeId, String jobId) {
            VAdminProto.GetScheduledJobStatusRequest getScheduledJobStatusRequest = VAdminProto.GetScheduledJobStatusRequest.newBuilder()
                                                                                                                            .setJobId(jobId)
                                                                                                                            .build();
            VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                              .setType(VAdminProto.AdminRequestType.GET_SCHEDULED_JOB_STATUS)
                                                                                              .setGetScheduledJobStatus(getScheduledJobStatusRequest)
                                                                                              .build();
            VAdminProto.GetScheduledJobStatusResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                          adminRequest,
                                                                                          VAdminProto.GetScheduledJobStatusResponse.newBuilder());

            if(response.hasError()) {
                helperOps.throwException(response.getError());
            }

            return response.getEnabled();
        }

        /**
         * Terminates a scheduled job on the particular node
         * 
         * @param nodeId The id of the node on which the request is running
         * @param jobId The id of the job to terminate
         */
        public void stopScheduledJob(int nodeId, String jobId) {
            VAdminProto.StopScheduledJobRequest stopScheduledJobRequest = VAdminProto.StopScheduledJobRequest.newBuilder()
                                                                                                             .setJobId(jobId)
                                                                                                             .build();
            VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                              .setType(VAdminProto.AdminRequestType.STOP_SCHEDULED_JOB)
                                                                                              .setStopScheduledJob(stopScheduledJobRequest)
                                                                                              .build();
            VAdminProto.StopScheduledJobResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                          adminRequest,
                                                                                          VAdminProto.StopScheduledJobResponse.newBuilder());

            if(response.hasError()) {
                helperOps.throwException(response.getError());
            }
        }

        /**
         * Enables a scheduled job on the particular node
         * 
         * @param nodeId The id of the node on which the request is running
         * @param jobId The id of the job to terminate
         */
        public void enableScheduledJob(int nodeId, String jobId) {
            VAdminProto.EnableScheduledJobRequest enableScheduledJobRequest = VAdminProto.EnableScheduledJobRequest.newBuilder()
                                                                                                                   .setJobId(jobId)
                                                                                                                   .build();
            VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                              .setType(VAdminProto.AdminRequestType.ENABLE_SCHEDULED_JOB)
                                                                                              .setEnableScheduledJob(enableScheduledJobRequest)
                                                                                              .build();
            VAdminProto.EnableScheduledJobResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                            adminRequest,
                                                                                            VAdminProto.EnableScheduledJobResponse.newBuilder());

            if(response.hasError()) {
                helperOps.throwException(response.getError());
            }
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
            String nodeName = currentCluster.getNodeById(nodeId).briefToString();

            // State to detect hung async jobs
            long oldStatusTime = -1;
            long lastStatusReportTime = -1;
            final long maxUnchangingStatusDelay = adminClientConfig.getMaxBackoffDelayMs() * 10; // 10 minutes under default settings

            while(System.currentTimeMillis() < waitUntil) {
                try {
                    AsyncOperationStatus status = getAsyncRequestStatus(nodeId, requestId);
                    long statusResponseTime = System.currentTimeMillis();
                    if(!status.getStatus().equalsIgnoreCase(oldStatus)) {
                        logger.info(nodeName + " : " +  status);
                        oldStatusTime = statusResponseTime;
                        lastStatusReportTime = oldStatusTime;
                    } else if (statusResponseTime - lastStatusReportTime > maxUnchangingStatusDelay) {
                        // If hung jobs are detected, print out a message periodically
                        logger.warn("Async Task ID " + requestId + " on " + nodeName + " has not progressed for "
                                    + ((statusResponseTime - oldStatusTime) / 1000) + " seconds.");
                        lastStatusReportTime = statusResponseTime;
                    }
                    oldStatus = status.getStatus();

                    if(higherStatus != null) {
                        higherStatus.setStatus("Status from " + nodeName + " ("
                                               + status.getDescription() + ") - "
                                               + status.getStatus());
                    }
                    description = status.getDescription();
                    if(status.hasException()) {
                        logger.error("Error waiting for completion of status on " + nodeName + " : "
                                     + status, status.getException());
                        throw status.getException();
                    }

                    if(status.isComplete()) {
                        return status.getStatus();
                    }

                    if(delay < adminClientConfig.getMaxBackoffDelayMs())
                        delay <<= 1;

                    try {
                        Thread.sleep(delay);
                    } catch(InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                } catch(Exception e) {
                    String errorMessage = "Failed while waiting for async task ("
                            + description + ") at " + nodeName
                            + " to finish";
                    if(e instanceof VoldemortException) {
                        throw (VoldemortException) e;
                    } else {
                        throw new VoldemortException(errorMessage, e);
                    }
                }
            }
            throw new AsyncOperationTimeoutException("Failed to finish task requestId: " + requestId
                                                     + " in maxWait " + maxWait + " " + timeUnit.toString()
                                                     + " on " + nodeName);
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
        private ByteArray getKeyArray() {
            try {
                return new ByteArray(SystemStoreConstants.VERSIONS_METADATA_KEY.getBytes("UTF8"));
            } catch (UnsupportedEncodingException ex) {
                throw new VoldemortApplicationException("Error retrieving metadata version store key", ex);
            }
        }

        public Versioned<Properties> getMetadataVersion(Integer nodeId) {
            ByteArray keyArray = getKeyArray();
            List<Versioned<byte[]>> valueObj = storeOps.getNodeKey(SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name(),
                                                                   nodeId,
                                                                   keyArray);

            return MetadataVersionStoreUtils.parseProperties(valueObj);
        }

        private void setMetadataVersion(int nodeId, Versioned<byte[]> value) {
            ByteArray keyArray = getKeyArray();
            
            NodeValue<ByteArray, byte[]> nodeKeyValue = new NodeValue<ByteArray, byte[]>(nodeId, keyArray, value);
            storeOps.putNodeKeyValue(SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name(),
                                     nodeKeyValue);

        }

        private void setMetadataVersion(Collection<Integer> nodeIds, Versioned<Properties> props) throws Exception {

            if (props == null || props.getValue() == null) {
                return;
            }

            if (nodeIds == null || nodeIds.size() == 0) {
                logger.warn("Ignoring set Metadata versions call due to empty nodeIds");
                return;
            }

            Exception lastEx = null;
            VectorClock version = (VectorClock) props.getVersion();
            version.incrementVersion(nodeIds.iterator().next(), System.currentTimeMillis());
            byte[] versionBytes = MetadataVersionStoreUtils.convertToByteArray(props.getValue());

            Versioned<byte[]> value = new Versioned<byte[]>(versionBytes, version);
            for (Integer nodeId : nodeIds) {
                try {
                    setMetadataVersion(nodeId, value);
                } catch(InvalidMetadataException invalidEx) {
                    // When Nodes are dropped after a cluster XML update, it
                    // throws InvalidMetadataException on meta data version
                    // updates
                    logger.info("Ignoring InvalidMetadataException as the node is removed from cluster. Node: "
                                + nodeId);
                } catch (Exception ex) {
                    lastEx = ex;
                    logger.info("Error updating metadata versions on the node " + nodeId, ex);
                }
            }
            
            if(lastEx != null) {
                throw lastEx;
            }
        }

        /**
         * Set the meta data versions on all nodes to the provided properties.
         * 
         * @param newProperties The new meta data versions to be set across all
         *        the nodes in the cluster
         */
        public void setMetadataVersion(Versioned<Properties> newProperties) {
            try {
                setMetadataVersion(getAdminClientCluster().getNodeIds(), newProperties);
            } catch(Exception ex) {
                throw new VoldemortApplicationException("Error setting metadata", ex);
            }
        }

        private Versioned<Properties> getMetadataVersion(Collection<Integer> nodeIds) {
            VectorClock version = new VectorClock();
            Properties props = new Properties();
            boolean atLeastOneSuccess = false;
            for(Integer nodeId: nodeIds) {
                try {
                    Versioned<Properties> versionedProps = getMetadataVersion(nodeId);
                    VectorClock curVersion = (VectorClock) versionedProps.getVersion();
                    Properties curProps = versionedProps.getValue();
                    
                    version = version.merge(curVersion);
                    props = MetadataVersionStoreUtils.mergeVersions(props, curProps);

                    atLeastOneSuccess = true;
                } catch(Exception e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Error retrieving metadata versions for node " + nodeId, e);
                    }
                }
            }
            
            if(atLeastOneSuccess == false) {
                throw new VoldemortApplicationException("Metadata version retrieval failed on all nodes"
                                                        + Arrays.toString(nodeIds.toArray()));
            }

            return new Versioned<Properties>(props, version);
        }

        private Properties refreshVersions(Properties props, Collection<String> versionKeys) {
            if(props == null) {
                props = new Properties();
            }
            for(String versionKey: versionKeys) {
                long newValue = 0;
                if(props.getProperty(versionKey) != null) {
                    newValue = System.currentTimeMillis();
                }
                props.setProperty(versionKey, Long.toString(newValue));
            }
            return props;
        }

        /**
         * Update the metadata versions for the given keys (cluster or store).
         * The new value set is the current timestamp.
         * 
         * @param nodeIds nodes on which the metadata key is to be updated
         * @param versionKey The metadata key for which Version should be
         *        incremented
         */
        public void updateMetadataversion(Collection<Integer> nodeIds, String versionKey) {
            updateMetadataversion(nodeIds, Arrays.asList(new String[] { versionKey }));
        }

        /**
         * Update the metadata versions for the given keys (cluster or store).
         * The new value set is the current timestamp.
         * 
         * @param nodeIds nodes on which the metadata key is to be updated
         * @param versionKeys The metadata keys for which Version should be
         *        incremented
         */
        public void updateMetadataversion(Collection<Integer> nodeIds,
                                          Collection<String> versionKeys) {
            try {
                Versioned<Properties> versionProps = getMetadataVersion(nodeIds);
                Properties props = refreshVersions(versionProps.getValue(), versionKeys);

                versionProps = new Versioned<Properties>(props, versionProps.getVersion());

                setMetadataVersion(nodeIds, versionProps);
            } catch(Exception ex) {
                logger.error("Error Updating version on individual nodes falling back to old behavior ",
                             ex);
                internalUpdateMetadataversion(versionKeys);
            }
        }

        /**
         * This was the old way of updating the metadata version. But often
         * times the vector clocks on the version for the server drifts from one
         * another and it does not consolidate them correctly. This caused the
         * client to not re-boot strap. So this method is kept as fall back and
         * the versions are now resolved other servers in the public methods and
         * updated correctly
         * 
         * @param versionKeys
         */
        private void internalUpdateMetadataversion(Collection<String> versionKeys) {
            Properties props = MetadataVersionStoreUtils.getProperties(AdminClient.this.metadataVersionSysStoreClient);
            props = refreshVersions(props, versionKeys);
            MetadataVersionStoreUtils.setProperties(AdminClient.this.metadataVersionSysStoreClient,
                                                    props);
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

            if(key.equals(SystemStoreConstants.STORES_VERSION_KEY)) {
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
        public void updateRemoteMetadata(Collection<Integer> remoteNodeIds,
                                         String key,
                                         Versioned<String> value) {
            for(Integer currentNodeId: remoteNodeIds) {
                logger.info("Setting " + key + " for "
                            + getAdminClientCluster().getNodeById(currentNodeId).getHost() + ":"
                            + getAdminClientCluster().getNodeById(currentNodeId).getId());
                updateRemoteMetadata(currentNodeId, key, value);
            }

            if(key.equals(SystemStoreConstants.CLUSTER_VERSION_KEY)
               || key.equals(SystemStoreConstants.STORES_VERSION_KEY)) {
                metadataMgmtOps.updateMetadataversion(remoteNodeIds, key);
            }

        }

        /**
         * Sets metadata.
         * 
         * @param nodeIds Node ids to set metadata
         * @param key Metadata key to set
         * @param value Metadata value to set
         */
        public void updateRemoteMetadata(Collection<Integer> nodeIds, String key, String value) {
            VectorClock updatedVersion = null;
            for(Integer nodeId: nodeIds) {
                if(updatedVersion == null) {
                    updatedVersion = (VectorClock) metadataMgmtOps.getRemoteMetadata(nodeId, key)
                                                                  .getVersion();
                } else {
                    updatedVersion = updatedVersion.merge((VectorClock) metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                                                          key)
                                                                                       .getVersion());
                }
                // Bump up version on first node
                updatedVersion = updatedVersion.incremented(nodeIds.iterator().next(),
                                                            System.currentTimeMillis());
            }
            metadataMgmtOps.updateRemoteMetadata(nodeIds,
                                                 key,
                                                 Versioned.value(value, updatedVersion));
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
         * @param nodeId
         * @param key Metadata key to update
         * @param value Value for the metadata key
         * 
         * */
        public void updateRemoteMetadata(Integer nodeId, String key, String value) {
            updateRemoteMetadata(Lists.newArrayList(nodeId), key, value);
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
         * Set offline or online state at the given remoteNodeId.
         * <p>
         * 
         * See {@link voldemort.store.metadata.MetadataStore} for more
         * information.
         * 
         * @param remoteNodeId Id of the node
         * @param setOffline Ture to transit from NORMAL_SERVER to
         *        OFFLINE_SERVER state, false to transit from OFFLINE_SERVER to
         *        NORMAL_SERVER state
         */
        public void setRemoteOfflineState(int remoteNodeId, boolean setOffline) {

            VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                         .setType(VAdminProto.AdminRequestType.SET_OFFLINE_STATE)
                                                                                         .setSetOfflineState(VAdminProto.SetOfflineStateRequest.newBuilder()
                                                                                                                                               .setOfflineMode(setOffline)
                                                                                                                                               .build())
                                                                                         .build();
            VAdminProto.SetOfflineStateResponse.Builder response = rpcOps.sendAndReceive(remoteNodeId,
                                                                                         request,
                                                                                         VAdminProto.SetOfflineStateResponse.newBuilder());
            if(response.hasError()) {
                helperOps.throwException(response.getError());
            }
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

            if (remoteNodeIds == null || remoteNodeIds.size() == 0) {
                throw new IllegalArgumentException("One ore more nodes expected for NodeIds");
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

            if(clusterKey.equals(SystemStoreConstants.CLUSTER_VERSION_KEY)) {
                // Setting cluster.xml will cause all the stores to be
                // re-bootstrapped anyway.
                metadataMgmtOps.updateMetadataversion(remoteNodeIds, clusterKey);
            } else if (storesKey.equals(SystemStoreConstants.STORES_VERSION_KEY)) {
                StoreDefinitionsMapper storeDefsMapper = new StoreDefinitionsMapper();
                List<StoreDefinition> storeDefs = storeDefsMapper.readStoreList(new StringReader(storesValue.getValue()));
                if(storeDefs != null) {
                    List<String> storeNames = new ArrayList<String>();
                    try {
                        for(StoreDefinition storeDef: storeDefs) {
                            storeNames.add(storeDef.getName());
                        }

                        metadataMgmtOps.updateMetadataversion(remoteNodeIds, storeNames);
                    } catch(Exception e) {
                        System.err.println("Error while updating metadata version for the specified store.");
                    }
                }
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

        public synchronized void fetchAndUpdateRemoteMetadata(int nodeId, String key, String value) {
            VectorClock currentClock = (VectorClock) getRemoteMetadata(nodeId, key).getVersion();

            updateRemoteMetadata(nodeId,
                                 key,
                                 new Versioned<String>(Boolean.toString(false),
                                                       currentClock.incremented(nodeId, 1)));
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

            metadataMgmtOps.updateMetadataversion(Arrays.asList(nodeId), MetadataStore.CLUSTER_KEY);
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
         * Update the store definitions on a list of remote nodes.
         * <p>
         * 
         * @param storeDefs The new store definition list
         * @param nodeIds The node id of the machine
         * @throws VoldemortException
         */
        public void updateRemoteStoreDefList(List<StoreDefinition> storeDefs,
                                             Collection<Integer> nodeIds) throws VoldemortException {
            // Check for backwards compatibility
            StoreDefinitionUtils.validateSchemasAsNeeded(storeDefs);

            for(Integer nodeId: nodeIds) {

                // Ensure it doesn't break the store
                Versioned<List<StoreDefinition>> remoteStoreDefList = metadataMgmtOps.getRemoteStoreDefList(nodeId);
                StoreDefinitionUtils.validateNewStoreDefsAreNonBreaking(remoteStoreDefList.getValue(), storeDefs);

                logger.info("Updating stores.xml for "
                            + currentCluster.getNodeById(nodeId).getHost() + ":" + nodeId);
                // get current version.
                VectorClock oldClock = (VectorClock) remoteStoreDefList.getVersion();

                Versioned<String> value = new Versioned<String>(storeMapper.writeStoreList(storeDefs),
                                                                oldClock.incremented(nodeId, 1));

                ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(MetadataStore.STORES_KEY,
                                                                      "UTF-8"));
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
                if(response.hasError()) {
                    helperOps.throwException(response.getError());
                }
            }
        }

        /**
         * Update the store definitions on a remote node.
         * <p>
         * 
         * @param nodeId The node id of the machine
         * @param storeDefs The new store definition list
         * @throws VoldemortException
         */
        public void updateRemoteStoreDefList(Integer nodeId, List<StoreDefinition> storeDefs)
                throws VoldemortException {
            updateRemoteStoreDefList(storeDefs, Arrays.asList(nodeId));
        }

        /**
         * Wrapper for updateRemoteStoreDefList : update this for all nodes
         * <p>
         * 
         * @param storeDefs The new store list
         * @throws VoldemortException
         */
        public void updateRemoteStoreDefList(List<StoreDefinition> storeDefs)
                throws VoldemortException {
            updateRemoteStoreDefList(storeDefs, currentCluster.getNodeIds());
        }
        
        private Versioned<List<StoreDefinition>> getRemoteStoreDefList(int nodeId,
                                                                       String metadataKey)
                throws VoldemortException {
            Versioned<String> value = metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                        metadataKey);
            List<StoreDefinition> storeList = storeMapper.readStoreList(new StringReader(value.getValue()),
                                                                        false);
            return new Versioned<List<StoreDefinition>>(storeList, value.getVersion());
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
            return getRemoteStoreDefList(nodeId, MetadataStore.STORES_KEY);
        }

        public Versioned<List<StoreDefinition>> getRemoteStoreDefList() throws VoldemortException {
            Integer nodeId = currentCluster.getNodeIds().iterator().next();
            return getRemoteStoreDefList(nodeId);
        }

        /**
         * Retrieve a store from a random node in the cluster.
         * Note that, store may present on some nodes and not on other
         * nodes, due to node failures or other reasons. In those cases
         * results will be inconsistent based on the node it choose to query.
         *
         * @param storeName name of the store
         * @return null if it does not exist, StoreDefinition if it exists.
         */
        public StoreDefinition getStoreDefinition(String storeName) {
            Integer nodeId = currentCluster.getNodeIds().iterator().next();
            return getStoreDefinition(nodeId, storeName);
        }

        /**
         * Retrieve the storeDefinition from a particular node.
         * 
         * @param nodeId node to retrieve the store from
         * @param storeName name of the store.
         * @return null if it does not exist, StoreDefinition if it exists.
         */
        public StoreDefinition getStoreDefinition(int nodeId, String storeName) {
            if (storeName == null || storeName.length() == 0) {
                throw new IllegalArgumentException("storeName");
            }

            String storeKey = fetchSingleStore ? storeName : MetadataStore.STORES_KEY;

            Versioned<List<StoreDefinition>> storeDef;
            try {
                storeDef = getRemoteStoreDefList(nodeId, storeKey);
            } catch (StoreNotFoundException ex) {
                logger.info("Store " + storeName + " is not found in node " + nodeId + " key used " + storeKey);
                return null;
            }

            if (storeDef == null || storeDef.getValue() == null) {
                logger.warn("Unexpected null returned from getRemoteStoreDefList " + storeDef);
                return null;
            }

            List<StoreDefinition> retrievedStoreDefs = storeDef.getValue();
            for (StoreDefinition retrievedStoreDef : retrievedStoreDefs) {
                if (retrievedStoreDef.getName().equals(storeName)) {
                    return retrievedStoreDef;
                }
            }
            logger.info("Store " + storeName + " is not found in node " + nodeId + " Total Store"
                    + retrievedStoreDefs.size() + " key used " + storeKey);
            return null;
        }

        /**
         * Interrogates a remote server to get the values of some of its configuration parameters.
         *
         * @param nodeId of the server we wish to interrogate.
         * @param configKeys for which we want to retrieve the values.
         * @return a {@link Map<String,String>} of the requested config key/value pairs.
         */
        public Map<String, String> getServerConfig(int nodeId, Set<String> configKeys) {
            VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest
                                                                   .newBuilder()
                                                                   .setType(VAdminProto.AdminRequestType.GET_CONFIG)
                                                                   .setGetConfig(VAdminProto.GetConfigRequest
                                                                                            .newBuilder()
                                                                                            .addAllConfigKey(configKeys))
                                                                   .build();
            VAdminProto.GetConfigResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                   request,
                                                                                   VAdminProto.GetConfigResponse.newBuilder());

            if (response.getInvalidConfigMapCount() > 0) {
                String nodeName = currentCluster.getNodeById(nodeId).briefToString();
                for (VAdminProto.MapFieldEntry entry: response.getInvalidConfigMapList()) {
                    logger.error(nodeName + " responded with an error to our GetConfigRequest for key '" +
                                 entry.getKey() + "': " + entry.getValue());
                }
            }

            Map<String, String> serverConfig = Maps.newHashMap();
            for (VAdminProto.MapFieldEntry entry: response.getConfigMapList()) {
                serverConfig.put(entry.getKey(), entry.getValue());
            }

            return serverConfig;
        }

        /**
         * Interrogates all remote servers in a cluster, and validates that they all contain the
         * expected values for a set of config keys. For any server, if an expected config is
         * missing, or if it has a value which does not equal the expected one, then the function
         * returns false. Otherwise, if all configs are as expected on all servers, it returns true.
         *
         * This is intended to be a generic way to manage the graceful negotiation of whether or not
         * to enable new features for which there is a requirement that the whole cluster needs to be
         * upgraded before it can be used.
         *
         * @param expectedConfigMap a map of expected key/value configs.
         * @param maxAmountOfUnreachableNodes This parameter controls what the threshold is for the
         *                                    maximum amount of unreachable node. If that number
         *                                    exceeds the specified number, then this function will
         *                                    bubble up an {@link UnreachableStoreException}.
         * @return true if all configs are present and as expected, false otherwise.
         * @throws UnreachableStoreException if the max amount of unreachable nodes is exceeded.
         */
        public boolean validateServerConfig(Map<String, String> expectedConfigMap, int maxAmountOfUnreachableNodes)
                throws UnreachableStoreException {
            Set<String> configKeysToRequest = expectedConfigMap.keySet();
            boolean configIsValid = true;
            int currentAmountOfUnreachableNodes = 0;

            for (Node node: currentCluster.getNodes()) {
                try {
                    Map<String, String> serverConfigs = getServerConfig(node.getId(), configKeysToRequest);
                    for (Entry expectedConfig: expectedConfigMap.entrySet()) {
                        String serverConfigValue = serverConfigs.get(expectedConfig.getKey());
                        if (serverConfigValue == null) {
                            logger.error(node.briefToString() + " does not contain config key '" +
                                         expectedConfig.getKey() + "'.");
                            configIsValid = false;
                        } else if (!serverConfigValue.equals(expectedConfig.getValue())) {
                            logger.error(node.briefToString() + " contains the wrong value for config key '" +
                                         expectedConfig.getKey() + "'. Expected: '" + expectedConfig.getValue() +
                                         "'. Actual: '" + serverConfigValue + "'.");
                            configIsValid = false;
                        } // else, we're good, moving on to the next config to validate on that node!
                    }
                } catch (UnreachableStoreException e) {
                    currentAmountOfUnreachableNodes++;
                    logger.error(node.briefToString() + " is unreachable!", e);
                } catch (Exception e) {
                    // TODO: Might want to refine this error handling further...
                    logger.error("Got an exception when trying to validateServerConfig() against " +
                                 node.briefToString() + ". The server may be running an old version.", e);
                    return false;
                }
            }

            if (currentAmountOfUnreachableNodes > maxAmountOfUnreachableNodes) {
                throw new UnreachableStoreException("As part of validateServerConfig(), " + currentAmountOfUnreachableNodes +
                                                    " nodes were unreachable which exceeds the maximum (" +
                                                    maxAmountOfUnreachableNodes + ").");
            }

            return configIsValid;
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
            addStore(def, currentCluster.getNodeIds());
        }

        /**
         * Add a new store definition to a particular node
         * <p>
         * 
         * @param def the definition of the store to add
         * @param nodeId Node on which to add the store
         */
        public void addStore(StoreDefinition def, int nodeId) {
            StoreDefinitionUtils.validateSchemasAsNeeded(Arrays.asList(def));

            String value = storeMapper.writeStore(def);
            VAdminProto.AddStoreRequest.Builder addStoreRequest =
                    VAdminProto.AddStoreRequest.newBuilder().setStoreDefinition(value);
            VAdminProto.VoldemortAdminRequest request =
                    VAdminProto.VoldemortAdminRequest.newBuilder().setType(VAdminProto.AdminRequestType.ADD_STORE)
                            .setAddStore(addStoreRequest)

                            .build();

            Node node = currentCluster.getNodeById(nodeId);
            if (null == node) {
                throw new VoldemortException("Invalid node id (" + nodeId + ") specified");
            }

            logger.info("Adding store " + def.getName() + " on " + node.briefToString());
            VAdminProto.AddStoreResponse.Builder response =
                    rpcOps.sendAndReceive(nodeId, request, VAdminProto.AddStoreResponse.newBuilder());
            if (response.hasError()) {
                helperOps.throwException(response.getError());
            }
            logger.info("Successfully added " + def.getName() + " on " + node.briefToString());
        }

        public void addStore(StoreDefinition def, Collection<Integer> nodeIds) {
            for(Integer nodeId: nodeIds) {
                addStore(def, nodeId);
            }
        }

        class NodeStoreRetriever implements Runnable {
            final int nodeId;
            final String key;
            final ConcurrentMap<Integer, List<StoreDefinition>> results;

            public NodeStoreRetriever(int nodeId, String key, ConcurrentMap<Integer, List<StoreDefinition>> results) {
                this.nodeId = nodeId;
                this.key = key;
                this.results = results;
            }

            @Override
            public void run() {
                List<StoreDefinition> retrievedStoreDefs =
                        metadataMgmtOps.getRemoteStoreDefList(nodeId, key).getValue();
                results.put(nodeId, retrievedStoreDefs);
            }
        }

        /**
         * Ideally this function should be in the SerializerDefinition class,
         * but that class already has different way of comparing Serializers.
         * Not sure what will be the impact of refactoring that code.
         * 
         * @param oldDef
         * @param newDef
         * @return
         */
        private boolean seriailizerMetadataEquals(SerializerDefinition oldDef,
                                                 SerializerDefinition newDef) {
            if(!oldDef.getName().equals(newDef.getName())) {
                return false;
            }
            return Objects.equal(oldDef.getCompression(), newDef.getCompression());
        }

        private StoreDefinition getNewStoreWithRemoteSerializer(StoreDefinition remoteStoreDef,
                StoreDefinition newStoreDef) {
            StoreDefinition newStoreDefWithRemoteSerializer =
                    new StoreDefinition(
                            newStoreDef.getName(),
                            newStoreDef.getType(),
                            newStoreDef.getDescription(),
                            remoteStoreDef.getKeySerializer(), // Remote Key SerDe
                            remoteStoreDef.getValueSerializer(), // Remote Value SerDe
                            newStoreDef.getTransformsSerializer(), newStoreDef.getRoutingPolicy(),
                            newStoreDef.getRoutingStrategyType(), newStoreDef.getReplicationFactor(),
                            newStoreDef.getPreferredReads(), newStoreDef.getRequiredReads(),
                            newStoreDef.getPreferredWrites(), newStoreDef.getRequiredWrites(),
                            newStoreDef.getViewTargetStoreName(), newStoreDef.getValueTransformation(),
                            newStoreDef.getZoneReplicationFactor(), newStoreDef.getZoneCountReads(),
                            newStoreDef.getZoneCountWrites(), newStoreDef.getRetentionDays(),
                            newStoreDef.getRetentionScanThrottleRate(), newStoreDef.getRetentionFrequencyDays(),
                            newStoreDef.getSerializerFactory(), newStoreDef.getHintedHandoffStrategyType(),
                            newStoreDef.getHintPrefListSize(), newStoreDef.getOwners(),
                            newStoreDef.getMemoryFootprintMB());

            return newStoreDefWithRemoteSerializer;
        }

        private void validateSerializerDefs(StoreDefinition remoteStoreDef, StoreDefinition newStoreDef, Node node,
                String localProcessName) {
            SerializerDefinition newKeySerializerDef = newStoreDef.getKeySerializer();
            SerializerDefinition newValueSerializerDef = newStoreDef.getValueSerializer();
            SerializerDefinition remoteKeySerializerDef = remoteStoreDef.getKeySerializer();
            SerializerDefinition remoteValueSerializerDef = remoteStoreDef.getValueSerializer();
            String newValSerDeName = newValueSerializerDef.getName();

            if(seriailizerMetadataEquals(remoteKeySerializerDef,newKeySerializerDef)
               && seriailizerMetadataEquals(remoteValueSerializerDef,newValueSerializerDef)) {
                Object remoteKeyDef, remoteValDef, localKeyDef, localValDef;
                if (newValSerDeName.equals(DefaultSerializerFactory.AVRO_GENERIC_VERSIONED_TYPE_NAME) ||
                        newValSerDeName.equals(DefaultSerializerFactory.AVRO_GENERIC_TYPE_NAME)) {
                    remoteKeyDef = Schema.parse(remoteKeySerializerDef.getCurrentSchemaInfo());
                    remoteValDef = Schema.parse(remoteValueSerializerDef.getCurrentSchemaInfo());
                    localKeyDef = Schema.parse(newKeySerializerDef.getCurrentSchemaInfo());
                    localValDef = Schema.parse(newValueSerializerDef.getCurrentSchemaInfo());
                } else if (newValSerDeName.equals(DefaultSerializerFactory.JSON_SERIALIZER_TYPE_NAME)) {
                    remoteKeyDef = JsonTypeDefinition.fromJson(remoteKeySerializerDef.getCurrentSchemaInfo());
                    remoteValDef = JsonTypeDefinition.fromJson(remoteValueSerializerDef.getCurrentSchemaInfo());
                    localKeyDef = JsonTypeDefinition.fromJson(newKeySerializerDef.getCurrentSchemaInfo());
                    localValDef = JsonTypeDefinition.fromJson(newValueSerializerDef.getCurrentSchemaInfo());
                } else {
                    throw new VoldemortException("verifyOrAddStore() only works with Avro Generic and JSON serialized stores!");
                }
                boolean serializerDefinitionsAreEqual = remoteKeyDef.equals(localKeyDef) && remoteValDef.equals(localValDef);
                
                if (serializerDefinitionsAreEqual) {
                    StoreDefinition newStoreDefWithRemoteSerializer =
                            getNewStoreWithRemoteSerializer(remoteStoreDef, newStoreDef);
                    if (remoteStoreDef.equals(newStoreDefWithRemoteSerializer)) {
                        // The difference is in one of the ignorable fields like owner, description
                        return;
                    } else {
                        // if we still get a fail, then we know that the store defs don't match for reasons
                        // OTHER than the key/value serializer
                        String errorMessage = "Your store schema is identical, " +
                                "but the store definition does not match on " + node.briefToString();
                        logger.error(errorMessage + diffMessage(newStoreDefWithRemoteSerializer, remoteStoreDef, localProcessName));
                        throw new VoldemortException(errorMessage);
                    }
                } else {
                    String errorMessage = "Your store definition does not match the store definition that is " +
                            "already defined on " + node.briefToString();
                    logger.error(errorMessage + diffMessage(newStoreDef, remoteStoreDef, localProcessName));
                    throw new VoldemortException(errorMessage);
                }
            } else {
                String errorMessage =
                        "Your store definition does not match the store definition that is " + "already defined on "
                                + node.briefToString();
                logger.error(errorMessage + diffMessage(newStoreDef, remoteStoreDef, localProcessName));
                throw new VoldemortException(errorMessage);
            }
        }

        /**
         * validate the newStoreDefinition is compatible with existing
         * storeDefinition. If they are incompatible, it throws an error.
         *
         * @param remoteStoreDef Store retrieved from the remote node
         * @param newStoreDef Store that needs to be created
         */
        private void validateStoreDefinition(StoreDefinition remoteStoreDef, StoreDefinition newStoreDef, Node node,
                String localProcessName) {
            if (remoteStoreDef == null || newStoreDef == null || node == null) {
                throw new IllegalArgumentException(" one of the input parameters is null");
            }

            String storeName = remoteStoreDef.getName();
            if (!storeName.equals(newStoreDef.getName())) {
                throw new IllegalArgumentException(" Remote Store " + storeName + " New Store " + newStoreDef.getName());
            }

            if (remoteStoreDef.equals(newStoreDef)) {
                // Remote Store and Current store is exact match, no need to add the store.
                return;
            }

            validateSerializerDefs(remoteStoreDef, newStoreDef, node, localProcessName);
        }

        private List<StoreDefinition> getFutureResult(int nodeId, Map<Integer, Future> nodeTasks,
                ConcurrentMap<Integer, List<StoreDefinition>> nodeStores) {
            validateTaskCompleted(nodeId, nodeTasks);
            List<StoreDefinition> storeDefs = nodeStores.get(nodeId);
            if (storeDefs == null) {
                throw new VoldemortException("No error in future, but empty stores returned, unexpected");
            }
            return storeDefs;
        }
        
        // unreachableNodes gets passed in as empty list, unreachable nodes are added to that list.
        private List<Integer> getNodesMissingNewStore(StoreDefinition newStoreDef, String localProcessName,
                ExecutorService executor, List<Node> unreachableNodes) {
            List<Integer> nodesMissingNewStore = Lists.newArrayList();

            ConcurrentMap<Integer, List<StoreDefinition>> nodeStores =
                    new ConcurrentHashMap<Integer, List<StoreDefinition>>();

            Map<Integer, Future> nodeTasks = new HashMap<Integer, Future>();

            // Get all StoreDefinitions or just the particular store, depending on the preference
            String storeKey = fetchSingleStore ? newStoreDef.getName() : MetadataStore.STORES_KEY;

            if (executor != null) {
                for (Node node : currentCluster.getNodes()) {
                    int nodeId = node.getId();
                    NodeStoreRetriever task = new NodeStoreRetriever(nodeId, storeKey, nodeStores);
                    Future future = executor.submit(task);
                    nodeTasks.put(nodeId, future);
                }
            }

            for (Node node : currentCluster.getNodes()) {
                int nodeId = node.getId();
                List<StoreDefinition> retrievedStoreDefs;
                try {
                    if (executor == null) {
                        retrievedStoreDefs = metadataMgmtOps.getRemoteStoreDefList(nodeId, storeKey).getValue();
                    } else {
                        retrievedStoreDefs = getFutureResult(nodeId, nodeTasks, nodeStores);
                    }
                } catch (StoreNotFoundException ex) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Store does not exist " + node.briefToString() + " message " + ex.getMessage());
                    }
                    // No store could be found, assume empty store definition
                    retrievedStoreDefs = Lists.newArrayList();
                } catch (VoldemortException e) {

                    // getRemoteStoreDefList() internally results in a socket pool checkout which can throw
                    // SocketException and possibly other subclasses of IOException, so we check for IOException
                    // to catch all of these cases...
                    if (ExceptionUtils.recursiveClassEquals(e, UnreachableStoreException.class, IOException.class)) {
                        logger.warn("Failed to contact " + node.briefToString() + " in order to validate the StoreDefinition.");
                        unreachableNodes.add(node);
                        continue;
                    } else {
                        throw e;
                    }
                }

                StoreDefinition remoteStoreDef = null;
                for (StoreDefinition retrievedStoreDef : retrievedStoreDefs) {
                    if (retrievedStoreDef.getName().equals(newStoreDef.getName())) {
                        remoteStoreDef = retrievedStoreDef;
                        break;
                    }
                }

                if (remoteStoreDef == null) {
                    nodesMissingNewStore.add(nodeId);
                } else {
                    validateStoreDefinition(remoteStoreDef, newStoreDef, node, localProcessName);
                }
            }

            return nodesMissingNewStore;
        }
        
        private void addStoresViaExecutorService(final StoreDefinition newStoreDef, List<Integer> nodesMissingNewStore,
                ExecutorService executor) {
            if (executor == null) {
                throw new IllegalArgumentException("executor is null");
            }

            Map<Integer, Future> nodeTasks = new HashMap<Integer, Future>();
            for (final Integer nodeId : nodesMissingNewStore) {
                Future future = executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        storeMgmtOps.addStore(newStoreDef, nodeId);
                    }
                });
                nodeTasks.put(nodeId, future);
            }
            
            RuntimeException lastEx = null;
            for(final Integer nodeId : nodesMissingNewStore) {
                try {
                    validateTaskCompleted(nodeId, nodeTasks);
                } catch (RuntimeException ex) {
                    // wait for all addStores to complete, throw the last one.
                    lastEx = ex;
                }
            }

            if (lastEx != null) {
                throw lastEx;
            }
        }

        /**
         * This function ensures that a StoreDefinition exists on all online Voldemort Servers.
         *
         * These are the steps this function goes through:
         * 1) For each node in the cluster, checks if a StoreDefinition already exists for this
         *    store name:
         *    1.1) If a store with that name does already exist, then it checks if the the
         *         definitions are consistent:
         *         1.1.1) If the definitions are inconsistent, then a {@link VoldemortException}
         *                is thrown with a detailed error message about the differences between
         *                the intended and the already existing (remote) definition.
         *         1.1.2) If the definitions are consistent, then this is a no-op.
         *    1.2) If a store with that name does not already exist, then it gets created.
         *
         * This function is idempotent, in the sense that it can be executed against a cluster
         * which already has the desired StoreDefinition registered on some or all nodes, and
         * it will fill in the blank as needed.
         *
         * WARNING: Only intended for Read-Only stores. Use on Read-Write stores at your own risk!
         *
         * @param newStoreDef StoreDefinition to make sure exists on all online Voldemort Servers
         * @param localProcessName Name of the process interested in creating the store
         *                         (for example: Build and Push), used for debugging purposes.
         * @param createStore whether or not add new store if stores are not found in the cluster.
         * @throws UnreachableStoreException Thrown if one or more server was unreachable. Can
         *                                   potentially be ignored, in certain use cases.
         * @throws VoldemortException Thrown if a server contains an incompatible StoreDefinitions.
         */
        public void verifyOrAddStore(StoreDefinition newStoreDef, String localProcessName, boolean createStore,
                ExecutorService executor) throws UnreachableStoreException, VoldemortException {
            if (!newStoreDef.getType().equals(ReadOnlyStorageConfiguration.TYPE_NAME)) {
                throw new VoldemortException("verifyOrAddStore() is intended only for Read-Only stores!");
            }

            long startTime = System.currentTimeMillis();
            List<Node> unreachableNodes = Lists.newArrayList();
            List<Integer> nodesMissingNewStore =
                    getNodesMissingNewStore(newStoreDef, localProcessName, executor, unreachableNodes);

            long verifyCompletionTime = System.currentTimeMillis();
            long elapsedTime = verifyCompletionTime - startTime;
            String timingInfo = "verifyOrAddStore() " + AdminClient.this.debugInfo + " Store: "
                                + newStoreDef.getName() + " Verification Time: " + elapsedTime
                                + " ms";

            if(!nodesMissingNewStore.isEmpty()) {
                if(!createStore) {
                    throw new VoldemortException("Store: " + newStoreDef.getName()
                                                 + " is not found in the current cluster.");
                }
                if (executor == null) {
                    storeMgmtOps.addStore(newStoreDef, nodesMissingNewStore);
                } else {
                    addStoresViaExecutorService(newStoreDef, nodesMissingNewStore, executor);
                }

                long createCompletionTime = System.currentTimeMillis();
                elapsedTime = createCompletionTime - verifyCompletionTime;
                timingInfo += ", Creation Time: " + elapsedTime + " ms";
            }

            logger.info(timingInfo);

            if (unreachableNodes.size() > 0) {
                String errorMessage = "verifyOrAddStore() failed against the following nodes: ";
                boolean first = true;
                for (Node node: unreachableNodes) {
                    if (first) {
                        first = false;
                    } else {
                        errorMessage += ", ";
                    }
                    errorMessage += node.briefToString();
                }
                throw new UnreachableStoreException(errorMessage);
            }
        }

        public void verifyOrAddStore(StoreDefinition newStoreDef, String localProcessName, boolean createStore) {
            verifyOrAddStore(newStoreDef, localProcessName, createStore, null);
        }

        public void verifyOrAddStore(StoreDefinition newStoreDef, String localProcessName) {
            verifyOrAddStore(newStoreDef, localProcessName, true, null);
        }

        public void verifyOrAddStore(StoreDefinition newStoreDef, String localProcessName, ExecutorService service) {
            verifyOrAddStore(newStoreDef, localProcessName, true, service);
        }

        private String diffMessage(StoreDefinition newStoreDef, StoreDefinition remoteStoreDef, String localProcessName) {
            String thisName = localProcessName + " has";
            String otherName = "Voldemort server has";
            String message = "\n" + thisName + ":\t" + newStoreDef +
                    "\n" + otherName + ":\t" + remoteStoreDef +
                    "\n" + newStoreDef.diff(remoteStoreDef, thisName, otherName);
            return message;
        }

        /**
         * Delete a store from all active nodes in the cluster
         *
         * @param storeName name of the store to delete
         * @throws VoldemortException of the first node which failed (note, there might be more)
         * @see {@link #deleteStore(String, java.util.List)} for more visibility into specific failures.
         */
        public void deleteStore(String storeName) {
            List<Integer> nodeIds = Lists.newArrayList(currentCluster.getNodeIds());
            Map<Integer, VoldemortException> exceptionMap = deleteStore(storeName, nodeIds);
            if (!exceptionMap.isEmpty()) {
                throw exceptionMap.values().iterator().next();
            }
        }

        /**
         * Delete a store from a particular node
         *
         * @param storeName name of the store to delete
         * @param nodeId Node on which we want to delete a store
         * @throws VoldemortException if it fails to delete
         */
        public void deleteStore(String storeName, int nodeId) {
            List<Integer> nodeIds = Lists.newArrayList(nodeId);
            Map<Integer, VoldemortException> exceptionMap = deleteStore(storeName, nodeIds);
            if (exceptionMap.containsKey(nodeId)) {
                throw exceptionMap.get(nodeId);
            }
        }

        /**
         * Delete a store from all specified nodes
         *
         * @param storeName name of the store to delete
         * @param nodeIds list of node IDs on which we want to delete the store
         * @return {@link java.util.Map<Integer, VoldemortException>} mapping each node ID to the
         *         exception it threw. If the map is empty, then the operation succeeded on all nodes.
         */
        public Map<Integer, VoldemortException> deleteStore(String storeName, List<Integer> nodeIds) {
            VAdminProto.DeleteStoreRequest.Builder deleteStoreRequest = VAdminProto.DeleteStoreRequest.newBuilder()
                                                                                                      .setStoreName(storeName);
            VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                         .setType(VAdminProto.AdminRequestType.DELETE_STORE)
                                                                                         .setDeleteStore(deleteStoreRequest)
                                                                                         .build();
            Map<Integer, VoldemortException> exceptionMap = Maps.newHashMap();

            for(Integer nodeId: nodeIds) {
                Node node = currentCluster.getNodeById(nodeId);
                if(node == null) {
                    throw new VoldemortException("Invalid node id (" + nodeId + ") specified");
                }

                logger.info("Deleting '" + storeName + "' on " + node.briefToString());
                VoldemortException ex = null;
                try {
                    VAdminProto.DeleteStoreResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                             request,
                                                                                             VAdminProto.DeleteStoreResponse.newBuilder());
                    if(response.hasError()) {
                        VProto.Error error = response.getError();
                        ex = AdminClient.this.errorMapper.getError((short) error.getErrorCode(),
                                                                                      error.getErrorMessage());
                    }
                } catch (UnreachableStoreException e) {
                    ex = e;
                }
                if (ex == null) {
                    logger.info("Successfully deleted '" + storeName + "' on " + node.briefToString());
                } else {
                    exceptionMap.put(nodeId, ex);
                }
            }

            return exceptionMap;
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
                                                       clientConfig.getSocketKeepAlive(),
                                                       "-admin");
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

        /**
         * Fetch values for given keys on a specific store present on a specific node.
         * 
         * @param storeName Name of the Store
         * @param nodeId Id of the node to query from
         * @param keys List of keys to be required.
         * @return
         * Only keys that has values will be returned in a Map.
         * Keys that does not exists will not be present in the Map.
         */
        public Map<ByteArray, List<Versioned<byte[]>>> getAllNodeKeys(String storeName, int nodeId,
                Iterable<ByteArray> keys) {
            SocketStore socketStore = adminStoreClient.getSocketStore(nodeId, storeName);
            return socketStore.getAll(keys, null);
        }

        /**
         * Delete a given key
         * 
         * @param storeName
         * @param nodeId
         * @param key
         * @return true if all versioned values get deleted
         */
        public boolean deleteNodeKeyValue(String storeName, int nodeId, ByteArray key) {
            SocketStore socketStore = adminStoreClient.getSocketStore(nodeId, storeName);
            List<Versioned<byte[]>> values = getNodeKey(storeName, nodeId, key);
            boolean result = true;
            for(Versioned<byte[]> value: values) {
                Version version = value.getVersion();
                result = result && socketStore.delete(key, version);
            }
            return result;
        }
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
         * {@link VoldemortException} has occurred.</li>
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
         * {@link VoldemortException} has occurred.</li>
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
                if(e instanceof SocketException) {
                    throw new SlopStreamingDisabledException("Failed to update slop entries to node "
                                                                     + node.getId(),
                                                             e);
                } else {
                    throw new VoldemortException(e);
                }
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
        // TODO: this method should be moved to helperOps
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
                        logger.error("Error during rebalance on node " + node.getId(), e);
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
                        metadataMgmtOps.updateMetadataversion(getAdminClientCluster().getNodeIds(),
                                                              SystemStoreConstants.CLUSTER_VERSION_KEY);
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
         * It also checks if the store definitions are consistent across the
         * cluster, except for one specific node.
         *
         * @param cluster The cluster metadata
         * @param nodeId Do not check this node, we don't trust it right now.
         *               May be -1 to check every node.
         * @return List of store definitions
         */
        public List<StoreDefinition> getCurrentStoreDefinitionsExcept(Cluster cluster, int nodeId) {
            List<StoreDefinition> storeDefs = null;
            for(Node node: cluster.getNodes()) {
                if (node.getId() == nodeId)
                    continue;
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
         * Given the cluster metadata, retrieves the list of store definitions.
         * It also checks if the store definitions are consistent across the
         * cluster
         *
         * @param cluster The cluster metadata
         * @return List of store definitions
         */
        public List<StoreDefinition> getCurrentStoreDefinitions(Cluster cluster) {
            return getCurrentStoreDefinitionsExcept(cluster, -1);
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
            final AdminClient mirrorAdminClient = new AdminClient(urlToMirrorFrom);
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
            try {
                return rpcOps.waitForCompletion(nodeId, asyncId, timeoutMs, TimeUnit.MILLISECONDS);
            } catch (AsyncOperationTimeoutException aote) {
                logger.error("Got an AsyncOperationTimeoutException for nodeId " + nodeId + " while waiting for"
                             + " completion of Async Operation ID " + asyncId + ". Will attempt to kill the job"
                             + " and rethrow the original exception afterwards.");
                try {
                    rpcOps.stopAsyncRequest(nodeId, asyncId);
                    logger.info("Successfully killed Async Operation ID " + asyncId);
                } catch (Exception e) {
                    logger.error("Failed to kill Async Operation ID " + asyncId, e);
                }
                throw aote;
            } catch (VoldemortException ve) {
                logger.error("Got a " + ve.getClass().getSimpleName() + " for nodeId " + nodeId + " while waiting for"
                             + " completion of Async Operation ID " + asyncId + ". Bubbling up.");
                throw ve;
            }
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
         * Wrapper to get RO storage format for single store on one node
         * 
         * @param nodeId
         * @param storeName
         * @return
         */
        public String getROStorageFormat(int nodeId, String storeName) {
            Map<String, String> mapStoreToFormat = getROStorageFormat(nodeId,
                                                                      Lists.newArrayList(storeName));
            return mapStoreToFormat.get(storeName);
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
         * This is a wrapper around {@link #getROMaxVersion(java.util.List, int)} where-in
         * we find the max versions on each machine and then return the max of
         * all of them, without tolerating any node failures.
         * 
         * @param storeNames List of all read-only stores
         * @return A map of store-name to their corresponding max version id
         */
        public Map<String, Long> getROMaxVersion(List<String> storeNames) {
            return getROMaxVersion(storeNames, 0);
        }

        /**
         * This is a wrapper around {@link #getROMaxVersion(int, List)} where-in
         * we find the max versions on each machine and then return the max of
         * all of them
         *
         * @param storeNames List of all read-only stores
         * @param maxNodeFailures The maximum number of nodes which can fail to respond
         * @return A map of store-name to their corresponding max version id
         */
        public Map<String, Long> getROMaxVersion(List<String> storeNames, int maxNodeFailures) {
            int nodeFailures = 0;
            Map<String, Long> storeToMaxVersion = Maps.newHashMapWithExpectedSize(storeNames.size());
            for(String storeName: storeNames) {
                storeToMaxVersion.put(storeName, 0L);
            }

            for(Node node: currentCluster.getNodes()) {
                try {
                    Map<String, Long> currentNodeVersions = getROMaxVersion(node.getId(), storeNames);
                    for(String storeName: currentNodeVersions.keySet()) {
                        Long maxVersion = storeToMaxVersion.get(storeName);
                        if(maxVersion != null && maxVersion < currentNodeVersions.get(storeName)) {
                            storeToMaxVersion.put(storeName, currentNodeVersions.get(storeName));
                        }
                    }
                } catch (VoldemortException e) {
                    nodeFailures++;
                    if (nodeFailures > maxNodeFailures) {
                        logger.error("Got an exception while trying to reach node " + node.getId() + ". " +
                                nodeFailures + " node failure(s) so far; maxNodeFailures exceeded, rethrowing.");
                        throw e;
                    } else {
                        logger.warn("Got an exception while trying to reach node " + node.getId() + ". " +
                                nodeFailures + " node failure(s) so far; continuing.", e);
                    }
                }
            }
            return storeToMaxVersion;
        }


        /**
         * Returns the file names of a specific store on one node.
         * 
         * @param nodeId Id of the node to query from
         * @param storeName Name of the store to look up
         * @return A list of file names corresponding to the store and node
         */

        public List<String> getROStorageFileList(int nodeId, String storeName) {
            VAdminProto.GetROStorageFileListResponse.Builder response = getROMetadata(nodeId,
                                                                                      storeName);
            return response.getFileNameList();
        }

        public List<ReadOnlyFileEntry> getROStorageFileMetadata(int nodeId, String storeName) {
            VAdminProto.GetROStorageFileListResponse.Builder response = getROMetadata(nodeId,
                                                                                      storeName);

            List<String> fileNames = response.getFileNameList();
            List<Integer> indexSizes = response.getIndexFileSizeList();
            List<Integer> dataSizes = response.getDataFileSizeList();

            List<ReadOnlyFileEntry> files = Lists.newArrayList();
            if(indexSizes.size() != dataSizes.size()) {
                String errorMessage = " Node returned different counts for data and index" + nodeId
                                      + " dataSize count " + dataSizes.size() + " indexSize count "
                                      + indexSizes.size();
                logger.error(errorMessage);
                throw new VoldemortApplicationException(errorMessage);
            }
            // Server running older version of the Code, which does not have sizes;
            if(indexSizes.size() == 0) {
                for(String file: fileNames) {
                    ReadOnlyFileEntry dataFile = new ReadOnlyFileEntry(file, FileType.DATA);
                    ReadOnlyFileEntry indexFile = new ReadOnlyFileEntry(file, FileType.INDEX);
                    files.add(dataFile);
                    files.add(indexFile);
                }
            } else {
                if(indexSizes.size() != fileNames.size()) {
                    String errorMessage = " Node returned different counts for fileNames and fileSize"
                                          + nodeId + " fileNames count " + fileNames.size()
                                          + " fileSizes count " + indexSizes.size();
                    logger.error(errorMessage);
                    throw new VoldemortApplicationException(errorMessage);
                }
                for(int i = 0; i < fileNames.size(); i++) {
                    String fileName = fileNames.get(i);
                    int dataFileSize = dataSizes.get(i);
                    int indexFileSize = indexSizes.get(i);

                    ReadOnlyFileEntry dataFile = new ReadOnlyFileEntry(fileName,
                                                                       FileType.DATA,
                                                                       dataFileSize);
                    ReadOnlyFileEntry indexFile = new ReadOnlyFileEntry(fileName,
                                                                        FileType.INDEX,
                                                                        indexFileSize);
                    files.add(dataFile);
                    files.add(indexFile);
                }
            }

            return files;
        }

        private VAdminProto.GetROStorageFileListResponse.Builder getROMetadata(int nodeId,
                                                                               String storeName) {
            VAdminProto.GetROStorageFileListRequest.Builder getRORequest = VAdminProto.GetROStorageFileListRequest.newBuilder()
                                                                                                                  .setStoreName(storeName);
            VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                              .setGetRoStorageFileList(getRORequest)
                                                                                              .setType(VAdminProto.AdminRequestType.GET_RO_STORAGE_FILE_LIST)
                                                                                              .build();
            VAdminProto.GetROStorageFileListResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                              adminRequest,
                                                                                              VAdminProto.GetROStorageFileListResponse.newBuilder());
            if(response.hasError()) {
                helperOps.throwException(response.getError());
            }

            return response;

        }

        public List<String> getSupportedROStorageCompressionCodecs() {
            Iterator<Node> nodesIterator = currentCluster.getNodes().iterator();
            VoldemortException lastException = null;
            while (nodesIterator.hasNext()) {
                Node node = nodesIterator.next();
                try {
                    return getSupportedROStorageCompressionCodecs(node.getId());
                } catch (VoldemortException e) {
                    String nextNodeMessage = "";
                    if (nodesIterator.hasNext()) {
                        nextNodeMessage = " Will try next node.";
                    } else {
                        nextNodeMessage = " Will abort, as all nodes failed.";
                    }
                    logger.error("Error while trying to ask " + node.briefToString() +
                            " for its supported compression codec." + nextNodeMessage +
                            " Exception message: " + e.getMessage());
                    lastException = e;
                }
            }
            throw new VoldemortException("Error while trying to ask the cluster for its compression settings. All nodes failed.", lastException);
        }

        public List<String> getSupportedROStorageCompressionCodecs(int nodeId) {
            VAdminProto.GetROStorageCompressionCodecListRequest.Builder getRORequest = VAdminProto.GetROStorageCompressionCodecListRequest.newBuilder();
            VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                                              .setGetRoCompressionCodecList(getRORequest)
                                                                                              .setType(VAdminProto.AdminRequestType.GET_RO_COMPRESSION_CODEC_LIST)
                                                                                              .build();
            VAdminProto.GetROStorageCompressionCodecListResponse.Builder response = rpcOps.sendAndReceive(nodeId,
                                                                                                     adminRequest,
                                                                                                          VAdminProto.GetROStorageCompressionCodecListResponse.newBuilder());
            if(response.hasError()) {
                helperOps.throwException(response.getError());
            }

            return response.getCompressionCodecsList();
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

        public VAdminProto.GetHighAvailabilitySettingsResponse getHighAvailabilitySettings() {
            Iterator<Node> nodesIterator = currentCluster.getNodes().iterator();
            VoldemortException lastException = null;
            while (nodesIterator.hasNext()) {
                Node node = nodesIterator.next();
                try {
                    return getHighAvailabilitySettings(node.getId());
                } catch (VoldemortException e) {
                    String nextNodeMessage = "";
                    if (nodesIterator.hasNext()) {
                        nextNodeMessage = " Will try next node.";
                    } else {
                        nextNodeMessage = " Will abort, as all nodes failed.";
                    }
                    logger.error("Error while trying to ask " + node.briefToString() +
                            " for its HA settings." + nextNodeMessage +
                            " Exception message: " + e.getMessage());
                    lastException = e;
                }
            }
            throw new VoldemortException("Error while trying to ask the cluster for its HA settings. All nodes failed.", lastException);
        }

        public VAdminProto.GetHighAvailabilitySettingsResponse getHighAvailabilitySettings(Integer nodeId) {
            VAdminProto.GetHighAvailabilitySettingsRequest getHighAvailabilitySettingsRequest =
                    VAdminProto.GetHighAvailabilitySettingsRequest.newBuilder().build();
            VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                    .setGetHaSettings(getHighAvailabilitySettingsRequest)
                    .setType(VAdminProto.AdminRequestType.GET_HA_SETTINGS)
                    .build();
            return rpcOps.sendAndReceive(nodeId,
                    adminRequest,
                    VAdminProto.GetHighAvailabilitySettingsResponse.newBuilder()).build();
        }

        /**
          * @return the {@link voldemort.client.protocol.pb.VAdminProto.DisableStoreVersionResponse}
         */
        public VAdminProto.DisableStoreVersionResponse disableStoreVersion(Integer nodeId, String storeName, Long storeVersion, String info) {
            VAdminProto.DisableStoreVersionRequest request = VAdminProto.DisableStoreVersionRequest.newBuilder()
                                                                                                   .setStoreName(storeName)
                                                                                                   .setPushVersion(storeVersion)
                                                                                                   .setInfo(info)
                                                                                                   .build();

            VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                                                                        .setDisableStoreVersion(request)
                                                                        .setType(VAdminProto.AdminRequestType.DISABLE_STORE_VERSION)
                                                                        .build();

            VAdminProto.DisableStoreVersionResponse response = null;
            VAdminProto.DisableStoreVersionResponse.Builder responseBuilder = VAdminProto.DisableStoreVersionResponse.newBuilder();
            try {
                response = rpcOps.sendAndReceive(nodeId,
                                                 adminRequest,
                                                 responseBuilder).build();
            } catch (UnreachableStoreException e) {
                String errorMessage = "Got an UnreachableStoreException while trying to disableStoreVersion on node " +
                        nodeId + ", store " + storeName + ", version " + storeVersion + ". If the node is actually " +
                        "up and merely net-split from us, it might continue serving stale data...";
                logger.warn(errorMessage, e);
                response = responseBuilder.setDisableSuccess(false)
                                          .setInfo(errorMessage)
                                          .setNodeId(nodeId)
                                          .build();
            }

            return response;
        }

        /**
         * @return true if it's still possible to do a swap, false otherwise.
         */
        public boolean handleFailedFetch(List<Integer> failedNodes, String storeName, Long storeVersion, String info) {
            VAdminProto.HandleFetchFailureRequest handleFetchFailureRequest =
                    VAdminProto.HandleFetchFailureRequest.newBuilder().setStoreName(storeName)
                                                                      .setPushVersion(storeVersion)
                                                                      .setInfo(info)
                                                                      .addAllFailedNodes(failedNodes)
                                                                      .build();

            List<Integer> liveNodes = Lists.newArrayList(currentCluster.getNodeIds());
            liveNodes.removeAll(failedNodes);
            if(liveNodes.isEmpty()) {
                return false;
            }
            int randomIndex = new Random().nextInt(liveNodes.size());
            Integer randomNodeId = liveNodes.get(randomIndex);
            Node randomNode = currentCluster.getNodeById(randomNodeId);

            try {
                VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                        .setHandleFetchFailure(handleFetchFailureRequest)
                        .setType(VAdminProto.AdminRequestType.HANDLE_FETCH_FAILURE)
                        .build();

                VAdminProto.HandleFetchFailureResponse response = rpcOps.sendAndReceive(randomNodeId,
                        adminRequest,
                        VAdminProto.HandleFetchFailureResponse.newBuilder()).build();

                if (response.getSwapIsPossible()) {
                    logger.info(randomNode.briefToString() +
                            " returned successful HandleFetchFailureResponse: " + response.getInfo());
                } else {
                    logger.error(randomNode.briefToString() +
                            " returned failed HandleFetchFailureResponse: " + response.getInfo());
                }

                for (VAdminProto.DisableStoreVersionResponse disableStoreVersionResponse: response.getDisableStoreResponsesList()) {
                    Node node = currentCluster.getNodeById(disableStoreVersionResponse.getNodeId());
                    String message = node.briefToString() + ": " + disableStoreVersionResponse.getInfo();
                    if (disableStoreVersionResponse.getDisableSuccess()) {
                        logger.info(message);
                    } else {
                        logger.error(message);
                    }
                }

                return response.getSwapIsPossible();
            } catch (UninitializedMessageException e) {
                // Not printing out the exception in the logs as that is a benign error.
                logger.error(randomNode.briefToString() + " does not support HA (introduced in release 1.9.20), so " +
                        "pushHighAvailability will be DISABLED on cluster: " + currentCluster.getName());
                return false;
            } catch (Exception e) {
                logger.error("Unexpected error while asking " + randomNode.briefToString() +
                        " to HandleFetchFailureRequest.", e);
                return false;
            }
        }
    }

    private static void validateTaskCompleted(int nodeId, Map<Integer, Future> nodeTasks) {
        try {
            nodeTasks.get(nodeId).get();
        } catch (ExecutionException ex) {
            Throwable t = ex.getCause();
            if (t instanceof VoldemortException) {
                throw (VoldemortException) t;
            } else {
                throw new RuntimeException("Unexpected exception from Future", t);
            }
        } catch (InterruptedException ex) {
            throw new VoldemortException("Future task is interrupted for Node" + nodeId, ex);
        }
    }

    public class QuotaManagementOperations {

        private VectorClock makeDenseClock() {
          // FIXME This is a temporary workaround for System store client not
          // being able to do a second insert. We simply generate a super
          // clock that will trump what is on storage
          // But this will not work, if the nodes are ever removed or re-assigned.
          // To complicate the issue further, SystemStore uses one clock for all
          // keys in a file. When you remove nodes, go delete, all version files from the disk
          // otherwise
          return VectorClockUtils.makeClockWithCurrentTime(currentCluster.getNodeIds());
        }

        public void setQuota(final String storeName, final QuotaType quotaType, final long quota,
                ExecutorService executor) {
            
            Map<Integer, Future> nodeTasks = new HashMap<Integer, Future>();

            if(executor != null) {
                for (final Integer id : currentCluster.getNodeIds()) {
                    Runnable task = new Runnable() {
                        @Override
                        public void run() {
                            setQuotaForNode(storeName, quotaType, id, quota);
                        }
                    };
                    Future future = executor.submit(task);
                    nodeTasks.put(id, future);
                }
            }

            RuntimeException lastEx = null;
            List<Integer> failedNodes = new ArrayList<Integer>();
            for (Integer nodeId : currentCluster.getNodeIds()) {
                try {
                    if (executor == null) {
                        setQuotaForNode(storeName, quotaType, nodeId, quota);
                    } else {
                        validateTaskCompleted(nodeId, nodeTasks);
                    }
                } catch(RuntimeException ex) {
                    lastEx = ex;
                    failedNodes.add(nodeId);
                    logger.info("Setting Quota on Store " + storeName + " Type " + quotaType
                                + " value " + quota + " failed. Reason " + ex.getMessage());
                }
            }

            if(lastEx != null) {
                logger.info("Setting Quota failed on Nodes "
                            + Arrays.toString(failedNodes.toArray()));
                throw lastEx;
            }
        }

        public void setQuota(String storeName, QuotaType quotaType, long quota) {
            setQuota(storeName, quotaType, quota, null);
        }

        public void unsetQuota(String storeName, QuotaType quotaType) {
            RuntimeException lastEx = null;

            for (Integer nodeId : currentCluster.getNodeIds()) {
                try {
                    deleteQuotaForNode(storeName, quotaType, nodeId);
                } catch (RuntimeException ex) {
                    lastEx = ex;
                    logger.info("Deleting Quota on Store " + storeName + " Type " + quotaType + "failed", ex);
                }
            }

            if (lastEx != null) {
                throw lastEx;
            }
        }

        public Versioned<String> getQuota(String storeName, QuotaType quotaType) {
            return getQuotaForNode(storeName, quotaType, currentCluster.getNodeIds().iterator().next());
        }

        public Map<String, Versioned<String>> getQuota(List<String> storeNames, QuotaType quotaType) {
            return getQuotaForNode(storeNames, quotaType, currentCluster.getNodeIds().iterator().next());
        }

        public void setQuotaForNode(String storeName, QuotaType quotaType, Integer nodeId, Long quota) {
            ByteArray keyArray = QuotaUtils.getByteArrayKey(storeName, quotaType);
            VectorClock clock = makeDenseClock();
            byte[] valueArray = ByteUtils.getBytes(quota.toString(), "UTF8");
            Versioned<byte[]> value = new Versioned<byte[]>(valueArray, clock);

            NodeValue<ByteArray, byte[]> nodeKeyValue = new NodeValue<ByteArray, byte[]>(nodeId,
                                                                                         keyArray,
                                                                                         value);
            storeOps.putNodeKeyValue(SystemStoreConstants.SystemStoreName.voldsys$_store_quotas.name(),
                                     nodeKeyValue);
        }
        
        public boolean deleteQuotaForNode(String storeName, QuotaType quotaType, Integer nodeId) {
            ByteArray keyArray = QuotaUtils.getByteArrayKey(storeName, quotaType);
            return storeOps.deleteNodeKeyValue(SystemStoreConstants.SystemStoreName.voldsys$_store_quotas.name(),
                                                   nodeId,
                                                   keyArray);
        }

        private Versioned<String> convertToString(List<Versioned<byte[]>> retrievedValue) {
            if (retrievedValue == null || retrievedValue.size() == 0) {
                return null;
            }

            if (retrievedValue.size() > 1) {
                throw new VoldemortApplicationException("More than one value present for same quota "
                        + Arrays.toString(retrievedValue.toArray()));
            }

            try {
                String quotaValue = new String(retrievedValue.get(0).getValue(), "UTF8");
                Version version = retrievedValue.get(0).getVersion();
                return new Versioned<String>(quotaValue, version);
            } catch (UnsupportedEncodingException ex) {
                throw new VoldemortApplicationException("Error converting quota value to String", ex);
            }
        }

        public Versioned<String> getQuotaForNode(String storeName,
                                                 QuotaType quotaType,
                                                 Integer nodeId) {
            ByteArray keyArray = QuotaUtils.getByteArrayKey(storeName, quotaType);
            List<Versioned<byte[]>> valueObj =
                    storeOps.getNodeKey(SystemStoreConstants.SystemStoreName.voldsys$_store_quotas.name(), nodeId,
                            keyArray);

            return convertToString(valueObj);
        }

        public Map<String, Versioned<String>> getQuotaForNode(List<String> storeNames, QuotaType quotaType,
                Integer nodeId) {
            if (storeNames == null || storeNames.size() == 0) {
                throw new IllegalArgumentException("Storenames is a required parameter");
            }

            Map<String, ByteArray> storeToKeysMap = new HashMap<String, ByteArray>();
            for (String storeName : storeNames) {
                ByteArray key = QuotaUtils.getByteArrayKey(storeName, quotaType);
                storeToKeysMap.put(storeName, key);
            }
            
            Map<ByteArray, List<Versioned<byte[]>>> storeToValueMap =
                    storeOps.getAllNodeKeys(SystemStoreConstants.SystemStoreName.voldsys$_store_quotas.name(), nodeId,
                    storeToKeysMap.values());

            Map<String, Versioned<String>> results = new HashMap<String, Versioned<String>>();

            for (Map.Entry<String, ByteArray> storeToKey : storeToKeysMap.entrySet()) {
                String storeName = storeToKey.getKey();
                ByteArray storeKey = storeToKey.getValue();
                
                Versioned<String> storeQuota = null;
                if (storeToValueMap.containsKey(storeKey)) {
                    storeQuota = convertToString(storeToValueMap.get(storeKey));
                }
                results.put(storeName, storeQuota);
            }
            return results;
        }

        /**
         * Reset quota based on number of nodes
         * 
         * @param storeName
         * @param quotaType
         */
        public void rebalanceQuota(String storeName, QuotaType quotaType) {
            Integer totalQuota = null;
            Set<Integer> nodeIds = currentCluster.getNodeIds();
            for(Integer nodeId: nodeIds) {
                Versioned<String> quotaValue = getQuotaForNode(storeName, quotaType, nodeId);
                if(quotaValue != null) {
                    Integer quota = Integer.parseInt(quotaValue.getValue());
                    if(totalQuota == null) {
                        totalQuota = 0;
                    }
                    totalQuota += quota;
                }
            }
            if(totalQuota == null) {
                logger.info("No quota set for " + quotaType.toString() + " of store " + storeName
                            + " ");
            } else {
                long averageQuota = totalQuota / nodeIds.size();
                logger.info("Resetting quota: Store: " + storeName + ", Quota: "
                            + quotaType.toString() + ", Total: " + totalQuota.toString()
                            + ", Average: " + averageQuota);
                setQuota(storeName, quotaType, averageQuota);
            }
        }
        

        /**
         * Reset quota based on number of nodes
         * 
         * @param storeName
         */
        public void rebalanceQuota(String storeName) {
            for(QuotaType quotaType: QuotaType.values()) {
                rebalanceQuota(storeName, quotaType);
            }
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
