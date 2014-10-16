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

package voldemort;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.HttpClient;
import org.apache.log4j.Logger;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import voldemort.client.ClientConfig;
import voldemort.client.RoutingTier;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.AbstractSocketService;
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.server.http.StoreServlet;
import voldemort.server.niosocket.NioSocketService;
import voldemort.server.protocol.RequestHandler;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.server.protocol.SocketRequestHandlerFactory;
import voldemort.server.protocol.admin.AsyncOperationService;
import voldemort.server.socket.SocketService;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.UnreachableStoreException;
import voldemort.store.http.HttpStore;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.slop.Slop;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Helper functions for testing with real server implementations
 * 
 * 
 */
public class ServerTestUtils {

    private static final Logger logger = Logger.getLogger(ServerTestUtils.class.getName());

    public static StoreRepository getStores(String storeName, String clusterXml, String storesXml) {
        StoreRepository repository = new StoreRepository();
        Store<ByteArray, byte[], byte[]> store = new InMemoryStorageEngine<ByteArray, byte[], byte[]>(storeName);
        repository.addLocalStore(store);
        repository.addRoutedStore(store);

        // create new metadata store.
        MetadataStore metadata = createMetadataStore(new ClusterMapper().readCluster(new StringReader(clusterXml)),
                                                     new StoreDefinitionsMapper().readStoreList(new StringReader(storesXml)));
        repository.addLocalStore(metadata);
        return repository;
    }

    public static VoldemortConfig getVoldemortConfig() {
        File temp = TestUtils.createTempDir();
        VoldemortConfig config = new VoldemortConfig(0, temp.getAbsolutePath());
        new File(config.getMetadataDirectory()).mkdir();
        return config;
    }

    public static AbstractSocketService getSocketService(boolean useNio,
                                                         String clusterXml,
                                                         String storesXml,
                                                         String storeName,
                                                         int port) {
        RequestHandlerFactory factory = getSocketRequestHandlerFactory(clusterXml,
                                                                       storesXml,
                                                                       getStores(storeName,
                                                                                 clusterXml,
                                                                                 storesXml));
        return getSocketService(useNio, factory, port, 5, 10, 10000);
    }

    public static RequestHandlerFactory getSocketRequestHandlerFactory(String clusterXml,
                                                                       String storesXml,
                                                                       StoreRepository storeRepository) {

        return new SocketRequestHandlerFactory(null,
                                               storeRepository,
                                               createMetadataStore(new ClusterMapper().readCluster(new StringReader(clusterXml)),
                                                                   new StoreDefinitionsMapper().readStoreList(new StringReader(storesXml))),
                                               null,
                                               null,
                                               null,
                                               null);
    }

    public static AbstractSocketService getSocketService(boolean useNio,
                                                         RequestHandlerFactory requestHandlerFactory,
                                                         int port,
                                                         int coreConnections,
                                                         int maxConnections,
                                                         int bufferSize) {
        AbstractSocketService socketService = null;

        if(useNio) {
            socketService = new NioSocketService(requestHandlerFactory,
                                                 port,
                                                 bufferSize,
                                                 coreConnections,
                                                 "client-request-service",
                                                 false,
                                                 -1);
        } else {
            socketService = new SocketService(requestHandlerFactory,
                                              port,
                                              coreConnections,
                                              maxConnections,
                                              bufferSize,
                                              "client-request-service",
                                              false);
        }

        return socketService;
    }

    public static Store<ByteArray, byte[], byte[]> getSocketStore(SocketStoreFactory storeFactory,
                                                                  String storeName,
                                                                  int port) {
        return getSocketStore(storeFactory, storeName, port, RequestFormatType.VOLDEMORT_V1);
    }

    public static Store<ByteArray, byte[], byte[]> getSocketStore(SocketStoreFactory storeFactory,
                                                                  String storeName,
                                                                  int port,
                                                                  RequestFormatType type) {
        return getSocketStore(storeFactory, storeName, "localhost", port, type);
    }

    public static Store<ByteArray, byte[], byte[]> getSocketStore(SocketStoreFactory storeFactory,
                                                                  String storeName,
                                                                  String host,
                                                                  int port,
                                                                  RequestFormatType type) {
        return getSocketStore(storeFactory, storeName, host, port, type, false);
    }

    public static Store<ByteArray, byte[], byte[]> getSocketStore(SocketStoreFactory storeFactory,
                                                                  String storeName,
                                                                  String host,
                                                                  int port,
                                                                  RequestFormatType type,
                                                                  boolean isRouted) {
        return getSocketStore(storeFactory, storeName, host, port, type, isRouted, false);
    }

    public static Store<ByteArray, byte[], byte[]> getSocketStore(SocketStoreFactory storeFactory,
                                                                  String storeName,
                                                                  String host,
                                                                  int port,
                                                                  RequestFormatType type,
                                                                  boolean isRouted,
                                                                  boolean ignoreChecks) {
        RequestRoutingType requestRoutingType = RequestRoutingType.getRequestRoutingType(isRouted,
                                                                                         ignoreChecks);
        return storeFactory.create(storeName, host, port, type, requestRoutingType);
    }

    public static Context getJettyServer(String clusterXml,
                                         String storesXml,
                                         String storeName,
                                         RequestFormatType requestFormat,
                                         int port) throws Exception {
        StoreRepository repository = getStores(storeName, clusterXml, storesXml);

        // initialize servlet
        Server server = new Server(port);
        server.setSendServerVersion(false);
        Context context = new Context(server, "/", Context.NO_SESSIONS);

        RequestHandler handler = getSocketRequestHandlerFactory(clusterXml, storesXml, repository).getRequestHandler(requestFormat);
        context.addServlet(new ServletHolder(new StoreServlet(handler)), "/stores");
        server.start();
        return context;
    }

    public static HttpStore getHttpStore(String storeName,
                                         RequestFormatType format,
                                         int port,
                                         final HttpClient httpClient) {
        return new HttpStore(storeName,
                             "localhost",
                             port,
                             httpClient,
                             new RequestFormatFactory().getRequestFormat(format),
                             false);
    }

    /**
     * Return a free port as chosen by new ServerSocket(0).
     * 
     * There is no guarantee that the port returned will be free when the caller
     * attempts to bind to the port. This is a time-of-check-to-time-of-use
     * (TOCTOU) issue that cannot be avoided.
     */
    public static int findFreePort() {
        return findFreePorts(1)[0];
    }

    /**
     * Return an array of free ports as chosen by new ServerSocket(0)
     * 
     * There is no guarantee that the ports returned will be free when the
     * caller attempts to bind to some returned port. This is a
     * time-of-check-to-time-of-use (TOCTOU) issue that cannot be avoided.
     */
    public static int[] findFreePorts(int n) {
        logger.info("findFreePorts cannot guarantee that ports identified as free will still be free when used. This is effectively a TOCTOU issue. Expect intermittent BindException when \"free\" ports are used.");
        int[] ports = new int[n];
        ServerSocket[] sockets = new ServerSocket[n];
        try {
            for(int i = 0; i < n; i++) {
                sockets[i] = new ServerSocket(0);
                ports[i] = sockets[i].getLocalPort();
            }
            return ports;
        } catch(IOException e) {
            throw new RuntimeException(e);
        } finally {
            for(int i = 0; i < n; i++) {
                try {
                    if(sockets[i] != null)
                        sockets[i].close();
                } catch(IOException e) {}
            }
        }
    }

    public static Cluster getLocalCluster(int numberOfNodes) {
        return getLocalCluster(numberOfNodes, findFreePorts(3 * numberOfNodes), null);
    }

    public static Cluster getLocalCluster(int numberOfNodes, int[][] partitionMap) {
        return getLocalCluster(numberOfNodes, findFreePorts(3 * numberOfNodes), partitionMap);
    }

    public static Cluster getLocalCluster(int numberOfNodes, int[] ports, int[][] partitionMap) {
        if(3 * numberOfNodes != ports.length)
            throw new IllegalArgumentException(3 * numberOfNodes + " ports required but only "
                                               + ports.length + " given.");
        List<Node> nodes = new ArrayList<Node>();
        for(int i = 0; i < numberOfNodes; i++) {
            List<Integer> partitions = ImmutableList.of(i);
            if(null != partitionMap) {
                partitions = new ArrayList<Integer>(partitionMap[i].length);
                for(int p: partitionMap[i]) {
                    partitions.add(p);
                }
            }

            nodes.add(new Node(i,
                               "localhost",
                               ports[3 * i],
                               ports[3 * i + 1],
                               ports[3 * i + 2],
                               partitions));
        }

        return new Cluster("test-cluster", nodes);
    }

    public static Cluster getLocalNonContiguousNodesCluster(int[] nodeIds, int[][] partitionMap) {
        return getLocalNonContiguousNodesCluster(nodeIds,
                                                 findFreePorts(3 * nodeIds.length),
                                                 partitionMap);
    }

    public static Cluster getLocalNonContiguousNodesCluster(int[] nodeIds,
                                                            int[] ports,
                                                            int[][] partitionMap) {
        if(3 * nodeIds.length != ports.length)
            throw new IllegalArgumentException(3 * nodeIds.length + " ports required but only "
                                               + ports.length + " given.");
        List<Node> nodes = new ArrayList<Node>();
        for(int i = 0; i < nodeIds.length; i++) {
            List<Integer> partitions = ImmutableList.of(i);
            if(null != partitionMap) {
                partitions = new ArrayList<Integer>(partitionMap[i].length);
                for(int p: partitionMap[i]) {
                    partitions.add(p);
                }
            }

            nodes.add(new Node(nodeIds[i],
                               "localhost",
                               ports[3 * i],
                               ports[3 * i + 1],
                               ports[3 * i + 2],
                               partitions));
        }

        return new Cluster("test-cluster", nodes);
    }

    /**
     * Update a cluster by replacing the specified server with a new host, i.e.
     * new ports since they are all localhost
     * 
     * @param original The original cluster to be updated
     * @param serverIds The ids of the server to be replaced with new hosts
     * @return updated cluster
     */
    public static Cluster updateClusterWithNewHost(Cluster original, int... serverIds) {
        int highestPortInuse = 0;

        for(Node node: original.getNodes()) {
            int nodeMaxPort = 0;
            nodeMaxPort = Math.max(nodeMaxPort, node.getAdminPort());
            nodeMaxPort = Math.max(nodeMaxPort, node.getHttpPort());
            nodeMaxPort = Math.max(nodeMaxPort, node.getSocketPort());
            highestPortInuse = Math.max(highestPortInuse, nodeMaxPort);
        }

        Set<Integer> newNodesSet = new HashSet<Integer>(serverIds.length);
        for(int id: serverIds) {
            newNodesSet.add(id);
        }

        List<Node> newNodeList = new ArrayList<Node>(serverIds.length);
        for(Node node: original.getNodes()) {
            if(newNodesSet.contains(node.getId())) {
                node = new Node(node.getId(),
                                "localhost",
                                ++highestPortInuse,
                                ++highestPortInuse,
                                ++highestPortInuse,
                                node.getPartitionIds());
            }
            newNodeList.add(node);
        }

        return new Cluster(original.getName(), newNodeList);
    }

    /**
     * Returns a list of zones with their proximity list being in increasing
     * order
     * 
     * @param numberOfZones The number of zones to return
     * @return List of zones
     */
    public static List<Zone> getZonesFromZoneIds(int[] zoneIds) {
        List<Zone> zones = Lists.newArrayList();
        Set<Integer> zoneIdsSet = new HashSet<Integer>();
        for(int i: zoneIds) {
            zoneIdsSet.add(i);
        }
        Set<Integer> removeSet = new HashSet<Integer>();
        for(int i = 0; i < zoneIds.length; i++) {
            removeSet.add(zoneIds[i]);
            zones.add(new Zone(zoneIds[i], Lists.newLinkedList(Sets.symmetricDifference(zoneIdsSet,
                                                                                        removeSet))));
            removeSet.clear();
        }
        return zones;
    }

    /**
     * Given zone ids, this method returns a list of zones with their proximity
     * list
     * 
     * @param list of zone ids
     * @return List of zones
     */
    public static List<Zone> getZones(int numberOfZones) {
        List<Zone> zones = Lists.newArrayList();
        for(int i = 0; i < numberOfZones; i++) {
            LinkedList<Integer> proximityList = Lists.newLinkedList();
            int zoneId = i + 1;
            for(int j = 0; j < numberOfZones; j++) {
                if(zoneId % numberOfZones != i) {
                    proximityList.add(zoneId % numberOfZones);
                }
                zoneId++;
            }
            zones.add(new Zone(i, proximityList));
        }
        return zones;
    }

    /**
     * Returns a cluster with <b>numberOfNodes</b> nodes in <b>numberOfZones</b>
     * zones. It is important that <b>numberOfNodes</b> be divisible by
     * <b>numberOfZones</b>
     * 
     * @param numberOfNodes Number of nodes in the cluster
     * @param partitionsPerNode Number of partitions in one node
     * @param numberOfZones Number of zones
     * @return Cluster
     */
    public static Cluster getLocalCluster(int numberOfNodes,
                                          int partitionsPerNode,
                                          int numberOfZones) {

        if(numberOfZones > 0 && numberOfNodes > 0 && numberOfNodes % numberOfZones != 0) {
            throw new VoldemortException("The number of nodes (" + numberOfNodes
                                         + ") is not divisible by number of zones ("
                                         + numberOfZones + ")");
        }

        int[] ports = findFreePorts(3 * numberOfNodes);

        List<Integer> partitions = Lists.newArrayList();

        for(int i = 0; i < partitionsPerNode * numberOfNodes; i++)
            partitions.add(i);

        Collections.shuffle(partitions);

        // Generate nodes
        int numberOfNodesPerZone = numberOfNodes / numberOfZones;
        List<Node> nodes = new ArrayList<Node>();
        for(int i = 0; i < numberOfNodes; i++) {
            nodes.add(new Node(i,
                               "localhost",
                               ports[3 * i],
                               ports[3 * i + 1],
                               ports[3 * i + 2],
                               i / numberOfNodesPerZone,
                               partitions.subList(partitionsPerNode * i, partitionsPerNode * i
                                                                         + partitionsPerNode)));
        }

        // Generate zones
        if(numberOfZones > 1) {
            List<Zone> zones = getZones(numberOfZones);
            return new Cluster("cluster", nodes, zones);
        } else {
            return new Cluster("cluster", nodes);
        }
    }

    public static Cluster getLocalZonedCluster(int numberOfNodes,
                                               int numberOfZones,
                                               int[] nodeToZoneMapping,
                                               int[][] partitionMapping) {
        return getLocalZonedCluster(numberOfNodes,
                                    numberOfZones,
                                    nodeToZoneMapping,
                                    partitionMapping,
                                    findFreePorts(3 * numberOfNodes));
    }

    /**
     * Returns a cluster with <b>numberOfNodes</b> nodes in <b>numberOfZones</b>
     * zones. It is important that <b>numberOfNodes</b> be divisible by
     * <b>numberOfZones</b>
     * 
     * @param numberOfNodes Number of nodes in the cluster
     * @param partitionsPerNode Number of partitions in one node
     * @param numberOfZones Number of zones
     * @return Cluster
     */
    public static Cluster getLocalZonedCluster(int numberOfNodes,
                                               int numberOfZones,
                                               int[] nodeToZoneMapping,
                                               int[][] partitionMapping,
                                               int[] ports) {

        List<Node> nodes = new ArrayList<Node>();
        for(int i = 0; i < numberOfNodes; i++) {

            List<Integer> partitions = new ArrayList<Integer>(partitionMapping[i].length);
            for(int p: partitionMapping[i]) {
                partitions.add(p);
            }

            nodes.add(new Node(i,
                               "localhost",
                               ports[3 * i],
                               ports[3 * i + 1],
                               ports[3 * i + 2],
                               nodeToZoneMapping[i],
                               partitions));
        }

        // Generate zones
        List<Zone> zones = getZones(numberOfZones);
        return new Cluster("cluster", nodes, zones);
    }

    /**
     * Returns a cluster with <b>numberOfZones</b> zones. The array
     * <b>nodesPerZone<b> indicates how many nodes are in each of the zones. The
     * a nodes in <b>numberOfZones</b> zones. It is important that
     * <b>numberOfNodes</b> be divisible by <b>numberOfZones</b>
     * 
     * Does
     * 
     * @param numberOfZones The number of zones in the cluster.
     * @param nodeIdsPerZone An array of size <b>numberOfZones<b> in which each
     *        internal array is a node ID.
     * @param partitionMap An array of size total number of nodes (derived from
     *        <b>nodesPerZone<b> that indicates the specific partitions on each
     *        node.
     * @return
     */
    // TODO: Method should eventually accept a list of ZoneIds so that
    // non-contig zone Ids can be tested.
    /*-
    public static Cluster getLocalZonedCluster(int numberOfZones,
                                               int[][] nodeIdsPerZone,
                                               int[][] partitionMap) {
        

        if(numberOfZones < 1) {
            throw new VoldemortException("The number of zones must be positive (" + numberOfZones
                                         + ")");
        }
        if(nodeIdsPerZone.length != numberOfZones) {
            throw new VoldemortException("Mismatch between numberOfZones (" + numberOfZones
                                         + ") and size of nodesPerZone array ("
                                         + nodeIdsPerZone.length + ").");
        }

        int numNodes = 0;
        for(int nodeIdsInZone[]: nodeIdsPerZone) {
            numNodes += nodeIdsInZone.length;
        }
        if(partitionMap.length != numNodes) {
            throw new VoldemortException("Mismatch between numNodes (" + numNodes
                                         + ") and size of partitionMap array (" + partitionMap
                                         + ").");
        }

        // Generate nodes
        List<Node> nodes = new ArrayList<Node>();
        int partitionMapOffset = 0;
        for(int zoneId = 0; zoneId < numberOfZones; zoneId++) {
            for(int nodeId: nodeIdsPerZone[zoneId]) {
                List<Integer> partitions = new ArrayList<Integer>(partitionMap[nodeId].length);
                for(int p: partitionMap[partitionMapOffset]) {
                    partitions.add(p);
                }
                nodes.add(new Node(nodeId,
                                   "node-" + nodeId,
                                   64000,
                                   64001,
                                   64002,
                                   zoneId,
                                   partitions));
                partitionMapOffset++;
            }
        }

        List<Zone> zones = Lists.newArrayList();
        for(int i = 0; i < numberOfZones; i++) {
            LinkedList<Integer> proximityList = Lists.newLinkedList();
            int zoneId = i + 1;
            for(int j = 0; j < numberOfZones; j++) {
                proximityList.add(zoneId % numberOfZones);
                zoneId++;
            }
            zones.add(new Zone(i, proximityList));
        }
        return new Cluster("cluster", nodes, zones);
    }
     */

    public static Cluster getLocalZonedCluster(int numberOfZones,
                                               int[][] nodeIdsPerZone,
                                               int[][] partitionMap,
                                               int[] ports) {

        if(numberOfZones < 1) {
            throw new VoldemortException("The number of zones must be positive (" + numberOfZones
                                         + ")");
        }
        if(nodeIdsPerZone.length != numberOfZones) {
            throw new VoldemortException("Mismatch between numberOfZones (" + numberOfZones
                                         + ") and size of nodesPerZone array ("
                                         + nodeIdsPerZone.length + ").");
        }

        int numNodes = 0;
        for(int nodeIdsInZone[]: nodeIdsPerZone) {
            numNodes += nodeIdsInZone.length;
        }
        if(partitionMap.length != numNodes) {
            throw new VoldemortException("Mismatch between numNodes (" + numNodes
                                         + ") and size of partitionMap array (" + partitionMap
                                         + ").");
        }

        // Generate nodes
        List<Node> nodes = new ArrayList<Node>();
        int offset = 0;
        for(int zoneId = 0; zoneId < numberOfZones; zoneId++) {
            for(int nodeId: nodeIdsPerZone[zoneId]) {
                List<Integer> partitions = new ArrayList<Integer>(partitionMap[nodeId].length);
                for(int p: partitionMap[offset]) {
                    partitions.add(p);
                }
                nodes.add(new Node(nodeId,
                                   "localhost",
                                   ports[nodeId * 3],
                                   ports[nodeId * 3 + 1],
                                   ports[nodeId * 3 + 2],
                                   zoneId,
                                   partitions));
                offset++;
            }
        }

        List<Zone> zones = getZones(numberOfZones);
        return new Cluster("cluster", nodes, zones);
    }

    public static Cluster getLocalNonContiguousZonedCluster(int[] zoneIds,
                                                            int[][] nodeIdsPerZone,
                                                            int[][] partitionMap,
                                                            int[] ports) {

        int numberOfZones = zoneIds.length;
        if(numberOfZones < 1) {
            throw new VoldemortException("The number of zones must be positive (" + numberOfZones
                                         + ")");
        }
        if(nodeIdsPerZone.length != numberOfZones) {
            throw new VoldemortException("Mismatch between numberOfZones (" + numberOfZones
                                         + ") and size of nodesPerZone array ("
                                         + nodeIdsPerZone.length + ").");
        }

        int numNodes = 0;
        for(int nodeIdsInZone[]: nodeIdsPerZone) {
            numNodes += nodeIdsInZone.length;
        }
        if(partitionMap.length != numNodes) {
            throw new VoldemortException("Mismatch between numNodes (" + numNodes
                                         + ") and size of partitionMap array (" + partitionMap
                                         + ").");
        }

        // Generate nodes
        List<Node> nodes = new ArrayList<Node>();
        int partitionOffset = 0;
        int zoneOffset = 0;
        for(int zoneId: zoneIds) {
            for(int nodeId: nodeIdsPerZone[zoneOffset]) {
                List<Integer> partitions = new ArrayList<Integer>(partitionMap[partitionOffset].length);
                for(int p: partitionMap[partitionOffset]) {
                    partitions.add(p);
                }
                nodes.add(new Node(nodeId,
                                   "localhost",
                                   ports[nodeId * 3],
                                   ports[nodeId * 3 + 1],
                                   ports[nodeId * 3 + 2],
                                   zoneId,
                                   partitions));
                partitionOffset++;
            }
            zoneOffset++;
        }

        List<Zone> zones = getZonesFromZoneIds(zoneIds);
        return new Cluster("cluster", nodes, zones);
    }

    public static Node getLocalNode(int nodeId, List<Integer> partitions) {
        int[] ports = findFreePorts(3);
        return new Node(nodeId, "localhost", ports[0], ports[1], ports[2], partitions);
    }

    public static MetadataStore createMetadataStore(Cluster cluster, List<StoreDefinition> storeDefs) {
        Store<String, String, String> innerStore = new InMemoryStorageEngine<String, String, String>("inner-store");
        innerStore.put(MetadataStore.CLUSTER_KEY,
                       new Versioned<String>(new ClusterMapper().writeCluster(cluster)),
                       null);
        innerStore.put(MetadataStore.STORES_KEY,
                       new Versioned<String>(new StoreDefinitionsMapper().writeStoreList(storeDefs)),
                       null);

        return new MetadataStore(innerStore, 0);
    }

    public static MetadataStore createMetadataStore(Cluster cluster,
                                                    List<StoreDefinition> storeDefs,
                                                    int nodeId) {
        Store<String, String, String> innerStore = new InMemoryStorageEngine<String, String, String>("inner-store");
        innerStore.put(MetadataStore.CLUSTER_KEY,
                       new Versioned<String>(new ClusterMapper().writeCluster(cluster)),
                       null);
        innerStore.put(MetadataStore.STORES_KEY,
                       new Versioned<String>(new StoreDefinitionsMapper().writeStoreList(storeDefs)),
                       null);

        return new MetadataStore(innerStore, nodeId);
    }

    public static List<StoreDefinition> getStoreDefs(int numStores) {
        List<StoreDefinition> defs = new ArrayList<StoreDefinition>();
        SerializerDefinition serDef = new SerializerDefinition("string");
        for(int i = 0; i < numStores; i++)
            defs.add(new StoreDefinitionBuilder().setName("test" + i)
                                                 .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                                 .setKeySerializer(serDef)
                                                 .setValueSerializer(serDef)
                                                 .setRoutingPolicy(RoutingTier.SERVER)
                                                 .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                 .setReplicationFactor(2)
                                                 .setPreferredReads(1)
                                                 .setRequiredReads(1)
                                                 .setPreferredWrites(1)
                                                 .setRequiredWrites(1)
                                                 .build());
        return defs;
    }

    public static StoreDefinition getStoreDef(String storeName,
                                              int replicationFactor,
                                              int preads,
                                              int rreads,
                                              int pwrites,
                                              int rwrites,
                                              String strategyType) {
        SerializerDefinition serDef = new SerializerDefinition("string");
        return new StoreDefinitionBuilder().setName(storeName)
                                           .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                           .setKeySerializer(serDef)
                                           .setValueSerializer(serDef)
                                           .setRoutingPolicy(RoutingTier.SERVER)
                                           .setRoutingStrategyType(strategyType)
                                           .setReplicationFactor(replicationFactor)
                                           .setPreferredReads(preads)
                                           .setRequiredReads(rreads)
                                           .setPreferredWrites(pwrites)
                                           .setRequiredWrites(rwrites)
                                           .build();
    }

    public static StoreDefinition getStoreDef(String storeName,
                                              int preads,
                                              int rreads,
                                              int pwrites,
                                              int rwrites,
                                              int zonereads,
                                              int zonewrites,
                                              HashMap<Integer, Integer> zoneReplicationFactor,
                                              HintedHandoffStrategyType hhType,
                                              String strategyType) {
        SerializerDefinition serDef = new SerializerDefinition("string");
        int replicationFactor = 0;
        for(Integer repFac: zoneReplicationFactor.values()) {
            replicationFactor += repFac;
        }
        return new StoreDefinitionBuilder().setName(storeName)
                                           .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                           .setKeySerializer(serDef)
                                           .setValueSerializer(serDef)
                                           .setRoutingPolicy(RoutingTier.SERVER)
                                           .setRoutingStrategyType(strategyType)
                                           .setPreferredReads(preads)
                                           .setRequiredReads(rreads)
                                           .setHintedHandoffStrategy(hhType)
                                           .setZoneCountReads(zonereads)
                                           .setZoneCountWrites(zonewrites)
                                           .setReplicationFactor(replicationFactor)
                                           .setZoneReplicationFactor(zoneReplicationFactor)
                                           .setPreferredWrites(pwrites)
                                           .setRequiredWrites(rwrites)
                                           .build();
    }

    public static HashMap<ByteArray, byte[]> createRandomKeyValuePairs(int numKeys) {
        HashMap<ByteArray, byte[]> map = new HashMap<ByteArray, byte[]>();
        for(int cnt = 0; cnt <= numKeys; cnt++) {
            int keyInt = (int) (Math.random() * 100000);
            ByteArray key = new ByteArray(ByteUtils.getBytes("" + keyInt, "UTF-8"));
            byte[] value = ByteUtils.getBytes("value-" + keyInt, "UTF-8");

            map.put(key, value);
        }

        return map;
    }

    public static List<Versioned<Slop>> createRandomSlops(int nodeId,
                                                          int numKeys,
                                                          String... storeNames) {
        return createRandomSlops(nodeId, numKeys, true, storeNames);

    }

    public static List<Versioned<Slop>> createRandomSlops(int nodeId,
                                                          int numKeys,
                                                          boolean generateTwice,
                                                          String... storeNames) {
        List<Versioned<Slop>> slops = new ArrayList<Versioned<Slop>>();

        for(int cnt = 0; cnt < numKeys; cnt++) {
            int storeId = (int) Math.round(Math.random() * (storeNames.length - 1));
            int operation = (int) Math.round(Math.random() + 1);

            Slop.Operation operationType;
            if(operation == 1)
                operationType = Slop.Operation.PUT;
            else
                operationType = Slop.Operation.DELETE;

            long keyInt = (long) (Math.random() * 1000000000L);
            ByteArray key = new ByteArray(ByteUtils.getBytes("" + keyInt, "UTF-8"));
            byte[] value = ByteUtils.getBytes("value-" + keyInt, "UTF-8");

            Versioned<Slop> versioned = Versioned.value(new Slop(storeNames[storeId],
                                                                 operationType,
                                                                 key,
                                                                 value,
                                                                 null,
                                                                 nodeId,
                                                                 new Date()));
            slops.add(versioned);
            if(generateTwice) {
                // Adding twice so as check if ObsoleteVersionExceptions are
                // swallowed correctly
                slops.add(versioned);
            }
        }

        return slops;
    }

    public static HashMap<String, String> createRandomKeyValueString(int numKeys) {
        HashMap<String, String> map = new HashMap<String, String>();
        for(int cnt = 0; cnt <= numKeys; cnt++) {
            int keyInt = (int) (Math.random() * 100000);
            map.put("" + keyInt, "value-" + keyInt);
        }

        return map;
    }

    public static VoldemortConfig createServerConfigWithDefs(boolean useNio,
                                                             int nodeId,
                                                             String baseDir,
                                                             Cluster cluster,
                                                             List<StoreDefinition> stores,
                                                             Properties properties)
            throws IOException {

        File clusterXml = new File(TestUtils.createTempDir(), "cluster.xml");
        File storesXml = new File(TestUtils.createTempDir(), "stores.xml");

        ClusterMapper clusterMapper = new ClusterMapper();
        StoreDefinitionsMapper storeDefMapper = new StoreDefinitionsMapper();

        FileWriter writer = new FileWriter(clusterXml);
        writer.write(clusterMapper.writeCluster(cluster));
        writer.close();

        writer = new FileWriter(storesXml);
        writer.write(storeDefMapper.writeStoreList(stores));
        writer.close();

        return createServerConfig(useNio,
                                  nodeId,
                                  baseDir,
                                  clusterXml.getAbsolutePath(),
                                  storesXml.getAbsolutePath(),
                                  properties);

    }

    public static VoldemortConfig createServerConfig(boolean useNio,
                                                     int nodeId,
                                                     String baseDir,
                                                     String clusterFile,
                                                     String storeFile,
                                                     Properties properties) throws IOException {
        Props props = new Props();
        props.put("node.id", nodeId);
        props.put("voldemort.home", baseDir + "/node-" + nodeId);
        props.put("bdb.cache.size", 1 * 1024 * 1024);
        props.put("jmx.enable", "false");
        props.put("enable.mysql.engine", "true");
        props.loadProperties(properties);

        VoldemortConfig config = new VoldemortConfig(props);
        config.setMysqlDatabaseName("voldemort");
        config.setMysqlUsername("voldemort");
        config.setMysqlPassword("voldemort");
        config.setStreamMaxReadBytesPerSec(10 * 1000 * 1000);
        config.setStreamMaxWriteBytesPerSec(10 * 1000 * 1000);

        config.setUseNioConnector(useNio);

        // clean and reinit metadata dir.
        File tempDir = new File(config.getMetadataDirectory());
        tempDir.mkdirs();
        tempDir.deleteOnExit();

        File tempDir2 = new File(config.getDataDirectory());
        tempDir2.mkdirs();
        tempDir2.deleteOnExit();

        // copy cluster.xml / stores.xml to temp metadata dir.
        if(null != clusterFile)
            FileUtils.copyFile(new File(clusterFile),
                               new File(tempDir.getAbsolutePath() + File.separatorChar
                                        + "cluster.xml"));
        if(null != storeFile)
            FileUtils.copyFile(new File(storeFile), new File(tempDir.getAbsolutePath()
                                                             + File.separatorChar + "stores.xml"));

        return config;
    }

    public static AdminClient getAdminClient(Cluster cluster) {

        AdminClientConfig config = new AdminClientConfig();
        return new AdminClient(cluster, config, new ClientConfig());
    }

    public static AdminClient getAdminClient(String bootstrapURL) {
        AdminClientConfig config = new AdminClientConfig();
        return new AdminClient(bootstrapURL, config, new ClientConfig());
    }

    public static RequestHandlerFactory getSocketRequestHandlerFactory(StoreRepository repository) {
        return new SocketRequestHandlerFactory(null, repository, null, null, null, null, null);
    }

    public static void stopVoldemortServer(VoldemortServer server) throws IOException {
        try {
            server.stop();
        } finally {
            FileUtils.deleteDirectory(new File(server.getVoldemortConfig().getVoldemortHome()));
        }
    }

    /**
     * Starts a Voldemort server for testing purposes.
     * 
     * Unless the ports passed in via cluster are guaranteed to be available,
     * this method is susceptible to BindExceptions in VoldemortServer.start().
     * (And, there is no good way of guaranteeing that ports will be available,
     * so...)
     * 
     * The method {@link ServerTestUtils#startVoldemortCluster} should be used
     * in preference to this method.}
     * 
     * @param socketStoreFactory
     * @param config
     * @param cluster
     * @return
     */
    public static VoldemortServer startVoldemortServer(SocketStoreFactory socketStoreFactory,
                                                       VoldemortConfig config,
                                                       Cluster cluster) throws BindException {

        // TODO: Some tests that use this method fail intermittently with the
        // following output:
        //
        // A successor version version() to this version() exists for key
        // cluster.xml
        // voldemort.versioning.ObsoleteVersionException: A successor version
        // version() to this version() exists for key cluster.xml"
        //
        // Need to trace through the constructor VoldemortServer(VoldemortConfig
        // config, Cluster cluster) to understand how this error is possible,
        // and why it only happens intermittently.
        VoldemortServer server = new VoldemortServer(config, cluster);
        try {
            server.start();
        } catch(VoldemortException ve) {
            if(ve.getCause() instanceof BindException) {
                ve.printStackTrace();
                throw new BindException(ve.getMessage());
            } else {
                throw ve;
            }
        }

        ServerTestUtils.waitForServerStart(socketStoreFactory, server.getIdentityNode());
        // wait till server starts or throw exception
        return server;
    }

    public static VoldemortServer startVoldemortServer(SocketStoreFactory socketStoreFactory,
                                                       VoldemortConfig config) {
        VoldemortServer server = new VoldemortServer(config);
        server.start();

        ServerTestUtils.waitForServerStart(socketStoreFactory, server.getIdentityNode());
        // wait till server start or throw exception
        return server;
    }

    public static void waitForServerStart(SocketStoreFactory socketStoreFactory, Node node) {
        boolean success = false;
        int retries = 10;
        Store<ByteArray, ?, ?> store = null;
        while(retries-- > 0 && !success) {
            store = ServerTestUtils.getSocketStore(socketStoreFactory,
                                                   MetadataStore.METADATA_STORE_NAME,
                                                   node.getSocketPort());
            try {
                store.get(new ByteArray(MetadataStore.CLUSTER_KEY.getBytes()), null);
                success = true;
            } catch(UnreachableStoreException e) {
                store.close();
                store = null;
                System.out.println("UnreachableSocketStore sleeping will try again " + retries
                                   + " times.");
                try {
                    Thread.sleep(1000);
                } catch(InterruptedException e1) {
                    // ignore
                }
            }
        }

        store.close();
        if(!success)
            throw new RuntimeException("Failed to connect with server:" + node);
    }

    /***
     * 
     * 
     * NOTE: This relies on the current behavior of the AsyncOperationService to
     * remove an operation if an explicit isComplete() is invoked. If/When that
     * is changed, this method will always block upto timeoutMs & return
     * 
     * @param server
     * @param asyncOperationPattern substring to match with the operation
     *        description
     * @param timeoutMs
     * @return
     */
    public static boolean waitForAsyncOperationOnServer(VoldemortServer server,
                                                        String asyncOperationPattern,
                                                        long timeoutMs) {
        long endTimeMs = System.currentTimeMillis() + timeoutMs;
        AsyncOperationService service = server.getAsyncRunner();
        List<Integer> matchingOperationIds = null;
        // wait till the atleast one matching operation shows up
        while(System.currentTimeMillis() < endTimeMs) {
            matchingOperationIds = service.getMatchingAsyncOperationList(asyncOperationPattern,
                                                                         true);
            if(matchingOperationIds.size() > 0) {
                break;
            }
        }
        // now wait for those operations to complete
        while(System.currentTimeMillis() < endTimeMs) {
            List<Integer> completedOps = new ArrayList<Integer>(matchingOperationIds.size());
            for(Integer op: matchingOperationIds) {
                if(service.isComplete(op)) {
                    completedOps.add(op);
                }
            }
            matchingOperationIds.removeAll(completedOps);
            if(matchingOperationIds.size() == 0) {
                return false;
            }
        }
        return false;
    }

    protected static Cluster internalStartVoldemortCluster(int numServers,
                                                           VoldemortServer[] voldemortServers,
                                                           int[][] partitionMap,
                                                           SocketStoreFactory socketStoreFactory,
                                                           boolean useNio,
                                                           String clusterFile,
                                                           String storeFile,
                                                           Properties properties,
                                                           Cluster customCluster)
            throws IOException {
        Cluster cluster = null;
        if(customCluster != null) {
            cluster = customCluster;
        } else {
            cluster = ServerTestUtils.getLocalCluster(numServers, partitionMap);
        }

        int count = 0;
        for(int nodeId: cluster.getNodeIds()) {

            voldemortServers[count] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                                           ServerTestUtils.createServerConfig(useNio,
                                                                                                              nodeId,
                                                                                                              TestUtils.createTempDir()
                                                                                                                       .getAbsolutePath(),
                                                                                                              clusterFile,
                                                                                                              storeFile,
                                                                                                              properties),
                                                                           cluster);
            count++;
        }
        return cluster;
    }

    /**
     * This method wraps up all of the work that is done in many different tests
     * to set up some number of Voldemort servers in a cluster. This method
     * masks an intermittent TOCTOU problem with the ports identified by
     * {@link #findFreePorts(int)} not actually being free when a server needs
     * to bind to them. If this method returns, it will return a non-null
     * cluster. This method is not guaranteed to return, but will likely
     * eventually do so...
     * 
     * @param numServers
     * @param voldemortServers
     * @param partitionMap
     * @param socketStoreFactory
     * @param useNio
     * @param clusterFile
     * @param storeFile
     * @param properties
     * @return Cluster object that was used to successfully start all of the
     *         servers.
     * @throws IOException
     */
    // TODO: numServers is likely not needed. If this method is refactored in
    // the future, then try and drop the numServers argument.
    public static Cluster startVoldemortCluster(int numServers,
                                                VoldemortServer[] voldemortServers,
                                                int[][] partitionMap,
                                                SocketStoreFactory socketStoreFactory,
                                                boolean useNio,
                                                String clusterFile,
                                                String storeFile,
                                                Properties properties) throws IOException {
        return startVoldemortCluster(numServers,
                                     voldemortServers,
                                     partitionMap,
                                     socketStoreFactory,
                                     useNio,
                                     clusterFile,
                                     storeFile,
                                     properties,
                                     null);
    }

    /**
     * This method wraps up all of the work that is done in many different tests
     * to set up some number of Voldemort servers in a cluster. This method
     * masks an intermittent TOCTOU problem with the ports identified by
     * {@link #findFreePorts(int)} not actually being free when a server needs
     * to bind to them. If this method returns, it will return a non-null
     * cluster. This method is not guaranteed to return, but will likely
     * eventually do so...
     * 
     * @param numServers
     * @param voldemortServers
     * @param partitionMap
     * @param socketStoreFactory
     * @param useNio
     * @param clusterFile
     * @param storeFile
     * @param properties
     * @param customCluster Use this specified cluster object
     * @return Cluster object that was used to successfully start all of the
     *         servers.
     * @throws IOException
     */
    // TODO: numServers is likely not needed. If this method is refactored in
    // the future, then try and drop the numServers argument.
    // So, is the socketStoreFactory argument.. It should be entirely hidden
    // within the helper method
    private static Cluster startVoldemortCluster(int numServers,
                                                 VoldemortServer[] voldemortServers,
                                                 int[][] partitionMap,
                                                 SocketStoreFactory socketStoreFactory,
                                                 boolean useNio,
                                                 String clusterFile,
                                                 String storeFile,
                                                 Properties properties,
                                                 Cluster customCluster) throws IOException {
        boolean started = false;
        Cluster cluster = null;

        while(!started) {
            try {
                cluster = internalStartVoldemortCluster(numServers,
                                                        voldemortServers,
                                                        partitionMap,
                                                        socketStoreFactory,
                                                        useNio,
                                                        clusterFile,
                                                        storeFile,
                                                        properties,
                                                        customCluster);
                started = true;
            } catch(BindException be) {
                logger.debug("Caught BindException when starting cluster. Will retry.");
            }
        }

        return cluster;
    }

    public static Cluster startVoldemortCluster(VoldemortServer[] voldemortServers,
                                                int[][] partitionMap,
                                                String clusterFile,
                                                String storeFile,
                                                Properties properties,
                                                Cluster customCluster) throws IOException {
        boolean started = false;
        Cluster cluster = null;

        SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                              10000,
                                                                              100000,
                                                                              32 * 1024);

        try {
            while(!started) {
                try {
                    cluster = internalStartVoldemortCluster(voldemortServers.length,
                                                            voldemortServers,
                                                            partitionMap,
                                                            socketStoreFactory,
                                                            true,
                                                            clusterFile,
                                                            storeFile,
                                                            properties,
                                                            customCluster);
                    started = true;
                } catch(BindException be) {
                    logger.debug("Caught BindException when starting cluster. Will retry.");
                }
            }
        } finally {
            socketStoreFactory.close();
        }

        return cluster;
    }

    public static Cluster startVoldemortCluster(VoldemortServer[] servers,
                                                int[][] partitionMap,
                                                Properties serverProperties,
                                                String storesXmlFile) throws IOException {

        SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                              10000,
                                                                              100000,
                                                                              32 * 1024);
        Cluster cluster = null;
        try {
            cluster = ServerTestUtils.startVoldemortCluster(servers.length,
                                                            servers,
                                                            partitionMap,
                                                            socketStoreFactory,
                                                            true,
                                                            null,
                                                            storesXmlFile,
                                                            serverProperties);
        } finally {
            socketStoreFactory.close();
        }

        return cluster;
    }

    public static VoldemortServer startStandAloneVoldemortServer(Properties serverProperties,
                                                                 String storesXmlFile)
            throws IOException {

        VoldemortServer[] servers = new VoldemortServer[1];
        int partitionMap[][] = { { 0, 1, 2, 3 } };

        SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                              10000,
                                                                              100000,
                                                                              32 * 1024);
        try {
            Cluster cluster = ServerTestUtils.startVoldemortCluster(1,
                                                                    servers,
                                                                    partitionMap,
                                                                    socketStoreFactory,
                                                                    true,
                                                                    null,
                                                                    storesXmlFile,
                                                                    serverProperties);
        } finally {
            socketStoreFactory.close();
        }

        return servers[0];
    }
}
