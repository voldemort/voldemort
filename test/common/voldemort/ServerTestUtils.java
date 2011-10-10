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

package voldemort;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
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

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.io.FileUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

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
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Helper functions for testing with real server implementations
 * 
 * 
 */
public class ServerTestUtils {

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
                                                 false);
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
        RequestRoutingType requestRoutingType = RequestRoutingType.getRequestRoutingType(isRouted,
                                                                                         false);
        return storeFactory.create(storeName, host, port, type, requestRoutingType);
    }

    public static ContextHandler getJettyServer(String clusterXml,
                                         String storesXml,
                                         String storeName,
                                         RequestFormatType requestFormat,
                                         int port) throws Exception {
        StoreRepository repository = getStores(storeName, clusterXml, storesXml);

        // initialize servlet
        Server server = new Server(port);
        server.setSendServerVersion(false);
        ServletContextHandler context = new ServletContextHandler(server, "/", false, true);

        RequestHandler handler = getSocketRequestHandlerFactory(clusterXml, storesXml, repository).getRequestHandler(requestFormat);
        context.addServlet(new ServletHolder(new StoreServlet(handler)), "/stores");
        server.start();
        return context;
    }

    public static HttpStore getHttpStore(String storeName, RequestFormatType format, int port) {
        return new HttpStore(storeName,
                             "localhost",
                             port,
                             new HttpClient(),
                             new RequestFormatFactory().getRequestFormat(format),
                             false);
    }

    /**
     * Return a free port as chosen by new ServerSocket(0)
     */
    public static int findFreePort() {
        return findFreePorts(1)[0];
    }

    /**
     * Return an array of free ports as chosen by new ServerSocket(0)
     */
    public static int[] findFreePorts(int n) {
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
    public static List<Zone> getZones(int numberOfZones) {
        List<Zone> zones = Lists.newArrayList();
        for(int i = 0; i < numberOfZones; i++) {
            LinkedList<Integer> proximityList = Lists.newLinkedList();
            int zoneId = i + 1;
            for(int j = 0; j < numberOfZones; j++) {
                if(zoneId % numberOfZones == i)
                    break;
                proximityList.add(zoneId % numberOfZones);
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
        } else {
            return new Cluster("cluster", nodes);
        }
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
            // Adding twice so as check if ObsoleteVersionExceptions are
            // swallowed correctly
            slops.add(versioned);
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
        Props props = new Props(properties);
        props.put("node.id", nodeId);
        props.put("voldemort.home", baseDir + "/node-" + nodeId);
        props.put("bdb.cache.size", 1 * 1024 * 1024);
        props.put("bdb.write.transactions", "true");
        props.put("bdb.flush.transactions", "true");
        props.put("jmx.enable", "false");
        props.put("enable.mysql.engine", "true");

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
        return new AdminClient(cluster, config);
    }

    public static AdminClient getAdminClient(String bootstrapURL) {
        AdminClientConfig config = new AdminClientConfig();
        return new AdminClient(bootstrapURL, config);
    }

    public static RequestHandlerFactory getSocketRequestHandlerFactory(StoreRepository repository) {
        return new SocketRequestHandlerFactory(null, repository, null, null, null, null);
    }

    public static void stopVoldemortServer(VoldemortServer server) throws IOException {
        try {
            server.stop();
        } finally {
            FileUtils.deleteDirectory(new File(server.getVoldemortConfig().getVoldemortHome()));
        }
    }

    public static VoldemortServer startVoldemortServer(SocketStoreFactory socketStoreFactory,
                                                       VoldemortConfig config,
                                                       Cluster cluster) {
        VoldemortServer server = new VoldemortServer(config, cluster);
        server.start();

        ServerTestUtils.waitForServerStart(socketStoreFactory, server.getIdentityNode());
        // wait till server start or throw exception
        return server;
    }

    public static void waitForServerStart(SocketStoreFactory socketStoreFactory, Node node) {
        boolean success = false;
        int retries = 10;
        Store<ByteArray, ?, ?> store = null;
        while(retries-- > 0) {
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
}
