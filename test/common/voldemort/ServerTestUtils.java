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
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.io.FileUtils;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import voldemort.client.RoutingTier;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClientRequestFormat;
import voldemort.client.protocol.admin.NativeAdminClientRequestFormat;
import voldemort.client.protocol.admin.ProtoBuffAdminClientRequestFormat;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.AbstractSocketService;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.http.StoreServlet;
import voldemort.server.niosocket.NioSocketService;
import voldemort.server.protocol.RequestHandler;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.server.protocol.SocketRequestHandlerFactory;
import voldemort.server.socket.SocketService;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.http.HttpStore;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;
import voldemort.store.socket.SocketStore;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;

import com.google.common.collect.ImmutableList;

/**
 * Helper functions for testing with real server implementations
 * 
 * @author jay
 * 
 */
public class ServerTestUtils {

    public static StoreRepository getStores(String storeName, String clusterXml, String storesXml) {
        StoreRepository repository = new StoreRepository();
        Store<ByteArray, byte[]> store = new InMemoryStorageEngine<ByteArray, byte[]>(storeName);
        repository.addLocalStore(store);
        repository.addRoutedStore(store);
        MetadataStore metadata = new MetadataStore(new InMemoryStorageEngine<String, String>("metadata"),
                                                   0);
        metadata.put(new ByteArray(MetadataStore.CLUSTER_KEY.getBytes()),
                     new Versioned<byte[]>(clusterXml.getBytes()));
        metadata.put(new ByteArray(MetadataStore.STORES_KEY.getBytes()),
                     new Versioned<byte[]>(storesXml.getBytes()));
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
        RequestHandlerFactory factory = new SocketRequestHandlerFactory(getStores(storeName,
                                                                                  clusterXml,
                                                                                  storesXml));
        return getSocketService(useNio, factory, port, 5, 10, 10000);
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

    public static SocketStore getSocketStore(String storeName, int port) {
        return getSocketStore(storeName, port, RequestFormatType.VOLDEMORT_V1);
    }

    public static SocketStore getSocketStore(String storeName, int port, RequestFormatType type) {
        SocketPool socketPool = new SocketPool(2, 10000, 100000, 32 * 1024);
        return new SocketStore(storeName,
                               new SocketDestination("localhost", port, type),
                               socketPool,
                               false);
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

        RequestHandler handler = new SocketRequestHandlerFactory(repository).getRequestHandler(requestFormat);
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
        return getLocalCluster(numberOfNodes, findFreePorts(2 * numberOfNodes));
    }

    public static Cluster getLocalCluster(int numberOfNodes, int[] ports) {
        if(2 * numberOfNodes != ports.length)
            throw new IllegalArgumentException(3 * numberOfNodes + " ports required but only "
                                               + ports.length + " given.");
        List<Node> nodes = new ArrayList<Node>();
        for(int i = 0; i < numberOfNodes; i++)
            nodes.add(new Node(i, "localhost", ports[2 * i], ports[2 * i + 1], ImmutableList.of(i)));
        return new Cluster("test-cluster", nodes);
    }

    public static Node getLocalNode(int nodeId, List<Integer> partitions) {
        int[] ports = findFreePorts(2);
        return new Node(nodeId, "localhost", ports[0], ports[1], partitions);
    }

    public static List<StoreDefinition> getStoreDefs(int numStores) {
        List<StoreDefinition> defs = new ArrayList<StoreDefinition>();
        SerializerDefinition serDef = new SerializerDefinition("string");
        for(int i = 0; i < numStores; i++)
            defs.add(new StoreDefinition("test" + i,
                                         InMemoryStorageConfiguration.TYPE_NAME,
                                         serDef,
                                         serDef,
                                         RoutingTier.SERVER,
                                         RoutingStrategyType.CONSISTENT_STRATEGY,
                                         2,
                                         1,
                                         1,
                                         1,
                                         1,
                                         1,
                                         1));
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
        return new StoreDefinition(storeName,
                                   InMemoryStorageConfiguration.TYPE_NAME,
                                   serDef,
                                   serDef,
                                   RoutingTier.SERVER,
                                   strategyType,
                                   replicationFactor,
                                   preads,
                                   rreads,
                                   pwrites,
                                   rwrites,
                                   1,
                                   1);
    }

    public static VoldemortConfig createServerConfig(int nodeId,
                                                     String baseDir,
                                                     String clusterFile,
                                                     String storeFile) throws IOException {
        Props props = new Props();
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
        config.setStreamMaxReadBytesPerSec(10 * 1000);
        config.setStreamMaxWriteBytesPerSec(10 * 1000);

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

    public static NativeAdminClientRequestFormat getAdminClient(Node identityNode,
                                                                MetadataStore metadataStore) {
        return new NativeAdminClientRequestFormat(metadataStore, new SocketPool(2,
                                                                                10000,
                                                                                100000,
                                                                                32 * 1024));
    }

    public static AdminClientRequestFormat getAdminClient(Node identityNode,
                                                          MetadataStore metadataStore,
                                                          boolean useProtocolBuffers) {
        if(useProtocolBuffers)
            return new ProtoBuffAdminClientRequestFormat(metadataStore, new SocketPool(2,
                                                                                       10000,
                                                                                       100000,
                                                                                       32 * 1024));
        else
            return getAdminClient(identityNode, metadataStore);

    }
}
