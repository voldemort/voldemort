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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.io.FileUtils;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.http.StoreServlet;
import voldemort.server.socket.SocketServer;
import voldemort.store.Store;
import voldemort.store.http.HttpStore;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketPool;
import voldemort.store.socket.SocketStore;
import voldemort.utils.ByteArray;

import com.google.common.collect.ImmutableList;

/**
 * Helper functions for testing with real server implementations
 * 
 * @author jay
 * 
 */
public class ServerTestUtils {

    public static ConcurrentMap<String, Store<ByteArray, byte[]>> getStores(String storeName,
                                                                            String clusterXml,
                                                                            String storesXml) {
        ConcurrentMap<String, Store<ByteArray, byte[]>> stores = new ConcurrentHashMap<String, Store<ByteArray, byte[]>>(1);
        stores.put(storeName, new InMemoryStorageEngine<ByteArray, byte[]>(storeName));
        // create metadata dir
        File metadataDir = TestUtils.getTempDirectory();
        try {
            FileUtils.writeStringToFile(new File(metadataDir, "cluster.xml"), clusterXml);
            FileUtils.writeStringToFile(new File(metadataDir, "stores.xml"), storesXml);
            MetadataStore metadata = new MetadataStore(metadataDir, stores);
            stores.put(MetadataStore.METADATA_STORE_NAME, metadata);
            return stores;
        } catch(IOException e) {
            throw new VoldemortException("Error creating metadata directory:", e);
        }
    }

    public static SocketServer getSocketServer(String clusterXml,
                                               String storesXml,
                                               String storeName,
                                               int port) {

        SocketServer socketServer = new SocketServer(getStores(storeName, clusterXml, storesXml),
                                                     port,
                                                     5,
                                                     10,
                                                     10000);
        socketServer.start();
        socketServer.awaitStartupCompletion();
        return socketServer;
    }

    public static SocketStore getSocketStore(String storeName, int port) {
        SocketPool socketPool = new SocketPool(1, 2, 1000, 32 * 1024);
        return new SocketStore(storeName, "localhost", port, socketPool);
    }

    public static Context getJettyServer(String clusterXml,
                                         String storesXml,
                                         String storeName,
                                         int port) throws Exception {
        ConcurrentMap<String, Store<ByteArray, byte[]>> stores = getStores(storeName,
                                                                           clusterXml,
                                                                           storesXml);

        // initialize servlet
        Server server = new Server(port);
        server.setSendServerVersion(false);
        Context context = new Context(server, "/", Context.NO_SESSIONS);

        context.addServlet(new ServletHolder(new StoreServlet(stores)), "/*");
        server.start();
        return context;
    }

    public static HttpStore getHttpStore(String storeName, int port) {
        return new HttpStore(storeName, "localhost", port, new HttpClient());
    }

    /**
     * Return a free port as chosen by new SocketServer(0)
     */
    public static int findFreePort() {
        return findFreePorts(1)[0];
    }

    /**
     * Return an array of free ports as chosen by new SocketServer(0)
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
            throw new IllegalArgumentException(2 * numberOfNodes + " ports required but only "
                                               + ports.length + " given.");
        List<Node> nodes = new ArrayList<Node>();
        for(int i = 0; i < numberOfNodes; i++)
            nodes.add(new Node(i, "localhost", ports[2 * i], ports[2 * i + 1], ImmutableList.of(i)));
        return new Cluster("test-cluster", nodes);
    }
}
