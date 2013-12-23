/**
 * Copyright 2013 LinkedIn, Inc
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
package voldemort.client.rebalance;


import org.apache.commons.io.IOUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import voldemort.*;
import voldemort.client.*;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.ByteArraySerializer;
import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.Serializer;
import voldemort.serialization.SlopSerializer;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.serialized.SerializingStore;
import voldemort.store.slop.Slop;
import voldemort.store.slop.SlopStorageEngine;
import voldemort.store.socket.SocketStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.SocketStoreFactoryForTest;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;


public class ZoneShrinkageEndToEndTest {
    static String INITIAL_CLUSTER_XML_FILE = "config/zone-shrinkage-test/initial-cluster.xml";
    static String INITIAL_STORES_XML_FILE = "config/zone-shrinkage-test/initial-stores.xml";
    static String FINAL_CLUSTER_XML_FILE = "config/zone-shrinkage-test/final-cluster.xml";
    static String FINAL_STORES_XML_FILE = "config/zone-shrinkage-test/final-stores.xml";
    static String STORE322_NAME = "test322";
    static String STORE211_NAME = "test211";
    String initialClusterXML;
    String initialStoresXML;
    String finalClusterXML;
    String finalStoresXML;
    Cluster cluster;
    List<StoreDefinition> storeDefs;
    Map<Integer, VoldemortServer> vservers = new HashMap<Integer, VoldemortServer>();
    Map<Integer, SocketStoreFactory> socketStoreFactories = new HashMap<Integer, SocketStoreFactory>();
    Map<Integer, VoldemortConfig> voldemortConfigs = new HashMap<Integer, VoldemortConfig>();
    String bootstrapURL;
    ClusterMapper clusterMapper = new ClusterMapper();
    StoreDefinitionsMapper storeDefinitionsMapper = new StoreDefinitionsMapper();
    List<Node> survivingNodes = new ArrayList<Node>();
    List<Node> droppingNodes = new ArrayList<Node>();
    Integer droppingZoneId = 0;


    @Before
    public void setup() throws IOException {
        initialClusterXML = IOUtils.toString(ClusterTestUtils.class.getResourceAsStream(INITIAL_CLUSTER_XML_FILE));
        initialStoresXML = IOUtils.toString(ClusterTestUtils.class.getResourceAsStream(INITIAL_STORES_XML_FILE));
        finalClusterXML = IOUtils.toString(ClusterTestUtils.class.getResourceAsStream(FINAL_CLUSTER_XML_FILE));
        finalStoresXML = IOUtils.toString(ClusterTestUtils.class.getResourceAsStream(FINAL_STORES_XML_FILE));
        // setup cluster and stores
        cluster =  clusterMapper.readCluster(new StringReader(initialClusterXML));
        storeDefs = storeDefinitionsMapper.readStoreList(new StringReader(initialStoresXML));
        // setup and start servers
        for(Node node: cluster.getNodes()) {
            String tempFolderPath = TestUtils.createTempDir().getAbsolutePath();
            // setup servers
            SocketStoreFactory ssf = new SocketStoreFactoryForTest();
            VoldemortConfig config = ServerTestUtils.createServerConfigWithDefs(true, node.getId(), tempFolderPath, cluster, storeDefs,new Properties());
            Assert.assertTrue(config.isSlopEnabled());
            Assert.assertTrue(config.isSlopPusherJobEnabled());
            Assert.assertTrue(config.getAutoPurgeDeadSlops());
            config.setSlopFrequencyMs(10000L);
            VoldemortServer vs = ServerTestUtils.startVoldemortServer(ssf,config,cluster);
            vservers.put(node.getId(), vs);
            socketStoreFactories.put(node.getId(), ssf);
            voldemortConfigs.put(node.getId(), config);
        }

        for(Node node: cluster.getNodes()) {
            if(node.getZoneId() == droppingZoneId) {
                droppingNodes.add(node);
            } else {
                survivingNodes.add(node);
            }
        }

        bootstrapURL = survivingNodes.get(0).getSocketUrl().toString();

    }

    static class DummyTestClient implements Runnable{
        class PrintableHashMap extends HashMap<String, Integer> {
            @Override
            public String toString() {
                StringBuilder builder = new StringBuilder();
                for(Object key: this.keySet()) {
                    builder.append(key.toString() + ": " + this.get(key) + "\n");
                }
                return builder.toString();
            }
        };
        static int MODE_ALLOW_GET = 0x01;
        static int MODE_ALLOW_GETALL = 0x02;
        static int MODE_ALLOW_PUT = 0x04;
        static int MODE_ALLOW_ALL = MODE_ALLOW_GET | MODE_ALLOW_GETALL | MODE_ALLOW_PUT;
        int operationMode = MODE_ALLOW_ALL;
        static final Integer KV_POOL_SIZE = 100;
        final StoreClient<String, String> client;
        final StoreClientFactory factory;
        boolean shouldStop = false;
        boolean stopped = true;   // not thread safe by multiple readers
        final Thread thread;
        final String clientName;
        List<String> keys = new ArrayList<String>(KV_POOL_SIZE);
        Map<String, String> kvMap = new HashMap<String, String>(KV_POOL_SIZE);
        Map<String, Integer> kvUpdateCount = new HashMap<String, Integer>(KV_POOL_SIZE);
        public final Map<String, Integer> exceptionCount = new PrintableHashMap();
        public final Map<String, Integer> requestCount = new PrintableHashMap();

        DummyTestClient(String clientName, String bootstrapURL, String storeName, Integer clientZoneId) {
            factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapURL).setClientZoneId(clientZoneId));
            client = factory.getStoreClient(storeName);
            thread = new Thread(this);
            this.clientName = clientName;

            int i = 0;

            while (i < KV_POOL_SIZE) {
                String k = TestUtils.randomString("ABCDEFGHIJKLMNOPQRSTUVWXYZ", 40);
                String v = TestUtils.randomString("abcdefghijklmnopqrstuvwxyz", 40);
                if(kvMap.containsKey(k)) {
                    // prevent duplicate keys
                    continue;
                }
                kvMap.put(k, v);
                kvUpdateCount.put(k, 0);
                keys.add(k);
                i++;
            }

            requestCount.put("GET", 0);
            requestCount.put("PUT", 0);
            requestCount.put("GETALL", 0);
        }

        public DummyTestClient setOperationMode(int mode) {
            operationMode = mode;
            return this;
        }

        public void initialize() {
            for(String k: kvMap.keySet()) {
                client.put(k, kvMap.get(k) + "_" + kvUpdateCount.get(k).toString());
            }
        }

        @Override
        public void run() {
            Random r = new Random(System.currentTimeMillis());
            while(!shouldStop) {
                String k = keys.get(r.nextInt(KV_POOL_SIZE));
                try {
                    switch(r.nextInt(3)) {
                        case 0:  // update
                            if((operationMode & MODE_ALLOW_PUT) == 0) {
                               break;
                            }
                            kvUpdateCount.put(k, kvUpdateCount.get(k) + 1);
                            client.put(k, kvMap.get(k) + "_" + kvUpdateCount.get(k).toString());
                            requestCount.put("GET", requestCount.get("GET") + 1);
                            break;
                        case 1:  // get
                            if((operationMode & MODE_ALLOW_GET) == 0) {
                                break;
                            }
                            Versioned<String> value = client.get(k);  // does not check versions, just prevent exceptions
                            if(value == null) {
                               throw new RuntimeException("Versioned is empty for key ["+k+"]"){};
                            } else {
                                if(value.getValue() == null) {
                                    throw new RuntimeException("Versioned has empty value inside for key ["+k+"]"){};
                                }
                            }
                            requestCount.put("PUT", requestCount.get("PUT") + 1);
                            break;
                        case 2:  // get all
                            if((operationMode & MODE_ALLOW_GETALL) == 0) {
                                break;
                            }
                            String k2 = keys.get(r.nextInt(KV_POOL_SIZE));
                            Map<String, Versioned<String>> result = client.getAll(Arrays.asList(k, k2));
                            if(result.get(k) == null) {
                                throw new RuntimeException("Versioned is empty for key ["+k+"]"){};
                            } else {
                                if(result.get(k).getValue() == null) {
                                    throw new RuntimeException("Versioned has empty value inside for key ["+k+"]"){};
                                }
                            }
                            if(result.get(k2) == null) {
                                throw new RuntimeException("Versioned is empty for key ["+k2+"]"){};
                            } else {
                                if(result.get(k2).getValue() == null) {
                                    throw new RuntimeException("Versioned has empty value inside for key ["+k2+"]"){};
                                }
                            }
                            requestCount.put("GETALL", requestCount.get("GETALL") + 1);
                            break;
                    }
                } catch(ObsoleteVersionException e) {
                } catch(Exception e) {
                    System.out.println("CLIENT EXCEPTION FAILURE on key ["+ k +"]" + e.toString());
                    e.printStackTrace();
                    String exceptionName = e.getClass().toString();
                    if(exceptionCount.containsKey(exceptionName)) {
                        exceptionCount.put(exceptionName, exceptionCount.get(exceptionName) + 1);
                    } else {
                        exceptionCount.put(exceptionName, 1);
                    }
                }
            }
        }

        public void start() {
            this.thread.start();
            this.stopped = false;
        }

        public void stop() {
            try {
                this.shouldStop = true;
                this.thread.join();
                this.stopped = true;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test(timeout = 60000)
    public void endToEndTestUpdateTogether() {
        List<DummyTestClient> clients = new ArrayList<DummyTestClient>();
        clients.add(new DummyTestClient("1st_CLIENT_211_STORE_ZONE_1", bootstrapURL, STORE211_NAME, 1));
        clients.add(new DummyTestClient("1st_CLIENT_211_STORE_ZONE_2", bootstrapURL, STORE211_NAME, 2));
        clients.add(new DummyTestClient("1st_CLIENT_322_STORE_ZONE_1", bootstrapURL, STORE322_NAME, 1));
        clients.add(new DummyTestClient("1st_CLIENT_322_STORE_ZONE_2", bootstrapURL, STORE322_NAME, 2));
        clients.add(new DummyTestClient("2nd_CLIENT_211_STORE_ZONE_1", bootstrapURL, STORE211_NAME, 1).setOperationMode(DummyTestClient.MODE_ALLOW_GET));
        clients.add(new DummyTestClient("2nd_CLIENT_211_STORE_ZONE_2", bootstrapURL, STORE211_NAME, 2).setOperationMode(DummyTestClient.MODE_ALLOW_GET));
        clients.add(new DummyTestClient("2nd_CLIENT_322_STORE_ZONE_1", bootstrapURL, STORE322_NAME, 1).setOperationMode(DummyTestClient.MODE_ALLOW_GET));
        clients.add(new DummyTestClient("2nd_CLIENT_322_STORE_ZONE_2", bootstrapURL, STORE322_NAME, 2).setOperationMode(DummyTestClient.MODE_ALLOW_GET));
        clients.add(new DummyTestClient("3nd_CLIENT_211_STORE_ZONE_1", bootstrapURL, STORE211_NAME, 1).setOperationMode(DummyTestClient.MODE_ALLOW_PUT));
        clients.add(new DummyTestClient("3nd_CLIENT_211_STORE_ZONE_2", bootstrapURL, STORE211_NAME, 2).setOperationMode(DummyTestClient.MODE_ALLOW_PUT));
        clients.add(new DummyTestClient("3nd_CLIENT_322_STORE_ZONE_1", bootstrapURL, STORE322_NAME, 1).setOperationMode(DummyTestClient.MODE_ALLOW_PUT));
        clients.add(new DummyTestClient("3nd_CLIENT_322_STORE_ZONE_2", bootstrapURL, STORE322_NAME, 2).setOperationMode(DummyTestClient.MODE_ALLOW_PUT));
        clients.add(new DummyTestClient("4nd_CLIENT_211_STORE_ZONE_1", bootstrapURL, STORE211_NAME, 1).setOperationMode(DummyTestClient.MODE_ALLOW_GETALL));
        clients.add(new DummyTestClient("4nd_CLIENT_211_STORE_ZONE_2", bootstrapURL, STORE211_NAME, 2).setOperationMode(DummyTestClient.MODE_ALLOW_GETALL));
        clients.add(new DummyTestClient("4nd_CLIENT_322_STORE_ZONE_1", bootstrapURL, STORE322_NAME, 1).setOperationMode(DummyTestClient.MODE_ALLOW_GETALL));
        clients.add(new DummyTestClient("4nd_CLIENT_322_STORE_ZONE_2", bootstrapURL, STORE322_NAME, 2).setOperationMode(DummyTestClient.MODE_ALLOW_GETALL));
        try {
            System.out.println("-------------------------------");
            System.out.println("       STARTING CLIENT         ");
            System.out.println("-------------------------------");
            // start clients
            for(DummyTestClient client: clients) {
                client.initialize();
                client.start();
            }
            System.out.println("-------------------------------");
            System.out.println("        CLIENT STARTED         ");
            System.out.println("-------------------------------");

            // warm up
            Thread.sleep(5000);

            executeShrinkZone();

            // cool down
            Thread.sleep(15000);

            System.out.println("-------------------------------");
            System.out.println("         STOPPING CLIENT       ");
            System.out.println("-------------------------------");

            for(DummyTestClient client: clients) {
                client.stop();
            }

            System.out.println("-------------------------------");
            System.out.println("         STOPPED CLIENT        ");
            System.out.println("-------------------------------");

            // verify that all clients has new cluster now
            Integer failCount = 0;
            for(DummyTestClient client: clients) {
                if (client.client instanceof LazyStoreClient) {
                    LazyStoreClient<String,String> lsc = (LazyStoreClient<String,String>)client.client;
                    if(lsc.getStoreClient() instanceof ZenStoreClient) {
                        ZenStoreClient<String, String> zsc = (ZenStoreClient<String, String>) lsc.getStoreClient();
                        Long clusterMetadataVersion = zsc.getAsyncMetadataVersionManager().getClusterMetadataVersion();
                        if(clusterMetadataVersion == 0) {
                            failCount++;
                            System.err.format("The client %s did not pick up the new cluster metadata\n", client.clientName);
                        }
                    } else {
                        Assert.fail("There is problem with DummyClient's real client's real client, which should be ZenStoreClient but not");
                    }
                } else {
                    Assert.fail("There is problem with DummyClient's real client which should be LazyStoreClient but not");
                }
            }
            Assert.assertTrue(failCount.toString() + " client(s) did not pickup new metadata", failCount == 0);


            System.out.println("-------------------------------");
            System.out.println("   WAITING FOR SLOPS TO DRAIN  ");
            System.out.println("-------------------------------");

            // wait for all servers's slop to go away
            long slopDrainTimoutMs = 40000L;
            long timeStart = System.currentTimeMillis();
            boolean allSlopsEmpty = false;
            while(System.currentTimeMillis() < timeStart + slopDrainTimoutMs) {
                allSlopsEmpty = true;
                for(Integer nodeId: vservers.keySet()) {
                    VoldemortServer vs = vservers.get(nodeId);
                    SlopStorageEngine sse = vs.getStoreRepository().getSlopStore();
                    ClosableIterator<ByteArray> keys = sse.keys();
                    int count = 0;
                    while(keys.hasNext()) {
                        keys.next();
                        count++;
                    }
                    keys.close();
                    if(count > 0) {
                        allSlopsEmpty = false;
                        System.out.format("Slop engine for node %d is not yet empty with %d slops\n", nodeId, count);
                    }
                }
                if(allSlopsEmpty) {
                    break;
                }
                Thread.sleep(1000);
            }
            if(!allSlopsEmpty) {
                Assert.fail("Timeout while waiting for all slops to drain");
            }

            System.out.println("-------------------------------");
            System.out.println("     SLOPS FINISHED DRAINED    ");
            System.out.println("-------------------------------");
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail(e.toString());
        } catch (RuntimeException e) {
            e.printStackTrace();
            Assert.fail(e.toString());
        } finally {
            for (DummyTestClient client : clients) {
                if(!client.stopped) {
                    client.stop();
                }
            }

            for (DummyTestClient client : clients) {
                Map<String, Integer> eMap = client.exceptionCount;
                System.out.println("-------------------------------------------------------------------");
                System.out.println("Client Operation Info of [" + client.clientName + "]");
                System.out.print(client.requestCount.toString());
                if (eMap.size() == 0) {
                    System.out.println("No Exception reported by DummyTestClient(ObsoleteVersionException are ignored)");
                } else {
                    System.out.println("Exceptions Count Map of the client: ");
                    System.out.println(eMap.toString());
                    Assert.fail("Found Exceptions by Client");
                }

                System.out.println("-------------------------------------------------------------------");
            }
        }
    }

    @Test(timeout = 60000)
    public void testAllServersSendingOutSlopsCorrectly() throws InterruptedException {
        final Serializer<ByteArray> slopKeySerializer = new ByteArraySerializer();
        final Serializer<Slop> slopValueSerializer = new SlopSerializer();
        final SlopSerializer slopSerializer = new SlopSerializer();

        StoreDefinition storeDef = storeDefs.get(0);
        SocketStoreFactoryForTest ssf = new SocketStoreFactoryForTest();

        Map<Integer, SocketStore> slopStoresCreatedBeforeShrink = new HashMap<Integer, SocketStore>();
        Map<Integer, SocketStore> slopStoresCreatedAfterShrink = new HashMap<Integer, SocketStore>();

        // generate for keys each all servers that will be hosted on each server except itself (2*N*(N-1) keys)
        // Map<Integer slopFinalDestinationNodeId, List<Pair<ByteArray key, Integer hostNodeId>>>
        Map<Integer, List<Pair<ByteArray, Integer>>> serverKeys = new HashMap<Integer, List<Pair<ByteArray, Integer>>>();
        for(Node slopFinalDestinationNode: cluster.getNodes()) {
            serverKeys.put(slopFinalDestinationNode.getId(), new ArrayList<Pair<ByteArray, Integer>>());
        }

        // make socket stores to all servers before shrink
        for(Integer nodeId: vservers.keySet()) {
            SocketStore slopStore = ssf.createSocketStore(vservers.get(nodeId).getIdentityNode(), "slop");
            SerializingStore.wrap(slopStore, slopKeySerializer, slopValueSerializer, new IdentitySerializer());
            slopStoresCreatedBeforeShrink.put(nodeId, slopStore);
        }

        for(int i = 0; i < 2; i++) {
            for(Integer slopHostId: vservers.keySet()) {
                SocketStore slopStore = slopStoresCreatedBeforeShrink.get(slopHostId);
                for(Integer destinationNodeId: vservers.keySet()) {
                    if(!destinationNodeId.equals(slopHostId)) {
                        ByteArray key = generateRandomKey(cluster, destinationNodeId, storeDef.getReplicationFactor());
                        serverKeys.get(destinationNodeId).add(new Pair<ByteArray, Integer>(key, slopHostId));
                        Slop slop = new Slop(storeDef.getName(), Slop.Operation.PUT, key.get(), key.get(), destinationNodeId, new Date());
                        slopStore.put(slop.makeKey(), new Versioned<byte[]>(slopSerializer.toBytes(slop), new VectorClock()), null);
                    }
                }
            }
        }

        // update metadata
        executeShrinkZone();

        System.out.println("-------------------------------");
        System.out.println("    CONNECTING SLOP STORES     ");
        System.out.println("-------------------------------");

        // make socket stores to all servers after shrink
        for(Integer nodeId: vservers.keySet()) {
            SocketStore slopStore = ssf.createSocketStore(vservers.get(nodeId).getIdentityNode(), "slop");
            SerializingStore.wrap(slopStore, slopKeySerializer, slopValueSerializer, new IdentitySerializer());
            slopStoresCreatedAfterShrink.put(nodeId, slopStore);
        }

        System.out.println("-------------------------------");
        System.out.println("     CONNECTED SLOP STORES     ");
        System.out.println("-------------------------------");

        System.out.println("-------------------------------");
        System.out.println("         SENDING SLOPS         ");
        System.out.println("-------------------------------");

        for(int i = 0; i < 2; i++) {
            for(Integer slopHostId: vservers.keySet()) {
                SocketStore slopStore = slopStoresCreatedAfterShrink.get(slopHostId);
                for(Integer destinationNodeId: vservers.keySet()) {
                    if(!destinationNodeId.equals(slopHostId)) {
                        ByteArray key = generateRandomKey(cluster, destinationNodeId, storeDef.getReplicationFactor());
                        serverKeys.get(destinationNodeId).add(new Pair<ByteArray, Integer>(key, slopHostId));
                        Slop slop = new Slop(storeDef.getName(), Slop.Operation.PUT, key.get(), key.get(), destinationNodeId, new Date());
                        slopStore.put(slop.makeKey(), new Versioned<byte[]>(slopSerializer.toBytes(slop), new VectorClock()), null);
                    }
                }
            }
        }

        System.out.println("-------------------------------");
        System.out.println("           SENT SLOPS          ");
        System.out.println("-------------------------------");

        System.out.println("-------------------------------");
        System.out.println("  WAITING FOR SLOPS TO DRAIN   ");
        System.out.println("-------------------------------");


        long slopDrainTimoutMs = 30000L;
        long timeStart = System.currentTimeMillis();
        boolean allSlopsEmpty = false;
        while(System.currentTimeMillis() < timeStart + slopDrainTimoutMs) {
            allSlopsEmpty = true;
            for(Integer nodeId: vservers.keySet()) {
                VoldemortServer vs = vservers.get(nodeId);
                SlopStorageEngine sse = vs.getStoreRepository().getSlopStore();
                ClosableIterator<ByteArray> keys = sse.keys();
                long count = 0;
                while(keys.hasNext()) {
                    keys.next();
                    count++;
                }
                keys.close();
                if(count > 0) {
                    allSlopsEmpty = false;
                    System.out.format("Slop engine for node %d is not yet empty with %d slops\n", nodeId, count);
                }
            }
            if(allSlopsEmpty) {
                break;
            }
            Thread.sleep(1000);
        }
        if(!allSlopsEmpty) {
            Assert.fail("Timeout while waiting for all slops to drain");
        }


        System.out.println("-------------------------------");
        System.out.println("        ALL SLOPS DRAINED      ");
        System.out.println("-------------------------------");

        // verify all proper slops is processed properly (arrived or dropped)
        boolean hasError = false;
        int goodCount = 0;
        int errorCount = 0;
        for(Integer nodeId: serverKeys.keySet()) {
            VoldemortServer vs = vservers.get(nodeId);
            Store<ByteArray, byte[], byte[]> store = vs.getStoreRepository().getStorageEngine(storeDef.getName());
            List<Pair<ByteArray, Integer>> keySet = serverKeys.get(nodeId);
            for(Pair<ByteArray, Integer> keyHostIdPair: keySet) {
                ByteArray key = keyHostIdPair.getFirst();
                Integer hostId = keyHostIdPair.getSecond();
                Integer nodeZoneId = cluster.getNodeById(nodeId).getZoneId();
                List<Versioned<byte[]>> result = store.get(key, null);
                if(cluster.getNodeById(nodeId).getZoneId() == droppingZoneId) {
                    if(!result.isEmpty()) {
                        System.err.format("Key %s for Node %d (zone %d) slopped on Node %d should be gone but exists\n",
                                key.toString(), nodeId, nodeZoneId, hostId);
                        hasError = true;
                        errorCount++;
                    } else {
                        goodCount++;
                    }
                } else {
                    if(result.isEmpty()) {
                        System.err.format("Key %s for Node %d (zone %d) slopped on Node %d should exist but not\n",
                                key.toString(), nodeId, nodeZoneId, hostId);
                        hasError = true;
                        errorCount++;
                    } else {
                        goodCount++;
                    }
                }
            }
        }
        System.out.format("Good keys count: %d; Error keys count: %d", goodCount, errorCount);
        Assert.assertFalse("Error Occurred BAD:" + errorCount + "; GOOD: " + goodCount + ". Check log.", hasError);
    }

    public void executeShrinkZone() {
        AdminClient adminClient;

        System.out.println("-------------------------------");
        System.out.println("        UPDATING BOTH XML      ");
        System.out.println("-------------------------------");

        // get admin client
        AdminClientConfig adminClientConfig = new AdminClientConfig();
        ClientConfig clientConfigForAdminClient = new ClientConfig();
        clientConfigForAdminClient.setClientZoneId(survivingNodes.get(0).getZoneId());
        adminClient = new AdminClient(bootstrapURL, adminClientConfig, clientConfigForAdminClient);

        // set stores metadata (simulating admin tools)
        String validatedStoresXML = storeDefinitionsMapper.writeStoreList(storeDefinitionsMapper.readStoreList(new StringReader(finalStoresXML)));
        String validatedClusterXML = clusterMapper.writeCluster(clusterMapper.readCluster(new StringReader(finalClusterXML)));
        VoldemortAdminTool.executeSetMetadataPair(-1, adminClient, "cluster.xml", validatedClusterXML, "stores.xml", validatedStoresXML);

        adminClient.close();

        System.out.println("-------------------------------");
        System.out.println("       UPDATED CLUSTER.XML     ");
        System.out.println("-------------------------------");
    }

    @After
    public void shutdown() {
        for(Integer nodeId: vservers.keySet()) {
            vservers.get(nodeId).stop();
        }
        for(Integer nodeId: socketStoreFactories.keySet()) {
            socketStoreFactories.get(nodeId).close();
        }
    }

    public static ByteArray generateRandomKey(Cluster cluster, Integer nodeId, Integer replicationFactor) {
        for(;;) {
            byte[] candidate = TestUtils.randomString("ABCDEFGHIJKLMN", 10).getBytes();
            RoutingStrategy rs = new ConsistentRoutingStrategy(cluster, replicationFactor);
            List<Node> routes = rs.routeRequest(candidate);
            if(routes.get(0).getId() == nodeId) {
                ByteArray key = new ByteArray(candidate);
                return key;
            }
        }
    }
}
