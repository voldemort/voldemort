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
package voldemort.server.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.BaseStoreRoutingPlan;
import voldemort.server.VoldemortServer;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

public class VersionedPutPruneJobTest {

    VoldemortServer[] servers;
    Cluster cluster;
    BaseStoreRoutingPlan oldRoutingPlan;
    BaseStoreRoutingPlan currentRoutingPlan;
    HashMap<Integer, Store<ByteArray, byte[], byte[]>> socketStoreMap;
    SocketStoreFactory socketStoreFactory;
    HashMap<String, String> testEntries;
    StoreClient<Object, Object> storeClient;

    @Before
    public void setup() throws Exception {

        socketStoreMap = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();
        socketStoreFactory = new ClientRequestExecutorPool(2, 10000, 100000, 32 * 1024);

        final int numServers = 4;
        servers = new VoldemortServer[numServers];
        int currentPartitionMap[][] = { { 0, 4 }, { 2, 6 }, { 1, 5 }, { 3, 7 } };

        cluster = ServerTestUtils.startVoldemortCluster(numServers,
                                                        servers,
                                                        currentPartitionMap,
                                                        socketStoreFactory,
                                                        true,
                                                        null,
                                                        "test/common/voldemort/config/single-store-322.xml",
                                                        new Properties());
        StringReader reader = new StringReader(VoldemortTestConstants.getSingleStore322Xml());
        StoreDefinition storeDef = new StoreDefinitionsMapper().readStoreList(reader).get(0);

        currentRoutingPlan = new BaseStoreRoutingPlan(cluster, storeDef);

        String bootStrapUrl = "";
        for(VoldemortServer server: servers) {
            Node node = server.getIdentityNode();
            socketStoreMap.put(node.getId(),
                               ServerTestUtils.getSocketStore(socketStoreFactory,
                                                              "test",
                                                              node.getHost(),
                                                              node.getSocketPort(),
                                                              RequestFormatType.PROTOCOL_BUFFERS,
                                                              false,
                                                              true));
            bootStrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();
        }
        testEntries = ServerTestUtils.createRandomKeyValueString(100);

        int oldPartitionMap[][] = { { 3, 6 }, { 1, 4 }, { 7, 2 }, { 5, 0 } };
        oldRoutingPlan = new BaseStoreRoutingPlan(ServerTestUtils.getLocalCluster(numServers,
                                                                                  oldPartitionMap),
                                                  storeDef);

        SocketStoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootStrapUrl));
        storeClient = factory.getStoreClient("test");
    }

    private VectorClock makeVersionedPutClock(BaseStoreRoutingPlan plan, byte[] key, long clockTs) {
        List<Integer> nodes = plan.getReplicationNodeList(key);
        int[] nodesArr = new int[nodes.size()];
        for(int i = 0; i < nodesArr.length; i++) {
            nodesArr[i] = nodes.get(i);
        }
        return TestUtils.getVersionedPutClock(clockTs, -1, nodesArr);
    }

    private boolean clockContainsNonReplicas(VectorClock clock, List<Integer> replicas) {
        for(ClockEntry cEntry: clock.getEntries()) {
            if(!replicas.contains((int) cEntry.getNodeId())) {
                return true;
            }
        }

        return false;
    }

    @Test
    public void testPruneJob() {

        long now = System.currentTimeMillis();

        // for all keys, do versioned puts based on old and new routing plans
        int recordCount = 0;
        String fetchedValue = "fetched";
        String onlineValue = "online";
        for(Map.Entry<String, String> entry: testEntries.entrySet()) {
            ByteArray key = new ByteArray(entry.getKey().getBytes());
            Versioned<Object> fetchedVersion = new Versioned<Object>(fetchedValue,
                                                                     makeVersionedPutClock(oldRoutingPlan,
                                                                                           key.get(),
                                                                                           now));
            storeClient.put(entry.getKey(), fetchedVersion);
            if(recordCount < 50) {
                // let the first 50 keys be active ones that received some
                // online writes before the prune job
                Versioned<Object> onlineVersion = new Versioned<Object>(onlineValue,
                                                                        makeVersionedPutClock(currentRoutingPlan,
                                                                                              key.get(),
                                                                                              now
                                                                                                      + Time.MS_PER_SECOND));
                storeClient.put(entry.getKey(), onlineVersion);
            }
            recordCount++;
        }

        // run the prune job
        AdminClient admin = new AdminClient(cluster, new AdminClientConfig(), new ClientConfig());
        for(int nodeid = 0; nodeid < servers.length; nodeid++) {
            admin.storeMntOps.pruneJob(nodeid, "test");
        }
        for(VoldemortServer server: servers) {
            ServerTestUtils.waitForAsyncOperationOnServer(server, "Prune", 5000);
        }

        // do the checks
        recordCount = 0;
        for(Map.Entry<String, String> entry: testEntries.entrySet()) {
            ByteArray key = new ByteArray(entry.getKey().getBytes());
            List<Integer> replicas = currentRoutingPlan.getReplicationNodeList(key.get());

            Versioned<Object> val = storeClient.get(entry.getKey());
            // check vector clock does not contain non replicas
            assertFalse("Clock must not contain any non replicas",
                        clockContainsNonReplicas((VectorClock) val.getVersion(), replicas));

            if(recordCount < 50) {
                assertEquals("Must have online value", onlineValue, val.getValue());
            } else {
                assertEquals("Must have fetched value", fetchedValue, val.getValue());
            }
            recordCount++;
        }

        // do subsequent writes
        String finalValue = "final";
        for(Map.Entry<String, String> entry: testEntries.entrySet()) {
            ByteArray key = new ByteArray(entry.getKey().getBytes());
            List<Integer> replicas = currentRoutingPlan.getReplicationNodeList(key.get());
            Versioned<Object> finalVersion = new Versioned<Object>(finalValue,
                                                                   makeVersionedPutClock(currentRoutingPlan,
                                                                                         key.get(),
                                                                                         now
                                                                                                 + 2
                                                                                                 * Time.MS_PER_SECOND));
            storeClient.put(entry.getKey(), finalVersion);
            for(Integer replica: replicas) {
                Store<ByteArray, byte[], byte[]> socketStore = socketStoreMap.get(replica);
                List<Versioned<byte[]>> vals = socketStore.get(key, null);
                assertEquals("No more conflicts expected", 1, vals.size());
                assertEquals("Key should have the final value",
                             finalValue,
                             new String(vals.get(0).getValue()));
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        for(VoldemortServer server: servers) {
            ServerTestUtils.stopVoldemortServer(server);
        }
        socketStoreFactory.close();
    }
}
