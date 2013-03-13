/*
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
package voldemort.client;

import static junit.framework.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

@RunWith(Parameterized.class)
public class AdminFetchTest {

    private static int TEST_STREAM_KEYS_SIZE = 100;
    private static String testStoreName = "users";
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";

    private StoreDefinition testStoreDef;
    private VoldemortServer[] servers;
    private Cluster cluster;
    private AdminClient adminClient;
    private RoutingStrategy routingStrategy;
    private HashMap<Integer, Set<String>> partitionToKeysMap;

    private final boolean useNio;

    public AdminFetchTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    @Before
    public void setUp() throws IOException {

        partitionToKeysMap = new HashMap<Integer, Set<String>>();

        SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                              10000,
                                                                              100000,
                                                                              32 * 1024);

        final int numServers = 2;
        servers = new VoldemortServer[numServers];
        int partitionMap[][] = { { 0, 1, 2, 3 }, { 4, 5, 6, 7 } };
        cluster = ServerTestUtils.startVoldemortCluster(numServers,
                                                        servers,
                                                        partitionMap,
                                                        socketStoreFactory,
                                                        this.useNio,
                                                        null,
                                                        storesXmlfile,
                                                        new Properties());

        List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new File(storesXmlfile));

        for(StoreDefinition storeDef: storeDefs)
            if(storeDef.getName().equals(testStoreName))
                testStoreDef = storeDef;

        routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(testStoreDef, cluster);

        adminClient = ServerTestUtils.getAdminClient(cluster);

        // load data into the servers
        Node firstServer = cluster.getNodes().iterator().next();

        String bootstrapUrl = "tcp://" + firstServer.getHost() + ":" + firstServer.getSocketPort();
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl)
                                                                                    .setSelectors(2));

        // create a client that executes operations on a single store
        StoreClient<String, String> voldClient = factory.getStoreClient("users");
        for(int i = 0; i < TEST_STREAM_KEYS_SIZE; i++) {
            String key = "key" + i;
            byte[] bkey = key.getBytes("UTF-8");
            int partition = routingStrategy.getPartitionList(bkey).get(0);
            if(!partitionToKeysMap.containsKey(partition))
                partitionToKeysMap.put(partition, new HashSet<String>());
            partitionToKeysMap.get(partition).add(key);
            voldClient.put(key, "value" + i);
        }
    }

    @After
    public void tearDown() throws IOException {
        for(VoldemortServer server: servers) {
            ServerTestUtils.stopVoldemortServer(server);
        }
    }

    private Set<String> getEntries(Iterator<Pair<ByteArray, Versioned<byte[]>>> itr) {
        HashSet<String> keySet = new HashSet<String>();
        while(itr.hasNext()) {
            Pair<ByteArray, Versioned<byte[]>> entry = itr.next();
            keySet.add(new String(entry.getFirst().get()));
        }
        return keySet;
    }

    @Test
    public void testFetchPartitionPrimaryEntries() {
        HashMap<Integer, List<Integer>> replicaToPartitionList = new HashMap<Integer, List<Integer>>();
        replicaToPartitionList.put(0, Arrays.asList(0, 3));
        Iterator<Pair<ByteArray, Versioned<byte[]>>> entriesItr = adminClient.bulkFetchOps.fetchEntries(0,
                                                                                                        testStoreName,
                                                                                                        replicaToPartitionList,
                                                                                                        null,
                                                                                                        false,
                                                                                                        cluster,
                                                                                                        0);
        // gather all the keys obtained
        Set<String> fetchedKeys = getEntries(entriesItr);
        // make sure it fetched all the entries from the partitions requested
        Set<String> partition0Keys = new HashSet<String>(partitionToKeysMap.get(0));
        Set<String> partition3Keys = new HashSet<String>(partitionToKeysMap.get(3));

        partition0Keys.removeAll(fetchedKeys);
        partition3Keys.removeAll(fetchedKeys);
        assertEquals("Remainder in partition 0" + partition0Keys, 0, partition0Keys.size());
        assertEquals("Remainder in partition 3" + partition3Keys, 0, partition3Keys.size());
    }

    @Test
    public void testFetchPartitionSecondaryEntries() {
        HashMap<Integer, List<Integer>> replicaToPartitionList = new HashMap<Integer, List<Integer>>();
        replicaToPartitionList.put(1, Arrays.asList(4, 6));
        Iterator<Pair<ByteArray, Versioned<byte[]>>> entriesItr = adminClient.bulkFetchOps.fetchEntries(0,
                                                                                                        testStoreName,
                                                                                                        replicaToPartitionList,
                                                                                                        null,
                                                                                                        false,
                                                                                                        cluster,
                                                                                                        0);
        // gather all the keys obtained
        Set<String> fetchedKeys = getEntries(entriesItr);
        // make sure it fetched all the entries from the partitions requested
        Set<String> partition4Keys = new HashSet<String>(partitionToKeysMap.get(4));
        Set<String> partition6Keys = new HashSet<String>(partitionToKeysMap.get(6));

        partition4Keys.removeAll(fetchedKeys);
        partition6Keys.removeAll(fetchedKeys);
        assertEquals("Remainder in partition 4" + partition4Keys, 0, partition4Keys.size());
        assertEquals("Remainder in partition 6" + partition6Keys, 0, partition6Keys.size());
    }

    @Test
    public void testFetchNonExistentEntriesPrimary() {
        HashMap<Integer, List<Integer>> replicaToPartitionList = new HashMap<Integer, List<Integer>>();
        replicaToPartitionList.put(0, Arrays.asList(5, 7));
        Iterator<Pair<ByteArray, Versioned<byte[]>>> entriesItr = adminClient.bulkFetchOps.fetchEntries(0,
                                                                                                        testStoreName,
                                                                                                        replicaToPartitionList,
                                                                                                        null,
                                                                                                        false,
                                                                                                        cluster,
                                                                                                        0);
        // gather all the keys obtained
        Set<String> fetchedKeys = getEntries(entriesItr);
        // make sure it fetched nothing since these partitions belong to server
        // 1
        assertEquals("Obtained something:" + fetchedKeys, 0, fetchedKeys.size());
    }

    @Test
    public void testFetchNonExistentEntriesSecondary() {
        HashMap<Integer, List<Integer>> replicaToPartitionList = new HashMap<Integer, List<Integer>>();
        replicaToPartitionList.put(1, Arrays.asList(1, 2));
        Iterator<Pair<ByteArray, Versioned<byte[]>>> entriesItr = adminClient.bulkFetchOps.fetchEntries(0,
                                                                                                        testStoreName,
                                                                                                        replicaToPartitionList,
                                                                                                        null,
                                                                                                        false,
                                                                                                        cluster,
                                                                                                        0);
        // gather all the keys obtained
        Set<String> fetchedKeys = getEntries(entriesItr);
        // make sure it fetched nothing since these partitions belong to server
        // 0 as primary
        assertEquals("Obtained something:" + fetchedKeys, 0, fetchedKeys.size());
    }
}
