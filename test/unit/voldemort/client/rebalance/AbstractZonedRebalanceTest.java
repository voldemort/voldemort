/*
 * Copyright 2008-2012 LinkedIn, Inc
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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.RoutingTier;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.slop.strategy.HintedHandoffStrategyType;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

/**
 * Rebalancing tests with zoned configurations with cross zone moves (since
 * {@link AbstractNonZonedRebalanceTest} should cover intra zone moves already
 * 
 */
public abstract class AbstractZonedRebalanceTest extends AbstractRebalanceTest {

    private static final Logger logger = Logger.getLogger(AbstractZonedRebalanceTest.class.getName());

    protected static String testStoreNameRW = "test";
    protected static String testStoreNameRW2 = "test2";

    protected static String storeDefFileWithoutReplication;
    protected static String storeDefFileWithReplication;
    protected static String rwStoreDefFileWithReplication;
    protected static String rwTwoStoreDefFileWithReplication;

    private List<StoreDefinition> storeDefWithoutReplication;
    private List<StoreDefinition> storeDefWithReplication;
    private StoreDefinition rwStoreDefWithoutReplication;
    private StoreDefinition rwStoreDefWithReplication;
    private StoreDefinition rwStoreDefWithReplication2;

    public AbstractZonedRebalanceTest(boolean useNio, boolean useDonorBased) {
        super(useNio, useDonorBased);
    }

    @Before
    public void setUp() throws IOException {
        // First without replication
        HashMap<Integer, Integer> zrfRWStoreWithoutReplication = new HashMap<Integer, Integer>();
        zrfRWStoreWithoutReplication.put(0, 1);
        zrfRWStoreWithoutReplication.put(1, 1);
        rwStoreDefWithoutReplication = new StoreDefinitionBuilder().setName(testStoreNameRW)
                                                                   .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                   .setKeySerializer(new SerializerDefinition("string"))
                                                                   .setValueSerializer(new SerializerDefinition("string"))
                                                                   .setRoutingPolicy(RoutingTier.CLIENT)
                                                                   .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                                   .setReplicationFactor(2)
                                                                   .setPreferredReads(1)
                                                                   .setRequiredReads(1)
                                                                   .setPreferredWrites(1)
                                                                   .setRequiredWrites(1)
                                                                   .setZoneCountReads(1)
                                                                   .setZoneCountWrites(1)
                                                                   .setZoneReplicationFactor(zrfRWStoreWithoutReplication)
                                                                   .setHintedHandoffStrategy(HintedHandoffStrategyType.PROXIMITY_STRATEGY)
                                                                   .build();

        storeDefWithoutReplication = Lists.newArrayList(rwStoreDefWithoutReplication);
        String storeDefWithoutReplicationString = new StoreDefinitionsMapper().writeStoreList(storeDefWithoutReplication);
        File file = File.createTempFile("two-stores-", ".xml");
        FileUtils.writeStringToFile(file, storeDefWithoutReplicationString);
        storeDefFileWithoutReplication = file.getAbsolutePath();

        // Now with replication
        HashMap<Integer, Integer> zrfRWStoreWithReplication = new HashMap<Integer, Integer>();
        zrfRWStoreWithReplication.put(0, 2);
        zrfRWStoreWithReplication.put(1, 2);
        rwStoreDefWithReplication = new StoreDefinitionBuilder().setName(testStoreNameRW)
                                                                .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                .setKeySerializer(new SerializerDefinition("string"))
                                                                .setValueSerializer(new SerializerDefinition("string"))
                                                                .setRoutingPolicy(RoutingTier.CLIENT)
                                                                .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                                .setReplicationFactor(4)
                                                                .setPreferredReads(1)
                                                                .setRequiredReads(1)
                                                                .setPreferredWrites(1)
                                                                .setRequiredWrites(1)
                                                                .setZoneCountReads(1)
                                                                .setZoneCountWrites(1)
                                                                .setZoneReplicationFactor(zrfRWStoreWithReplication)
                                                                .setHintedHandoffStrategy(HintedHandoffStrategyType.PROXIMITY_STRATEGY)
                                                                .build();
        rwStoreDefWithReplication2 = new StoreDefinitionBuilder().setName(testStoreNameRW2)
                                                                 .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                 .setKeySerializer(new SerializerDefinition("string"))
                                                                 .setValueSerializer(new SerializerDefinition("string"))
                                                                 .setRoutingPolicy(RoutingTier.CLIENT)
                                                                 .setRoutingStrategyType(RoutingStrategyType.ZONE_STRATEGY)
                                                                 .setReplicationFactor(4)
                                                                 .setPreferredReads(1)
                                                                 .setRequiredReads(1)
                                                                 .setPreferredWrites(1)
                                                                 .setRequiredWrites(1)
                                                                 .setZoneCountReads(1)
                                                                 .setZoneCountWrites(1)
                                                                 .setZoneReplicationFactor(zrfRWStoreWithReplication)
                                                                 .setHintedHandoffStrategy(HintedHandoffStrategyType.PROXIMITY_STRATEGY)
                                                                 .build();

        file = File.createTempFile("rw-stores-", ".xml");
        FileUtils.writeStringToFile(file,
                                    new StoreDefinitionsMapper().writeStoreList(Lists.newArrayList(rwStoreDefWithReplication)));
        rwStoreDefFileWithReplication = file.getAbsolutePath();

        file = File.createTempFile("rw-two-stores-", ".xml");
        FileUtils.writeStringToFile(file,
                                    new StoreDefinitionsMapper().writeStoreList(Lists.newArrayList(rwStoreDefWithReplication,
                                                                                                   rwStoreDefWithReplication2)));
        rwTwoStoreDefFileWithReplication = file.getAbsolutePath();

        storeDefWithReplication = Lists.newArrayList(rwStoreDefWithReplication);
        String storeDefWithReplicationString = new StoreDefinitionsMapper().writeStoreList(storeDefWithReplication);
        file = File.createTempFile("two-stores-", ".xml");
        FileUtils.writeStringToFile(file, storeDefWithReplicationString);
        storeDefFileWithReplication = file.getAbsolutePath();
    }

    @After
    public void tearDown() {
        testEntries.clear();
        testEntries = null;
        socketStoreFactory.close();
        socketStoreFactory = null;
    }

    @Test(timeout = 600000)
    public void testRWRebalance() throws Exception {
        logger.info("Starting testRWRebalance");

        Cluster currentCluster = ServerTestUtils.getLocalZonedCluster(4,
                                                                      2,
                                                                      new int[] { 0, 0, 1, 1 },
                                                                      new int[][] { { 0, 2, 4, 6 },
                                                                              {}, { 1, 3, 5, 7 },
                                                                              {} });
        Cluster targetCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                    3,
                                                                    Lists.newArrayList(2, 6));
        targetCluster = RebalanceUtils.createUpdatedCluster(targetCluster,
                                                            1,
                                                            Lists.newArrayList(3, 7));

        // start all the servers
        List<Integer> serverList = Arrays.asList(0, 1, 2, 3);
        Map<String, String> configProps = new HashMap<String, String>();
        configProps.put("admin.max.threads", "50");
        currentCluster = startServers(currentCluster,
                                      storeDefFileWithoutReplication,
                                      serverList,
                                      configProps);
        // Update the cluster information based on the node information
        targetCluster = updateCluster(targetCluster);

        RebalanceClientConfig config = new RebalanceClientConfig();
        config.setDeleteAfterRebalancingEnabled(true);
        RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(currentCluster,
                                                                                      0),
                                                                      config);
        try {
            populateData(currentCluster, rwStoreDefWithoutReplication);

            rebalanceAndCheck(currentCluster,
                              targetCluster,
                              storeDefWithoutReplication,
                              rebalanceClient,
                              Arrays.asList(1, 2));

            checkConsistentMetadata(targetCluster, serverList);
        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    @Test(timeout = 600000)
    public void testRWRebalanceWithReplication() throws Exception {
        logger.info("Starting testRWRebalanceWithReplication");

        Cluster currentCluster = ServerTestUtils.getLocalZonedCluster(4,
                                                                      2,
                                                                      new int[] { 0, 0, 1, 1 },
                                                                      new int[][] { { 0, 2, 4 },
                                                                              { 6 }, { 1, 3, 5 },
                                                                              { 7 } });
        Cluster targetCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                    3,
                                                                    Lists.newArrayList(2));
        targetCluster = RebalanceUtils.createUpdatedCluster(targetCluster, 1, Lists.newArrayList(3));

        // start servers
        List<Integer> serverList = Arrays.asList(0, 1, 2, 3);
        Map<String, String> configProps = new HashMap<String, String>();
        configProps.put("admin.max.threads", "50");

        currentCluster = startServers(currentCluster,
                                      storeDefFileWithReplication,
                                      serverList,
                                      configProps);
        // Update the cluster information based on the node information
        targetCluster = updateCluster(targetCluster);

        RebalanceClientConfig config = new RebalanceClientConfig();
        config.setDeleteAfterRebalancingEnabled(true);
        RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(currentCluster,
                                                                                      0),
                                                                      config);
        try {

            populateData(currentCluster, rwStoreDefWithReplication);

            rebalanceAndCheck(currentCluster,
                              targetCluster,
                              storeDefWithReplication,
                              rebalanceClient,
                              Arrays.asList(0, 1, 2, 3));
            checkConsistentMetadata(targetCluster, serverList);
        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    @Test(timeout = 600000)
    public void testRebalanceCleanPrimarySecondary() throws Exception {
        logger.info("Starting testRebalanceCleanPrimary");
        Cluster currentCluster = ServerTestUtils.getLocalZonedCluster(6, 2, new int[] { 0, 0, 0, 1,
                1, 1 }, new int[][] { { 0 }, { 1, 6 }, { 2 }, { 3 }, { 4, 7 }, { 5 } });
        Cluster targetCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                    2,
                                                                    Lists.newArrayList(7));
        targetCluster = RebalanceUtils.createUpdatedCluster(targetCluster, 5, Lists.newArrayList(6));

        // start servers
        List<Integer> serverList = Arrays.asList(0, 1, 2, 3, 4, 5);
        Map<String, String> configProps = new HashMap<String, String>();
        configProps.put("enable.repair", "true");
        currentCluster = startServers(currentCluster,
                                      rwStoreDefFileWithReplication,
                                      serverList,
                                      configProps);
        // Update the cluster information based on the node information
        targetCluster = updateCluster(targetCluster);

        RebalanceClientConfig config = new RebalanceClientConfig();
        config.setDeleteAfterRebalancingEnabled(false);
        RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(currentCluster,
                                                                                      0),
                                                                      config);
        try {
            populateData(currentCluster, rwStoreDefWithReplication);

            AdminClient admin = rebalanceClient.getAdminClient();

            List<ByteArray> p6KeySamples = sampleKeysFromPartition(admin,
                                                                   1,
                                                                   rwStoreDefWithReplication.getName(),
                                                                   Arrays.asList(6),
                                                                   20);
            List<ByteArray> p1KeySamples = sampleKeysFromPartition(admin,
                                                                   1,
                                                                   rwStoreDefWithReplication.getName(),
                                                                   Arrays.asList(1),
                                                                   20);
            List<ByteArray> p3KeySamples = sampleKeysFromPartition(admin,
                                                                   0,
                                                                   rwStoreDefWithReplication.getName(),
                                                                   Arrays.asList(3),
                                                                   20);
            List<ByteArray> p2KeySamples = sampleKeysFromPartition(admin,
                                                                   1,
                                                                   rwStoreDefWithReplication.getName(),
                                                                   Arrays.asList(2),
                                                                   20);
            List<ByteArray> p7KeySamples = sampleKeysFromPartition(admin,
                                                                   4,
                                                                   rwStoreDefWithReplication.getName(),
                                                                   Arrays.asList(7),
                                                                   20);

            rebalanceAndCheck(currentCluster,
                              targetCluster,
                              Lists.newArrayList(rwStoreDefWithReplication),
                              rebalanceClient,
                              Arrays.asList(0, 1, 2, 3));
            checkConsistentMetadata(targetCluster, serverList);

            // Do the cleanup operation
            for(int i = 0; i < 6; i++) {
                admin.storeMntOps.repairJob(i);
            }
            // wait for the repairs to complete
            for(int i = 0; i < 6; i++) {
                ServerTestUtils.waitForAsyncOperationOnServer(serverMap.get(i), "Repair", 5000);
            }

            // confirm a primary movement in zone 0 : P6 : s1 -> S2. The zone 0
            // primary changes when p6 moves cross zone
            // check for existence of p6 in server 2,
            checkForKeyExistence(admin, 2, rwStoreDefWithReplication.getName(), p6KeySamples);
            // also check for p6 absence in server 1.
            checkForKeyNonExistence(admin, 1, rwStoreDefWithReplication.getName(), p6KeySamples);

            // confirm a secondary movement in zone 0.. p2 : s1 -> s0
            // check for its existence in server 0
            checkForKeyExistence(admin, 0, rwStoreDefWithReplication.getName(), p2KeySamples);
            // check for its absernce in server 1
            checkForKeyNonExistence(admin, 1, rwStoreDefWithReplication.getName(), p2KeySamples);

            // also check that p1 is stable in server 1 [primary stability]
            checkForKeyExistence(admin, 1, rwStoreDefWithReplication.getName(), p1KeySamples);
            // check that p3 is stable in server 0 [Secondary stability]
            checkForKeyExistence(admin, 0, rwStoreDefWithReplication.getName(), p3KeySamples);

            // finally, test for server 4 which now became the secondary for p7
            // from being a primary before
            checkForKeyExistence(admin, 4, rwStoreDefWithReplication.getName(), p7KeySamples);
        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    protected void populateData(Cluster cluster, StoreDefinition storeDef) throws Exception {

        // Create SocketStores for each Node first
        Map<Integer, Store<ByteArray, byte[], byte[]>> storeMap = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();
        for(Node node: cluster.getNodes()) {
            storeMap.put(node.getId(),
                         getSocketStore(storeDef.getName(), node.getHost(), node.getSocketPort()));
            System.err.printf("%d,%s,%s,%s\n",
                              node.getId(),
                              storeDef.getName(),
                              node.getSocketPort(),
                              node.getAdminPort());
        }

        RoutingStrategy routing = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                     cluster);
        for(Entry<String, String> entry: testEntries.entrySet()) {
            ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(entry.getKey(), "UTF-8"));
            List<Integer> preferenceNodes = RebalanceUtils.getNodeIds(routing.routeRequest(keyBytes.get()));

            System.err.println(preferenceNodes);

            // Go over every node
            for(int nodeId: preferenceNodes) {
                try {
                    storeMap.get(nodeId)
                            .put(keyBytes,
                                 new Versioned<byte[]>(ByteUtils.getBytes(entry.getValue(), "UTF-8")),
                                 null);
                } catch(ObsoleteVersionException e) {
                    System.out.println("Why are we seeing this at all here ?? ");
                    e.printStackTrace();
                }
            }
        }

        // close all socket stores
        for(Store<ByteArray, byte[], byte[]> store: storeMap.values()) {
            store.close();
        }
    }
}