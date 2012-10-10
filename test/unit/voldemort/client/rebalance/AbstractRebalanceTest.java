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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.DefaultStoreClient;
import voldemort.client.RoutingTier;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.json.JsonReader;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.UnreachableStoreException;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.JsonStoreBuilder;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageEngineTestInstance;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.swapper.AdminStoreSwapper;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.KeyLocationValidation;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Lists;

public abstract class AbstractRebalanceTest {

    protected static int NUM_KEYS = 20;
    protected static int NUM_RO_CHUNKS_PER_BUCKET = 10;
    protected static String testStoreNameRW = "test";
    protected static String testStoreNameRW2 = "test2";
    protected static String testStoreNameRO = "test-ro";

    protected static String storeDefFileWithoutReplication;
    protected static String storeDefFileWithReplication;
    protected static String roStoreDefFileWithReplication;
    protected static String rwStoreDefFileWithReplication;
    protected static String rwTwoStoreDefFileWithReplication;

    private List<StoreDefinition> storeDefWithoutReplication;
    private List<StoreDefinition> storeDefWithReplication;
    private StoreDefinition roStoreDefWithoutReplication;
    private StoreDefinition rwStoreDefWithoutReplication;
    private StoreDefinition roStoreDefWithReplication;
    private StoreDefinition rwStoreDefWithReplication;
    private StoreDefinition rwStoreDefWithReplication2;

    protected SocketStoreFactory socketStoreFactory;
    HashMap<String, String> testEntries;

    @Before
    public void setUp() throws IOException {
        testEntries = ServerTestUtils.createRandomKeyValueString(getNumKeys());
        socketStoreFactory = new ClientRequestExecutorPool(2, 10000, 100000, 32 * 1024);

        // First without replication
        roStoreDefWithoutReplication = new StoreDefinitionBuilder().setName(testStoreNameRO)
                                                                   .setType(ReadOnlyStorageConfiguration.TYPE_NAME)
                                                                   .setKeySerializer(new SerializerDefinition("string"))
                                                                   .setValueSerializer(new SerializerDefinition("string"))
                                                                   .setRoutingPolicy(RoutingTier.SERVER)
                                                                   .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                   .setReplicationFactor(1)
                                                                   .setPreferredReads(1)
                                                                   .setRequiredReads(1)
                                                                   .setPreferredWrites(1)
                                                                   .setRequiredWrites(1)
                                                                   .build();
        rwStoreDefWithoutReplication = new StoreDefinitionBuilder().setName(testStoreNameRW)
                                                                   .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                   .setKeySerializer(new SerializerDefinition("string"))
                                                                   .setValueSerializer(new SerializerDefinition("string"))
                                                                   .setRoutingPolicy(RoutingTier.SERVER)
                                                                   .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                   .setReplicationFactor(1)
                                                                   .setPreferredReads(1)
                                                                   .setRequiredReads(1)
                                                                   .setPreferredWrites(1)
                                                                   .setRequiredWrites(1)
                                                                   .build();

        storeDefWithoutReplication = Lists.newArrayList(roStoreDefWithoutReplication,
                                                        rwStoreDefWithoutReplication);
        String storeDefWithoutReplicationString = new StoreDefinitionsMapper().writeStoreList(storeDefWithoutReplication);
        File file = File.createTempFile("two-stores-", ".xml");
        FileUtils.writeStringToFile(file, storeDefWithoutReplicationString);
        storeDefFileWithoutReplication = file.getAbsolutePath();

        // Now with replication

        roStoreDefWithReplication = new StoreDefinitionBuilder().setName(testStoreNameRO)
                                                                .setType(ReadOnlyStorageConfiguration.TYPE_NAME)
                                                                .setKeySerializer(new SerializerDefinition("string"))
                                                                .setValueSerializer(new SerializerDefinition("string"))
                                                                .setRoutingPolicy(RoutingTier.SERVER)
                                                                .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                .setReplicationFactor(2)
                                                                .setPreferredReads(1)
                                                                .setRequiredReads(1)
                                                                .setPreferredWrites(1)
                                                                .setRequiredWrites(1)
                                                                .build();
        file = File.createTempFile("ro-stores-", ".xml");
        FileUtils.writeStringToFile(file,
                                    new StoreDefinitionsMapper().writeStoreList(Lists.newArrayList(roStoreDefWithReplication)));
        roStoreDefFileWithReplication = file.getAbsolutePath();

        rwStoreDefWithReplication = new StoreDefinitionBuilder().setName(testStoreNameRW)
                                                                .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                .setKeySerializer(new SerializerDefinition("string"))
                                                                .setValueSerializer(new SerializerDefinition("string"))
                                                                .setRoutingPolicy(RoutingTier.SERVER)
                                                                .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                .setReplicationFactor(2)
                                                                .setPreferredReads(1)
                                                                .setRequiredReads(1)
                                                                .setPreferredWrites(1)
                                                                .setRequiredWrites(1)
                                                                .build();
        rwStoreDefWithReplication2 = new StoreDefinitionBuilder().setName(testStoreNameRW2)
                                                                 .setType(BdbStorageConfiguration.TYPE_NAME)
                                                                 .setKeySerializer(new SerializerDefinition("string"))
                                                                 .setValueSerializer(new SerializerDefinition("string"))
                                                                 .setRoutingPolicy(RoutingTier.SERVER)
                                                                 .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                 .setReplicationFactor(2)
                                                                 .setPreferredReads(1)
                                                                 .setRequiredReads(1)
                                                                 .setPreferredWrites(1)
                                                                 .setRequiredWrites(1)
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

        storeDefWithReplication = Lists.newArrayList(roStoreDefWithReplication,
                                                     rwStoreDefWithReplication);
        String storeDefWithReplicationString = new StoreDefinitionsMapper().writeStoreList(storeDefWithReplication);
        file = File.createTempFile("two-stores-", ".xml");
        FileUtils.writeStringToFile(file, storeDefWithReplicationString);
        storeDefFileWithReplication = file.getAbsolutePath();
    }

    @After
    public void tearDown() {
        testEntries.clear();
        socketStoreFactory.close();
    }

    protected abstract Cluster startServers(Cluster cluster,
                                            String StoreDefXmlFile,
                                            List<Integer> nodeToStart,
                                            Map<String, String> configProps) throws Exception;

    protected abstract void stopServer(List<Integer> nodesToStop) throws Exception;

    protected Cluster updateCluster(Cluster template) {
        return template;
    }

    protected Store<ByteArray, byte[], byte[]> getSocketStore(String storeName,
                                                              String host,
                                                              int port) {
        return getSocketStore(storeName, host, port, false);
    }

    protected Store<ByteArray, byte[], byte[]> getSocketStore(String storeName,
                                                              String host,
                                                              int port,
                                                              boolean isRouted) {
        return ServerTestUtils.getSocketStore(socketStoreFactory,
                                              storeName,
                                              host,
                                              port,
                                              RequestFormatType.PROTOCOL_BUFFERS,
                                              isRouted);
    }

    protected abstract Cluster getCurrentCluster(int nodeId);

    protected abstract MetadataStore.VoldemortState getCurrentState(int nodeId);

    protected abstract boolean useDonorBased();

    public void checkConsistentMetadata(Cluster targetCluster, List<Integer> serverList) {
        for(int nodeId: serverList) {
            assertEquals(targetCluster, getCurrentCluster(nodeId));
            assertEquals(MetadataStore.VoldemortState.NORMAL_SERVER, getCurrentState(nodeId));
        }
    }

    protected int getNumKeys() {
        return NUM_KEYS;
    }

    @Test(timeout = 60000)
    public void testRORWRebalance() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, {} });

        Cluster targetCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                    1,
                                                                    Lists.newArrayList(2, 3));

        // start servers 0 , 1 only
        List<Integer> serverList = Arrays.asList(0, 1);
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

            // Populate the two stores
            populateData(currentCluster,
                         roStoreDefWithoutReplication,
                         rebalanceClient.getAdminClient(),
                         true);

            populateData(currentCluster,
                         rwStoreDefWithoutReplication,
                         rebalanceClient.getAdminClient(),
                         false);

            rebalanceAndCheck(currentCluster,
                              targetCluster,
                              storeDefWithoutReplication,
                              rebalanceClient,
                              Arrays.asList(1));

            checkConsistentMetadata(targetCluster, serverList);
        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    @Test(timeout = 60000)
    public void testRORWRebalanceWithReplication() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6 }, { 7, 8 } });

        Cluster targetCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                    1,
                                                                    Lists.newArrayList(2, 3));

        // start servers 0 , 1 only
        List<Integer> serverList = Arrays.asList(0, 1);
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
            // Populate the two stores
            populateData(currentCluster,
                         roStoreDefWithReplication,
                         rebalanceClient.getAdminClient(),
                         true);

            populateData(currentCluster,
                         rwStoreDefWithReplication,
                         rebalanceClient.getAdminClient(),
                         false);

            rebalanceAndCheck(currentCluster,
                              targetCluster,
                              storeDefWithReplication,
                              rebalanceClient,
                              Arrays.asList(0, 1));
            checkConsistentMetadata(targetCluster, serverList);
        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    @Test(timeout = 60000)
    public void testRORebalanceWithReplication() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6 }, { 7, 8 } });

        Cluster targetCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                    1,
                                                                    Lists.newArrayList(2, 3));

        // start servers 0 , 1 only
        List<Integer> serverList = Arrays.asList(0, 1);

        // If this test fails, consider increasing the number of admin threads.
        // In particular, if this test fails by RejectedExecutionHandler in
        // SocketServer.java fires with an error message like the following:
        // "[18:46:32,994 voldemort.server.socket.SocketServer[admin-server]]
        // ERROR Too many open connections, 20 of 20 threads in use, denying
        // connection from /127.0.0.1:43756 [Thread-552]". Note, this issues
        // seems to only affect ThreadPoolBasedNonblockingStoreImpl tests rather
        // than Nio-based tests.
        Map<String, String> configProps = new HashMap<String, String>();
        configProps.put("admin.max.threads", "50");
        currentCluster = startServers(currentCluster,
                                      roStoreDefFileWithReplication,
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
            populateData(currentCluster,
                         roStoreDefWithReplication,
                         rebalanceClient.getAdminClient(),
                         true);

            rebalanceAndCheck(currentCluster,
                              targetCluster,
                              Lists.newArrayList(roStoreDefWithReplication),
                              rebalanceClient,
                              Arrays.asList(0, 1));
            checkConsistentMetadata(targetCluster, serverList);
        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    @Test(timeout = 60000)
    public void testRWRebalanceWithReplication() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6 }, { 7, 8 } });

        Cluster targetCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                    1,
                                                                    Lists.newArrayList(2, 3));

        // start servers 0 , 1 only
        List<Integer> serverList = Arrays.asList(0, 1);
        currentCluster = startServers(currentCluster,
                                      rwStoreDefFileWithReplication,
                                      serverList,
                                      null);
        // Update the cluster information based on the node information
        targetCluster = updateCluster(targetCluster);

        RebalanceClientConfig config = new RebalanceClientConfig();
        config.setDeleteAfterRebalancingEnabled(true);
        config.setStealerBasedRebalancing(!useDonorBased());
        RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(currentCluster,
                                                                                      0),
                                                                      config);
        try {
            populateData(currentCluster,
                         rwStoreDefWithReplication,
                         rebalanceClient.getAdminClient(),
                         false);

            rebalanceAndCheck(currentCluster,
                              targetCluster,
                              Lists.newArrayList(rwStoreDefWithReplication),
                              rebalanceClient,
                              Arrays.asList(0, 1));
            checkConsistentMetadata(targetCluster, serverList);
        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    @Test(timeout = 60000)
    public void testRebalanceCleanPrimary() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0 }, { 1, 3 },
                { 2 } });

        Cluster targetCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                    2,
                                                                    Lists.newArrayList(3));

        // start servers 0 , 1, 2
        Map<String, String> configProps = new HashMap<String, String>();
        configProps.put("enable.repair", "true");
        List<Integer> serverList = Arrays.asList(0, 1, 2);
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
            populateData(currentCluster,
                         rwStoreDefWithReplication,
                         rebalanceClient.getAdminClient(),
                         false);

            // Figure out the positive and negative keys to check
            ByteArray[] checkKeysNegative = new ByteArray[20];
            List<Integer> movedPartitions = new ArrayList<Integer>();
            movedPartitions.add(3);
            AdminClient admin = rebalanceClient.getAdminClient();
            Iterator<ByteArray> keys = null;
            keys = admin.fetchKeys(1,
                                   rwStoreDefWithReplication.getName(),
                                   movedPartitions,
                                   null,
                                   false);
            int keyIndex = 0;
            while(keys.hasNext() && keyIndex < 20) {
                checkKeysNegative[keyIndex++] = keys.next();
            }
            ByteArray[] checkKeysPositive = new ByteArray[20];
            List<Integer> stablePartitions = new ArrayList<Integer>();
            stablePartitions.add(1);
            Iterator<ByteArray> keys2 = null;
            keys2 = admin.fetchKeys(1,
                                    rwStoreDefWithReplication.getName(),
                                    stablePartitions,
                                    null,
                                    false);
            int keyIndex2 = 0;
            while(keys2.hasNext() && keyIndex2 < 20) {
                checkKeysPositive[keyIndex2++] = keys2.next();
            }

            rebalanceAndCheck(currentCluster,
                              targetCluster,
                              Lists.newArrayList(rwStoreDefWithReplication),
                              rebalanceClient,
                              Arrays.asList(0, 1, 2));
            checkConsistentMetadata(targetCluster, serverList);

            // Do the cleanup operation

            for(int i = 0; i < 3; i++) {
                admin.repairJob(i);
            }

            boolean cleanNode = true;
            for(int i = 0; i < keyIndex; i++) {
                KeyLocationValidation val = new KeyLocationValidation(targetCluster,
                                                                      1,
                                                                      rwStoreDefWithReplication,
                                                                      checkKeysNegative[i]);
                if(!val.validate(false))
                    cleanNode = false;
            }
            for(int i = 0; i < keyIndex2; i++) {
                KeyLocationValidation val = new KeyLocationValidation(targetCluster,
                                                                      1,
                                                                      rwStoreDefWithReplication,
                                                                      checkKeysPositive[i]);
                if(!val.validate(true))
                    cleanNode = false;
            }
            if(cleanNode)
                System.out.println("[Primary] Successful clean after Rebalancing");
            else
                System.out.println("[Primary] Rebalancing not clean");

        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    @Test(timeout = 60000)
    public void testRebalanceCleanSecondary() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(3, new int[][] { { 0, 3 }, { 1 },
                { 2 } });

        Cluster targetCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                    2,
                                                                    Lists.newArrayList(3));

        // start servers 0 , 1, 2
        Map<String, String> configProps = new HashMap<String, String>();
        configProps.put("enable.repair", "true");
        List<Integer> serverList = Arrays.asList(0, 1, 2);
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
            populateData(currentCluster,
                         rwStoreDefWithReplication,
                         rebalanceClient.getAdminClient(),
                         false);

            // Figure out the positive and negative keys to check
            ByteArray[] checkKeysNegative = new ByteArray[20];
            List<Integer> movedPartitions = new ArrayList<Integer>();
            movedPartitions.add(3);
            AdminClient admin = rebalanceClient.getAdminClient();
            Iterator<ByteArray> keys = null;
            keys = admin.fetchKeys(1,
                                   rwStoreDefWithReplication.getName(),
                                   movedPartitions,
                                   null,
                                   false);
            int keyIndex = 0;
            while(keys.hasNext() && keyIndex < 20) {
                checkKeysNegative[keyIndex++] = keys.next();
            }

            ByteArray[] checkKeysPositive = new ByteArray[20];
            List<Integer> stablePartitions = new ArrayList<Integer>();
            stablePartitions.add(3);
            Iterator<ByteArray> keys2 = null;
            keys2 = admin.fetchKeys(0,
                                    rwStoreDefWithReplication.getName(),
                                    stablePartitions,
                                    null,
                                    false);
            int keyIndex2 = 0;
            while(keys2.hasNext() && keyIndex2 < 20) {
                checkKeysPositive[keyIndex2++] = keys2.next();
            }

            rebalanceAndCheck(currentCluster,
                              targetCluster,
                              Lists.newArrayList(rwStoreDefWithReplication),
                              rebalanceClient,
                              Arrays.asList(0, 1, 2));
            checkConsistentMetadata(targetCluster, serverList);

            // Do the cleanup operation

            for(int i = 0; i < 3; i++) {
                admin.repairJob(i);
            }

            boolean cleanNode = true;
            for(int i = 0; i < keyIndex; i++) {
                KeyLocationValidation val = new KeyLocationValidation(targetCluster,
                                                                      1,
                                                                      rwStoreDefWithReplication,
                                                                      checkKeysNegative[i]);
                if(!val.validate(false))
                    cleanNode = false;
            }
            for(int i = 0; i < keyIndex2; i++) {
                KeyLocationValidation val = new KeyLocationValidation(targetCluster,
                                                                      0,
                                                                      rwStoreDefWithReplication,
                                                                      checkKeysPositive[i]);
                if(!val.validate(true))
                    cleanNode = false;
            }
            if(cleanNode)
                System.out.println("[Secondary] Successful clean after Rebalancing");
            else
                System.out.println("[Secondary] Rebalancing not clean");

        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    @Test(timeout = 60000)
    public void testRWRebalanceFourNodes() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                { 0, 1, 4, 7, 9 }, { 2, 3, 5, 6, 8 }, {}, {} });

        ArrayList<Node> nodes = Lists.newArrayList(currentCluster.getNodes());
        int totalPortNum = nodes.size() * 3;
        int[] ports = new int[totalPortNum];
        for(int i = 0; i < nodes.size(); i++) {
            ports[i * 3] = nodes.get(i).getHttpPort();
            ports[i * 3 + 1] = nodes.get(i).getSocketPort();
            ports[i * 3 + 2] = nodes.get(i).getAdminPort();
        }

        Cluster targetCluster = ServerTestUtils.getLocalCluster(4, ports, new int[][] {
                { 0, 4, 7 }, { 2, 8 }, { 1, 6 }, { 3, 5, 9 } });

        // start servers
        List<Integer> serverList = Arrays.asList(0, 1, 2, 3);
        currentCluster = startServers(currentCluster,
                                      rwTwoStoreDefFileWithReplication,
                                      serverList,
                                      null);
        // Update the cluster information based on the node information
        targetCluster = updateCluster(targetCluster);

        RebalanceClientConfig config = new RebalanceClientConfig();
        config.setDeleteAfterRebalancingEnabled(true);
        config.setStealerBasedRebalancing(!useDonorBased());
        config.setPrimaryPartitionBatchSize(100);
        config.setMaxParallelRebalancing(5);
        RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(currentCluster,
                                                                                      0),
                                                                      config);
        try {
            populateData(currentCluster,
                         rwStoreDefWithReplication,
                         rebalanceClient.getAdminClient(),
                         false);

            populateData(currentCluster,
                         rwStoreDefWithReplication2,
                         rebalanceClient.getAdminClient(),
                         false);

            rebalanceAndCheck(currentCluster,
                              targetCluster,
                              Lists.newArrayList(rwStoreDefWithReplication,
                                                 rwStoreDefWithReplication2),
                              rebalanceClient,
                              serverList);
            checkConsistentMetadata(targetCluster, serverList);
        } catch(Exception e) {
            fail(e.getMessage());
        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    @Test(timeout = 60000)
    public void testRWRebalanceSerial() throws Exception {
        Cluster currentCluster = ServerTestUtils.getLocalCluster(4, new int[][] {
                { 0, 1, 4, 7, 9 }, { 2, 3, 5, 6, 8 }, {}, {} });

        ArrayList<Node> nodes = Lists.newArrayList(currentCluster.getNodes());
        int totalPortNum = nodes.size() * 3;
        int[] ports = new int[totalPortNum];
        for(int i = 0; i < nodes.size(); i++) {
            ports[i * 3] = nodes.get(i).getHttpPort();
            ports[i * 3 + 1] = nodes.get(i).getSocketPort();
            ports[i * 3 + 2] = nodes.get(i).getAdminPort();
        }

        Cluster targetCluster = ServerTestUtils.getLocalCluster(4, ports, new int[][] {
                { 0, 4, 7 }, { 2, 8 }, { 1, 6 }, { 3, 5, 9 } });

        // start servers
        Map<String, String> serverProps = new HashMap<String, String>();
        serverProps.put("max.parallel.stores.rebalancing", String.valueOf(1));
        List<Integer> serverList = Arrays.asList(0, 1, 2, 3);
        currentCluster = startServers(currentCluster,
                                      rwTwoStoreDefFileWithReplication,
                                      serverList,
                                      serverProps);
        // Update the cluster information based on the node information
        targetCluster = updateCluster(targetCluster);

        RebalanceClientConfig config = new RebalanceClientConfig();
        config.setDeleteAfterRebalancingEnabled(true);
        config.setStealerBasedRebalancing(!useDonorBased());
        config.setPrimaryPartitionBatchSize(100);
        config.setMaxParallelRebalancing(5);
        RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(currentCluster,
                                                                                      0),
                                                                      config);
        try {
            populateData(currentCluster,
                         rwStoreDefWithReplication,
                         rebalanceClient.getAdminClient(),
                         false);

            populateData(currentCluster,
                         rwStoreDefWithReplication2,
                         rebalanceClient.getAdminClient(),
                         false);

            rebalanceAndCheck(currentCluster,
                              targetCluster,
                              Lists.newArrayList(rwStoreDefWithReplication,
                                                 rwStoreDefWithReplication2),
                              rebalanceClient,
                              serverList);
            checkConsistentMetadata(targetCluster, serverList);
        } catch(Exception e) {
            fail(e.getMessage());
        } finally {
            // stop servers
            stopServer(serverList);
        }
    }

    @Test(timeout = 60000)
    public void testProxyGetDuringRebalancing() throws Exception {
        final Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6 }, { 7, 8 } });

        final Cluster targetCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                          1,
                                                                          Lists.newArrayList(2, 3));
        // start servers 0 , 1 only
        final List<Integer> serverList = Arrays.asList(0, 1);
        Map<String, String> configProps = new HashMap<String, String>();
        configProps.put("admin.max.threads", "50");
        final Cluster updatedCurrentCluster = startServers(currentCluster,
                                                           storeDefFileWithReplication,
                                                           serverList,
                                                           configProps);
        final Cluster updatedTargetCluster = updateCluster(targetCluster);

        ExecutorService executors = Executors.newFixedThreadPool(2);
        final AtomicBoolean rebalancingToken = new AtomicBoolean(false);
        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

        RebalanceClientConfig rebalanceClientConfig = new RebalanceClientConfig();
        rebalanceClientConfig.setMaxParallelRebalancing(2);

        final RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCurrentCluster,
                                                                                            0),
                                                                            rebalanceClientConfig);

        // Populate the two stores
        populateData(updatedCurrentCluster,
                     roStoreDefWithReplication,
                     rebalanceClient.getAdminClient(),
                     true);

        populateData(updatedCurrentCluster,
                     rwStoreDefWithReplication,
                     rebalanceClient.getAdminClient(),
                     false);

        final SocketStoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(getBootstrapUrl(updatedCurrentCluster,
                                                                                                                                  0))
                                                                                                .setEnableLazy(false)
                                                                                                .setSocketTimeout(120,
                                                                                                                  TimeUnit.SECONDS));

        final StoreClient<String, String> storeClientRW = new DefaultStoreClient<String, String>(testStoreNameRW,
                                                                                                 null,
                                                                                                 factory,
                                                                                                 3);

        final StoreClient<String, String> storeClientRO = new DefaultStoreClient<String, String>(testStoreNameRO,
                                                                                                 null,
                                                                                                 factory,
                                                                                                 3);

        // start get operation.
        executors.execute(new Runnable() {

            public void run() {
                try {
                    List<String> keys = new ArrayList<String>(testEntries.keySet());

                    while(!rebalancingToken.get()) {
                        // should always able to get values.
                        int index = (int) (Math.random() * keys.size());

                        // should get a valid value
                        try {
                            Versioned<String> value = storeClientRW.get(keys.get(index));
                            assertNotSame("StoreClient get() should not return null.", null, value);
                            assertEquals("Value returned should be good",
                                         new Versioned<String>(testEntries.get(keys.get(index))),
                                         value);

                            value = storeClientRO.get(keys.get(index));
                            assertNotSame("StoreClient get() should not return null.", null, value);
                            assertEquals("Value returned should be good",
                                         new Versioned<String>(testEntries.get(keys.get(index))),
                                         value);

                        } catch(Exception e) {
                            e.printStackTrace();
                            exceptions.add(e);
                        }
                    }

                } catch(Exception e) {
                    exceptions.add(e);
                } finally {
                    factory.close();
                }
            }

        });

        executors.execute(new Runnable() {

            public void run() {
                try {

                    Thread.sleep(500);
                    rebalanceAndCheck(updatedCurrentCluster,
                                      updatedTargetCluster,
                                      storeDefWithReplication,
                                      rebalanceClient,
                                      Arrays.asList(0, 1));
                    Thread.sleep(500);
                    rebalancingToken.set(true);
                    checkConsistentMetadata(updatedTargetCluster, serverList);

                } catch(Exception e) {
                    exceptions.add(e);
                } finally {
                    // stop servers
                    try {
                        stopServer(serverList);
                    } catch(Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });

        executors.shutdown();
        executors.awaitTermination(300, TimeUnit.SECONDS);

        // check No Exception
        if(exceptions.size() > 0) {
            for(Exception e: exceptions) {
                e.printStackTrace();
            }
            fail("Should not see any exceptions.");
        }
    }

    @Test(timeout = 60000)
    public void testServerSideRouting() throws Exception {
        final Cluster currentCluster = ServerTestUtils.getLocalCluster(2, new int[][] {
                { 0, 1, 2, 3, 4, 5, 6 }, { 7, 8 } });

        final Cluster targetCluster = RebalanceUtils.createUpdatedCluster(currentCluster,
                                                                          1,
                                                                          Lists.newArrayList(2, 3));

        final List<Integer> serverList = Arrays.asList(0, 1);
        Map<String, String> configProps = new HashMap<String, String>();
        configProps.put("admin.max.threads", "50");
        final Cluster updatedCurrentCluster = startServers(currentCluster,
                                                           storeDefFileWithReplication,
                                                           serverList,
                                                           configProps);
        final Cluster updatedTargetCluster = updateCluster(targetCluster);

        ExecutorService executors = Executors.newFixedThreadPool(2);
        final AtomicBoolean rebalancingToken = new AtomicBoolean(false);
        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

        // populate data now.
        RebalanceClientConfig rebalanceClientConfig = new RebalanceClientConfig();
        rebalanceClientConfig.setMaxParallelRebalancing(2);

        final RebalanceController rebalanceClient = new RebalanceController(getBootstrapUrl(updatedCurrentCluster,
                                                                                            0),
                                                                            rebalanceClientConfig);

        // Populate the two stores
        populateData(updatedCurrentCluster,
                     roStoreDefWithReplication,
                     rebalanceClient.getAdminClient(),
                     true);

        populateData(updatedCurrentCluster,
                     rwStoreDefWithReplication,
                     rebalanceClient.getAdminClient(),
                     false);

        Node node = updatedCurrentCluster.getNodeById(1);
        final Store<ByteArray, byte[], byte[]> serverSideRoutingStoreRW = getSocketStore(testStoreNameRW,
                                                                                         node.getHost(),
                                                                                         node.getSocketPort(),
                                                                                         true);
        final Store<ByteArray, byte[], byte[]> serverSideRoutingStoreRO = getSocketStore(testStoreNameRO,
                                                                                         node.getHost(),
                                                                                         node.getSocketPort(),
                                                                                         true);

        final CountDownLatch latch = new CountDownLatch(1);

        // start get operation.
        executors.execute(new Runnable() {

            public void run() {
                try {
                    List<String> keys = new ArrayList<String>(testEntries.keySet());

                    while(!rebalancingToken.get()) {
                        // should always able to get values.
                        int index = (int) (Math.random() * keys.size());

                        // should get a valid value
                        try {
                            List<Versioned<byte[]>> values = serverSideRoutingStoreRW.get(new ByteArray(ByteUtils.getBytes(keys.get(index),
                                                                                                                           "UTF-8")),
                                                                                          null);

                            assertEquals("serverSideRoutingStore should return value.",
                                         1,
                                         values.size());
                            assertEquals("Value returned should be good",
                                         new Versioned<String>(testEntries.get(keys.get(index))),
                                         new Versioned<String>(ByteUtils.getString(values.get(0)
                                                                                         .getValue(),
                                                                                   "UTF-8"),
                                                               values.get(0).getVersion()));
                            values = serverSideRoutingStoreRO.get(new ByteArray(ByteUtils.getBytes(keys.get(index),
                                                                                                   "UTF-8")),
                                                                  null);

                            assertEquals("serverSideRoutingStore should return value.",
                                         1,
                                         values.size());
                            assertEquals("Value returned should be good",
                                         new Versioned<String>(testEntries.get(keys.get(index))),
                                         new Versioned<String>(ByteUtils.getString(values.get(0)
                                                                                         .getValue(),
                                                                                   "UTF-8"),
                                                               values.get(0).getVersion()));

                        } catch(UnreachableStoreException e) {
                            // ignore
                        } catch(Exception e) {
                            exceptions.add(e);
                        }
                    }

                    latch.countDown();
                } catch(Exception e) {
                    exceptions.add(e);
                }
            }

        });

        executors.execute(new Runnable() {

            public void run() {
                try {
                    Thread.sleep(500);
                    rebalanceAndCheck(updatedCurrentCluster,
                                      updatedTargetCluster,
                                      storeDefWithReplication,
                                      rebalanceClient,
                                      Arrays.asList(0, 1));

                    Thread.sleep(500);
                    rebalancingToken.set(true);
                    checkConsistentMetadata(targetCluster, serverList);
                } catch(Exception e) {
                    exceptions.add(e);
                } finally {
                    // stop servers as soon as the client thread has exited its
                    // loop.
                    try {
                        latch.await(300, TimeUnit.SECONDS);
                        stopServer(serverList);
                    } catch(Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });

        executors.shutdown();
        executors.awaitTermination(300, TimeUnit.SECONDS);

        // check No Exception
        if(exceptions.size() > 0) {
            for(Exception e: exceptions) {
                e.printStackTrace();
            }
            fail("Should not see any exceptions !!");
        }
    }

    protected void populateData(Cluster cluster,
                                StoreDefinition storeDef,
                                AdminClient adminClient,
                                boolean isReadOnly) throws Exception {

        // Populate Read write stores
        if(!isReadOnly) {
            // Create SocketStores for each Node first
            Map<Integer, Store<ByteArray, byte[], byte[]>> storeMap = new HashMap<Integer, Store<ByteArray, byte[], byte[]>>();
            for(Node node: cluster.getNodes()) {
                storeMap.put(node.getId(),
                             getSocketStore(storeDef.getName(),
                                            node.getHost(),
                                            node.getSocketPort()));

            }

            RoutingStrategy routing = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                         cluster);
            for(Entry<String, String> entry: testEntries.entrySet()) {
                ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(entry.getKey(), "UTF-8"));
                List<Integer> preferenceNodes = RebalanceUtils.getNodeIds(routing.routeRequest(keyBytes.get()));

                // Go over every node
                for(int nodeId: preferenceNodes) {
                    try {
                        storeMap.get(nodeId)
                                .put(keyBytes,
                                     new Versioned<byte[]>(ByteUtils.getBytes(entry.getValue(),
                                                                              "UTF-8")),
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

        } else {
            // Populate Read only stores

            File baseDir = TestUtils.createTempDir();
            JsonReader reader = ReadOnlyStorageEngineTestInstance.makeTestDataReader(testEntries,
                                                                                     baseDir);

            RoutingStrategy router = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                        cluster);

            File outputDir = TestUtils.createTempDir(baseDir);
            JsonStoreBuilder storeBuilder = new JsonStoreBuilder(reader,
                                                                 cluster,
                                                                 storeDef,
                                                                 router,
                                                                 outputDir,
                                                                 null,
                                                                 testEntries.size() / 5,
                                                                 1,
                                                                 NUM_RO_CHUNKS_PER_BUCKET,
                                                                 10000,
                                                                 false);
            storeBuilder.build(ReadOnlyStorageFormat.READONLY_V2);

            AdminStoreSwapper swapper = new AdminStoreSwapper(cluster,
                                                              Executors.newFixedThreadPool(cluster.getNumberOfNodes()),
                                                              adminClient,
                                                              100000);
            swapper.swapStoreData(testStoreNameRO, outputDir.getAbsolutePath(), 1L);
        }
    }

    protected String getBootstrapUrl(Cluster cluster, int nodeId) {
        Node node = cluster.getNodeById(nodeId);
        return "tcp://" + node.getHost() + ":" + node.getSocketPort();
    }

    private void rebalanceAndCheck(Cluster currentCluster,
                                   Cluster targetCluster,
                                   List<StoreDefinition> storeDefs,
                                   RebalanceController rebalanceClient,
                                   List<Integer> nodeCheckList) {
        rebalanceClient.rebalance(targetCluster);

        for(StoreDefinition storeDef: storeDefs) {
            Map<Integer, Set<Pair<Integer, Integer>>> currentNodeToPartitionTuples = RebalanceUtils.getNodeIdToAllPartitions(currentCluster,
                                                                                                                             storeDef,
                                                                                                                             true);
            Map<Integer, Set<Pair<Integer, Integer>>> targetNodeToPartitionTuples = RebalanceUtils.getNodeIdToAllPartitions(targetCluster,
                                                                                                                            storeDef,
                                                                                                                            true);

            for(int nodeId: nodeCheckList) {
                Set<Pair<Integer, Integer>> currentPartitionTuples = currentNodeToPartitionTuples.get(nodeId);
                Set<Pair<Integer, Integer>> targetPartitionTuples = targetNodeToPartitionTuples.get(nodeId);

                HashMap<Integer, List<Integer>> flattenedPresentTuples = RebalanceUtils.flattenPartitionTuples(Utils.getAddedInTarget(currentPartitionTuples,
                                                                                                                                      targetPartitionTuples));
                Store<ByteArray, byte[], byte[]> store = getSocketStore(storeDef.getName(),
                                                                        targetCluster.getNodeById(nodeId)
                                                                                     .getHost(),
                                                                        targetCluster.getNodeById(nodeId)
                                                                                     .getSocketPort());
                checkGetEntries(targetCluster.getNodeById(nodeId),
                                targetCluster,
                                storeDef,
                                store,
                                flattenedPresentTuples);
            }
        }

    }

    private void checkGetEntries(Node node,
                                 Cluster cluster,
                                 StoreDefinition def,
                                 Store<ByteArray, byte[], byte[]> store,
                                 HashMap<Integer, List<Integer>> flattenedPresentTuples) {
        RoutingStrategy routing = new RoutingStrategyFactory().updateRoutingStrategy(def, cluster);

        for(Entry<String, String> entry: testEntries.entrySet()) {
            ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(entry.getKey(), "UTF-8"));

            List<Integer> partitions = routing.getPartitionList(keyBytes.get());

            if(RebalanceUtils.checkKeyBelongsToPartition(partitions,
                                                         node.getPartitionIds(),
                                                         flattenedPresentTuples)) {
                List<Versioned<byte[]>> values = store.get(keyBytes, null);

                // expecting exactly one version
                if(values.size() == 0) {
                    fail("unable to find value for key=" + entry.getKey() + " on node="
                         + node.getId());
                }
                assertEquals("Expecting exactly one version", 1, values.size());
                Versioned<byte[]> value = values.get(0);
                // check version matches (expecting base version for all)
                assertEquals("Value version should match", new VectorClock(), value.getVersion());
                // check value matches.
                assertEquals("Value bytes should match",
                             entry.getValue(),
                             ByteUtils.getString(value.getValue(), "UTF-8"));

            }
        }

    }
}