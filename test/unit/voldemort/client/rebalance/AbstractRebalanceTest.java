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
package voldemort.client.rebalance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.QueryKeyResult;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.routing.StoreRoutingPlan;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;
import voldemort.utils.RebalanceUtils;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public abstract class AbstractRebalanceTest {

    Map<Integer, VoldemortServer> serverMap;

    protected final boolean useNio;
    protected final boolean useDonorBased;
    HashMap<String, String> testEntries;
    protected SocketStoreFactory socketStoreFactory;

    public AbstractRebalanceTest(boolean useNio, boolean useDonorBased) {
        this.useNio = useNio;
        this.useDonorBased = useDonorBased;
        this.serverMap = new HashMap<Integer, VoldemortServer>();
        testEntries = ServerTestUtils.createRandomKeyValueString(getNumKeys());
        socketStoreFactory = new ClientRequestExecutorPool(2, 10000, 100000, 32 * 1024);
    }

    // This method is susceptible to BindException issues due to TOCTOU
    // problem with getLocalCluster (which is used to construct cluster that is
    // passed in).
    // TODO: (refactor) AbstractRebalanceTest to take advantage of
    // ServerTestUtils.startVoldemortCluster.
    protected Cluster startServers(Cluster cluster,
                                   String storeXmlFile,
                                   List<Integer> nodeToStart,
                                   Map<String, String> configProps) throws Exception {
        for(int node: nodeToStart) {
            Properties properties = new Properties();
            if(null != configProps) {
                for(Entry<String, String> property: configProps.entrySet()) {
                    properties.put(property.getKey(), property.getValue());
                }
            }
            // turn proxy puts on
            properties.put("proxy.puts.during.rebalance", "true");

            VoldemortConfig config = ServerTestUtils.createServerConfig(useNio,
                                                                        node,
                                                                        TestUtils.createTempDir()
                                                                                 .getAbsolutePath(),
                                                                        null,
                                                                        storeXmlFile,
                                                                        properties);

            VoldemortServer server = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                                          config,
                                                                          cluster);
            serverMap.put(node, server);
        }

        return cluster;
    }

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

    protected VoldemortState getCurrentState(int nodeId) {
        VoldemortServer server = serverMap.get(nodeId);
        if(server == null) {
            throw new VoldemortException("Node id " + nodeId + " does not exist");
        } else {
            return server.getMetadataStore().getServerStateUnlocked();
        }
    }

    protected Cluster getCurrentCluster(int nodeId) {
        VoldemortServer server = serverMap.get(nodeId);
        if(server == null) {
            throw new VoldemortException("Node id " + nodeId + " does not exist");
        } else {
            return server.getMetadataStore().getCluster();
        }
    }

    public void checkConsistentMetadata(Cluster targetCluster, List<Integer> serverList) {
        for(int nodeId: serverList) {
            assertEquals(targetCluster, getCurrentCluster(nodeId));
            assertEquals(MetadataStore.VoldemortState.NORMAL_SERVER, getCurrentState(nodeId));
        }
    }

    protected void stopServer(List<Integer> nodesToStop) throws Exception {
        for(int node: nodesToStop) {
            try {
                ServerTestUtils.stopVoldemortServer(serverMap.get(node));
            } catch(VoldemortException e) {
                // ignore these at stop time
            }
        }
    }

    /**
     * This method determines the "size" of the test to run...
     * 
     * @return
     */
    protected abstract int getNumKeys();

    protected String getBootstrapUrl(Cluster cluster, int nodeId) {
        Node node = cluster.getNodeById(nodeId);
        return "tcp://" + node.getHost() + ":" + node.getSocketPort();
    }

    /**
     * Does the rebalance and then checks that it succeeded.
     * 
     * @param rebalancePlan
     * @param rebalanceClient
     * @param nodeCheckList
     */
    protected void rebalanceAndCheck(RebalancePlan rebalancePlan,
                                     RebalanceController rebalanceClient,
                                     List<Integer> nodeCheckList) {
        rebalanceClient.rebalance(rebalancePlan);
        checkEntriesPostRebalance(rebalancePlan.getCurrentCluster(),
                                  rebalancePlan.getFinalCluster(),
                                  rebalancePlan.getCurrentStores(),
                                  nodeCheckList,
                                  testEntries,
                                  null);
    }

    /**
     * Makes sure that all expected partition-stores are on each server after
     * the rebalance.
     * 
     * @param currentCluster
     * @param targetCluster
     * @param storeDefs
     * @param nodeCheckList
     * @param baselineTuples
     * @param baselineVersions
     */
    // TODO: (atomic cluster/store update) change from storeDefs to
    // currentStoreDefs and finalStoreDefs to
    // handle zone expansion/shrink tests.
    protected void checkEntriesPostRebalance(Cluster currentCluster,
                                             Cluster targetCluster,
                                             List<StoreDefinition> storeDefs,
                                             List<Integer> nodeCheckList,
                                             HashMap<String, String> baselineTuples,
                                             HashMap<String, VectorClock> baselineVersions) {
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
                                flattenedPresentTuples,
                                baselineTuples,
                                baselineVersions);
            }
        }
    }

    protected void checkGetEntries(Node node,
                                   Cluster cluster,
                                   StoreDefinition def,
                                   Store<ByteArray, byte[], byte[]> store,
                                   HashMap<Integer, List<Integer>> flattenedPresentTuples,
                                   HashMap<String, String> baselineTuples,
                                   HashMap<String, VectorClock> baselineVersions) {
        RoutingStrategy routing = new RoutingStrategyFactory().updateRoutingStrategy(def, cluster);

        for(Entry<String, String> entry: baselineTuples.entrySet()) {
            ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(entry.getKey(), "UTF-8"));

            List<Integer> partitions = routing.getPartitionList(keyBytes.get());

            if(StoreRoutingPlan.checkKeyBelongsToPartition(partitions,
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
                // check version matches
                if(baselineVersions == null) {
                    // expecting base version for all
                    assertEquals("Value version should match",
                                 new VectorClock(),
                                 value.getVersion());
                } else {
                    assertEquals("Value version should match",
                                 baselineVersions.get(entry.getKey()),
                                 value.getVersion());
                }

                // check value matches.
                assertEquals("Value bytes should match",
                             entry.getValue(),
                             ByteUtils.getString(value.getValue(), "UTF-8"));

            }
        }
    }

    protected List<ByteArray> sampleKeysFromPartition(AdminClient admin,
                                                      int serverId,
                                                      String store,
                                                      List<Integer> partitionsToSample,
                                                      int numSamples) {
        List<ByteArray> samples = new ArrayList<ByteArray>(numSamples);
        Iterator<ByteArray> keys = admin.bulkFetchOps.fetchKeys(serverId,
                                                                store,
                                                                partitionsToSample,
                                                                null,
                                                                false);
        int count = 0;
        while(keys.hasNext() && count < numSamples) {
            samples.add(keys.next());
            count++;
        }
        return samples;
    }

    /**
     * REFACTOR: these should belong AdminClient so existence checks can be done
     * easily across the board
     * 
     * @param admin
     * @param serverId
     * @param store
     * @param keyList
     */
    protected void checkForKeyExistence(AdminClient admin,
                                        int serverId,
                                        String store,
                                        List<ByteArray> keyList) {
        // do the positive tests
        Iterator<QueryKeyResult> positiveTestResultsItr = admin.streamingOps.queryKeys(serverId,
                                                                                       store,
                                                                                       keyList.iterator());
        while(positiveTestResultsItr.hasNext()) {
            QueryKeyResult item = positiveTestResultsItr.next();
            ByteArray key = item.getKey();
            List<Versioned<byte[]>> vals = item.getValues();
            Exception e = item.getException();

            assertEquals("Error fetching key " + key, null, e);
            assertEquals("Value not found for key " + key, true, vals != null & vals.size() != 0);

        }
    }

    /**
     * REFACTOR: these should belong AdminClient so existence checks can be done
     * easily across the board
     * 
     * @param admin
     * @param serverId
     * @param store
     * @param keyList
     */
    protected void checkForTupleEquivalence(AdminClient admin,
                                            int serverId,
                                            String store,
                                            List<ByteArray> keyList,
                                            HashMap<String, String> baselineTuples,
                                            HashMap<String, VectorClock> baselineVersions) {
        // do the positive tests
        Iterator<QueryKeyResult> positiveTestResultsItr = admin.streamingOps.queryKeys(serverId,
                                                                                       store,
                                                                                       keyList.iterator());
        while(positiveTestResultsItr.hasNext()) {
            QueryKeyResult item = positiveTestResultsItr.next();
            ByteArray key = item.getKey();
            List<Versioned<byte[]>> vals = item.getValues();
            Exception e = item.getException();

            assertEquals("Error fetching key " + key, null, e);
            assertEquals("Value not found for key " + key, true, vals != null & vals.size() != 0);

            String keyStr = ByteUtils.getString(key.get(), "UTF-8");
            if(baselineTuples != null)
                assertEquals("Value does not match up ",
                             baselineTuples.get(keyStr),
                             ByteUtils.getString(vals.get(0).getValue(), "UTF-8"));
            if(baselineVersions != null)
                assertEquals("Version does not match up",
                             baselineVersions.get(keyStr),
                             vals.get(0).getVersion());
        }
    }

    /**
     * REFACTOR: these should belong AdminClient so existence checks can be done
     * easily across the board
     * 
     * @param admin
     * @param serverId
     * @param store
     * @param keyList
     */
    protected void checkForKeyNonExistence(AdminClient admin,
                                           int serverId,
                                           String store,
                                           List<ByteArray> keyList) {
        Iterator<QueryKeyResult> negativeTestResultsItr = admin.streamingOps.queryKeys(serverId,
                                                                                       store,
                                                                                       keyList.iterator());
        while(negativeTestResultsItr.hasNext()) {
            QueryKeyResult item = negativeTestResultsItr.next();
            ByteArray key = item.getKey();
            List<Versioned<byte[]>> vals = item.getValues();
            Exception e = item.getException();

            assertEquals("Error fetching key " + key, null, e);
            assertEquals("Value " + vals + "found for key " + key,
                         true,
                         vals == null || vals.size() == 0);

        }
    }
}
