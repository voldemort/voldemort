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

package voldemort.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ConsistencyCheck.ClusterNode;
import voldemort.utils.ConsistencyCheck.HashedValue;
import voldemort.utils.ConsistencyCheck.KeyFetchTracker;
import voldemort.utils.ConsistencyCheck.Reporter;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class ConsistencyCheckTest {

    final String STORE_NAME = "consistency-check";
    final String STORES_XML = "test/common/voldemort/config/consistency-stores.xml";

    Node n1 = new Node(1, "localhost", 10000, 10001, 10002, 0, new ArrayList<Integer>());
    Node n1_dup = new Node(1, "localhost", 10000, 10001, 10002, 0, new ArrayList<Integer>());
    Node n2 = new Node(2, "localhost", 10000, 10001, 10002, 0, new ArrayList<Integer>());
    Node n3 = new Node(3, "localhost", 10000, 10001, 10002, 0, new ArrayList<Integer>());
    Node n4 = new Node(4, "localhost", 10000, 10001, 10002, 0, new ArrayList<Integer>());
    ClusterNode cn0_1 = new ClusterNode(0, n1);
    ClusterNode cn0_1_dup = new ClusterNode(0, n1);
    ClusterNode cn1_1dup = new ClusterNode(1, n1_dup);
    ClusterNode cn0_2 = new ClusterNode(0, n2);
    ClusterNode cn0_3 = new ClusterNode(0, n3);
    ClusterNode cn0_4 = new ClusterNode(0, n4);
    ClusterNode cn1_2 = new ClusterNode(1, n2); // 1.1

    long now = System.currentTimeMillis();

    byte[] value1 = { 0, 1, 2, 3, 4 };
    byte[] value2 = { 0, 1, 2, 3, 5 };
    byte[] value3 = { 0, 1, 2, 3, 6 };
    byte[] value4 = { 0, 1, 2, 3, 7 };
    VectorClock vc1 = new VectorClock(now - Time.MS_PER_DAY);
    VectorClock vc2 = new VectorClock(now);
    VectorClock vc3 = new VectorClock(now - Time.MS_PER_HOUR * 24 + 500 * Time.MS_PER_SECOND);
    Versioned<byte[]> versioned1 = new Versioned<byte[]>(value1, vc1);
    Versioned<byte[]> versioned2 = new Versioned<byte[]>(value2, vc2);
    Versioned<byte[]> versioned3 = new Versioned<byte[]>(value3, vc3);
    ConsistencyCheck.Value hv1 = new ConsistencyCheck.HashedValue(versioned1);
    ConsistencyCheck.Value hv1_dup = new ConsistencyCheck.HashedValue(versioned1);
    ConsistencyCheck.Value hv2 = new ConsistencyCheck.HashedValue(versioned2);

    ConsistencyCheck.Value hv3 = new ConsistencyCheck.HashedValue(new Versioned<byte[]>(value1,vc3));

    // make set
    Set<ConsistencyCheck.ClusterNode> setFourNodes = new HashSet<ConsistencyCheck.ClusterNode>();
    Set<ConsistencyCheck.ClusterNode> setThreeNodes = new HashSet<ConsistencyCheck.ClusterNode>();

    @Before
    public void setUp() {
        setFourNodes.add(cn0_1);
        setFourNodes.add(cn0_2);
        setFourNodes.add(cn0_3);
        setFourNodes.add(cn0_4);
        setThreeNodes.add(cn0_1);
        setThreeNodes.add(cn0_2);
        setThreeNodes.add(cn0_3);
    }

    @Test
    public void testClusterNode() {

        // test getter
        assertEquals(cn0_1.getNode(), n1);
        assertEquals(cn1_1dup.getNode(), n1_dup);
        assertEquals(cn0_2.getNode(), n2);
        assertEquals(new Integer(0), cn0_1.getPrefixId());
        assertEquals(new Integer(1), cn1_1dup.getPrefixId());
        assertEquals(new Integer(0), cn0_2.getPrefixId());

        // test equals function
        assertTrue(cn0_1.equals(cn0_1_dup));
        assertFalse(cn1_1dup.equals(cn0_1));
        assertFalse(cn0_2.equals(cn0_1));
        assertFalse(cn0_2.equals(cn1_1dup));

        // test toString function
        assertEquals("0.1", cn0_1.toString());
        assertEquals("1.1", cn1_1dup.toString());
        assertEquals("0.2", cn0_2.toString());
    }

    @Test
    public void testHashedValue() {

        assertTrue(hv1.equals(hv1_dup));
        assertEquals(hv1.hashCode(), hv1_dup.hashCode());
        assertFalse(hv1.hashCode() == hv2.hashCode());
        assertFalse(hv1.equals(hv2));
        assertFalse(hv1.equals(null));
        assertFalse(hv1.equals(new Versioned<byte[]>(null)));
        assertFalse(hv1.equals(new Integer(0)));
    }

    @Test
    public void testRetentionChecker() {
        ConsistencyCheck.RetentionChecker rc1 = new ConsistencyCheck.RetentionChecker(0);
        ConsistencyCheck.RetentionChecker rc2 = new ConsistencyCheck.RetentionChecker(1);

        // Test HashedValue timestamp
        assertFalse(rc1.isExpired(hv3));
        assertTrue(rc2.isExpired(hv3));

        // Test VersionValue time stamp
        assertFalse(rc1.isExpired(new ConsistencyCheck.VersionValue(versioned1)));
        assertFalse(rc1.isExpired(new ConsistencyCheck.VersionValue(versioned2)));
        assertFalse(rc1.isExpired(new ConsistencyCheck.VersionValue(versioned3)));
        assertTrue (rc2.isExpired(new ConsistencyCheck.VersionValue(versioned1)));
        assertFalse(rc2.isExpired(new ConsistencyCheck.VersionValue(versioned2)));
        assertTrue (rc2.isExpired(new ConsistencyCheck.VersionValue(versioned3)));
    }

    @Test
    public void testDetermineConsistencyVectorClock() {
        Map<ConsistencyCheck.Value, Set<ConsistencyCheck.ClusterNode>> versionNodeSetMap = new HashMap<ConsistencyCheck.Value, Set<ConsistencyCheck.ClusterNode>>();
        int replicationFactor = 4;

        // Version is vector clock
        VectorClock vc1 = new VectorClock();
        vc1.incrementVersion(1, 100000001);
        vc1.incrementVersion(2, 100000003);

        VectorClock vc2= new VectorClock();
        vc2.incrementVersion(1, 100000001);
        vc2.incrementVersion(3, 100000002);
        VectorClock vc3 = new VectorClock();
        vc3.incrementVersion(1, 100000001);
        vc3.incrementVersion(4, 100000001);

        ConsistencyCheck.Value v1 = new ConsistencyCheck.VersionValue(new Versioned<byte[]>(value1, vc1));
        ConsistencyCheck.Value v2 = new ConsistencyCheck.VersionValue(new Versioned<byte[]>(value2, vc2));
        ConsistencyCheck.Value v3 = new ConsistencyCheck.VersionValue(new Versioned<byte[]>(value3, vc3));

        // FULL: simple
        versionNodeSetMap.put(v1, setFourNodes);
        assertEquals(ConsistencyCheck.ConsistencyLevel.FULL,
                     ConsistencyCheck.determineConsistency(versionNodeSetMap, replicationFactor));

        // FULL: three versions
        versionNodeSetMap.clear();
        versionNodeSetMap.put(v1, setFourNodes);
        versionNodeSetMap.put(v2, setFourNodes);
        versionNodeSetMap.put(v3, setFourNodes);
        assertEquals(ConsistencyCheck.ConsistencyLevel.FULL,
                     ConsistencyCheck.determineConsistency(versionNodeSetMap, replicationFactor));

        // LATEST_CONSISTENCY: two versions
        versionNodeSetMap.clear();
        versionNodeSetMap.put(v1, setFourNodes);
        versionNodeSetMap.put(v2, setThreeNodes);
        assertEquals(ConsistencyCheck.ConsistencyLevel.LATEST_CONSISTENT,
                     ConsistencyCheck.determineConsistency(versionNodeSetMap, replicationFactor));

        // INCONSISTENT: one version
        versionNodeSetMap.clear();
        versionNodeSetMap.put(v1, setThreeNodes);
        assertEquals(ConsistencyCheck.ConsistencyLevel.INCONSISTENT,
                     ConsistencyCheck.determineConsistency(versionNodeSetMap, replicationFactor));

        // INCONSISTENT: non-latest consistent
        versionNodeSetMap.clear();
        versionNodeSetMap.put(v1, setThreeNodes);
        versionNodeSetMap.put(v2, setFourNodes);
        assertEquals(ConsistencyCheck.ConsistencyLevel.INCONSISTENT,
                     ConsistencyCheck.determineConsistency(versionNodeSetMap, replicationFactor));

        // INCONSISTENT: three versions
        versionNodeSetMap.clear();
        versionNodeSetMap.put(v1, setThreeNodes);
        versionNodeSetMap.put(v2, setFourNodes);
        versionNodeSetMap.put(v3, setThreeNodes);
        assertEquals(ConsistencyCheck.ConsistencyLevel.INCONSISTENT,
                     ConsistencyCheck.determineConsistency(versionNodeSetMap, replicationFactor));
    }

    @Test
    public void testDetermineConsistencyHashValue() {
        Map<ConsistencyCheck.Value, Set<ConsistencyCheck.ClusterNode>> versionNodeSetMap = new HashMap<ConsistencyCheck.Value, Set<ConsistencyCheck.ClusterNode>>();
        int replicationFactor = 4;

        // vector clocks
        Version v1 = new VectorClock();
        ((VectorClock) v1).incrementVersion(1, 100000001);
        ((VectorClock) v1).incrementVersion(2, 100000003);
        Version v2 = new VectorClock();
        ((VectorClock) v2).incrementVersion(1, 100000001);
        ((VectorClock) v2).incrementVersion(3, 100000002);
        Version v3 = new VectorClock();
        ((VectorClock) v3).incrementVersion(1, 100000001);
        ((VectorClock) v3).incrementVersion(4, 100000001);

        // Version is HashedValue
        Versioned<byte[]> versioned1 = new Versioned<byte[]>(value1, v1);
        Versioned<byte[]> versioned2 = new Versioned<byte[]>(value2, v2);
        Versioned<byte[]> versioned3 = new Versioned<byte[]>(value3, v3);
        ConsistencyCheck.Value hv1 = new ConsistencyCheck.HashedValue(versioned1);
        ConsistencyCheck.Value hv2 = new ConsistencyCheck.HashedValue(versioned2);
        ConsistencyCheck.Value hv3 = new ConsistencyCheck.HashedValue(versioned3);

        // FULL
        // one version
        versionNodeSetMap.clear();
        versionNodeSetMap.put(hv1, setFourNodes);
        assertEquals(ConsistencyCheck.ConsistencyLevel.FULL,
                     ConsistencyCheck.determineConsistency(versionNodeSetMap, replicationFactor));

        // three versions
        versionNodeSetMap.clear();
        versionNodeSetMap.put(hv1, setFourNodes);
        versionNodeSetMap.put(hv2, setFourNodes);
        versionNodeSetMap.put(hv3, setFourNodes);
        assertEquals(ConsistencyCheck.ConsistencyLevel.FULL,
                     ConsistencyCheck.determineConsistency(versionNodeSetMap, replicationFactor));

        // LATEST_CONSISTENT: not possible since timestamp is ignored

        // INCONSISTENT
        versionNodeSetMap.clear();
        versionNodeSetMap.put(hv1, setThreeNodes);
        assertEquals(ConsistencyCheck.ConsistencyLevel.INCONSISTENT,
                     ConsistencyCheck.determineConsistency(versionNodeSetMap, replicationFactor));

        versionNodeSetMap.clear();
        versionNodeSetMap.put(hv1, setFourNodes);
        versionNodeSetMap.put(hv2, setThreeNodes);
        assertEquals(ConsistencyCheck.ConsistencyLevel.LATEST_CONSISTENT,
                     ConsistencyCheck.determineConsistency(versionNodeSetMap, replicationFactor));
    }

    @Test
    public void testCleanInlegibleKeys() {
        // versions
        VectorClock vc1 = new VectorClock();
        vc1.incrementVersion(1, 100000001);
        vc1.incrementVersion(2, 100000003);
        VectorClock vc2 = new VectorClock();
        vc2.incrementVersion(1, 100000002);

        ConsistencyCheck.Value v1 = new ConsistencyCheck.VersionValue(new Versioned<byte[]>(value1, vc1));
        ConsistencyCheck.Value v2 = new ConsistencyCheck.VersionValue(new Versioned<byte[]>(value2, vc2));


        // setup
        Map<ByteArray, Map<ConsistencyCheck.Value, Set<ClusterNode>>> map = new HashMap<ByteArray, Map<ConsistencyCheck.Value, Set<ClusterNode>>>();
        Map<ConsistencyCheck.Value, Set<ClusterNode>> nodeSetMap = new HashMap<ConsistencyCheck.Value, Set<ClusterNode>>();
        Set<ClusterNode> oneNodeSet = new HashSet<ClusterNode>();
        oneNodeSet.add(cn0_1);
        Set<ClusterNode> twoNodeSet = new HashSet<ClusterNode>();
        twoNodeSet.add(cn0_1);
        twoNodeSet.add(cn0_2);
        int requiredWrite = 2;
        ByteArray key1 = new ByteArray(value1);

        // delete one key
        map.clear();
        nodeSetMap.clear();
        nodeSetMap.put(v1, oneNodeSet);
        map.put(key1, nodeSetMap);

        assertEquals(1, map.size());
        ConsistencyCheck.cleanIneligibleKeys(map, requiredWrite);
        assertEquals(0, map.size());

        // delete one version out of two versions
        map.clear();
        nodeSetMap.clear();
        nodeSetMap.put(v1, oneNodeSet);
        nodeSetMap.put(v2, twoNodeSet);
        map.put(key1, nodeSetMap);

        assertEquals(2, map.get(key1).size());
        ConsistencyCheck.cleanIneligibleKeys(map, requiredWrite);
        assertEquals(1, map.size());
        assertEquals(1, map.get(key1).size());

    }

    @Test
    public void testKeyVersionToString() {
        byte[] keyBytes = { 0, 1, 2, 17, 4 };
        ByteArray key = new ByteArray(keyBytes);
        long now = System.currentTimeMillis();
        VectorClock vc1 = new VectorClock(now);
        VectorClock vc2 = new VectorClock(now + 1);
        Versioned<byte[]> versioned = new Versioned<byte[]>(value1, vc1);

        ConsistencyCheck.Value v1 = new ConsistencyCheck.VersionValue(new Versioned<byte[]>(value1, vc1));
        ConsistencyCheck.Value v2 = new ConsistencyCheck.VersionValue(new Versioned<byte[]>(value2, vc2));


        // make Prefix Nodes
        Set<ClusterNode> set = new HashSet<ClusterNode>();
        set.add(cn0_1);
        set.add(cn1_2);
        set.add(cn0_3);

        // test vector clock
        Map<ConsistencyCheck.Value, Set<ClusterNode>> mapVector = new HashMap<ConsistencyCheck.Value, Set<ClusterNode>>();
        mapVector.put(v1, set);
        vc1.incrementVersion(1, now);
        v1 = new ConsistencyCheck.VersionValue(new Versioned<byte[]>(value1, vc1));
        String sVector = ConsistencyCheck.keyVersionToString(key, mapVector, "testStore", 99);
        assertEquals("BAD_KEY,testStore,99,0001021104," + set.toString().replace(", ", ";") + ","
                     + now + ",[1:1]", sVector);

        // test two lines
        vc2.incrementVersion(1, now);
        vc2.incrementVersion(1, now + 1);
        v2 = new ConsistencyCheck.VersionValue(new Versioned<byte[]>(value2, vc2));

        mapVector.put(v2, set);
        String sVector2 = ConsistencyCheck.keyVersionToString(key, mapVector, "testStore", 99);
        String s1 = "BAD_KEY,testStore,99,0001021104," + set.toString().replace(", ", ";") + ","
                    + now + ",[1:1]";

        String s2 = "BAD_KEY,testStore,99,0001021104," + set.toString().replace(", ", ";") + ","
                    + (now + 1) + ",[1:2]";
        assertTrue(sVector2.equals(s1 + s2) || sVector2.equals(s2 + s1));

        // test value hash
        ConsistencyCheck.Value v3 = new HashedValue(versioned);
        Map<ConsistencyCheck.Value, Set<ClusterNode>> mapHashed = new HashMap<ConsistencyCheck.Value, Set<ClusterNode>>();
        mapHashed.put(v3, set);
        assertEquals("BAD_KEY,testStore,99,0001021104," + set.toString().replace(", ", ";") + ","
                             + now + ",[1:1],-1172398097",
                     ConsistencyCheck.keyVersionToString(key, mapHashed, "testStore", 99));

    }

    @Test
    public void testKeyFetchTracker() {
        KeyFetchTracker tracker = new KeyFetchTracker(4);
        tracker.recordFetch(cn0_1, new ByteArray(value1));
        tracker.recordFetch(cn0_2, new ByteArray(value1));
        tracker.recordFetch(cn0_3, new ByteArray(value1));
        tracker.recordFetch(cn0_4, new ByteArray(value1));
        tracker.recordFetch(cn0_1, new ByteArray(value2));
        tracker.recordFetch(cn0_2, new ByteArray(value2));
        tracker.recordFetch(cn0_3, new ByteArray(value2));
        assertNull(tracker.nextFinished());
        tracker.recordFetch(cn0_4, new ByteArray(value2));
        assertEquals(new ByteArray(value1), tracker.nextFinished());
        assertNull(tracker.nextFinished());
        // multiple fetch on same node same key
        tracker.recordFetch(cn0_1, new ByteArray(value3));
        tracker.recordFetch(cn0_2, new ByteArray(value3));
        tracker.recordFetch(cn0_3, new ByteArray(value3));
        tracker.recordFetch(cn0_4, new ByteArray(value3));
        tracker.recordFetch(cn0_4, new ByteArray(value3));
        tracker.recordFetch(cn0_4, new ByteArray(value3));
        assertEquals(new ByteArray(value2), tracker.nextFinished());

        tracker.recordFetch(cn0_1, new ByteArray(value4));
        tracker.recordFetch(cn0_2, new ByteArray(value4));
        tracker.recordFetch(cn0_3, new ByteArray(value4));

        assertNull(tracker.nextFinished());

        tracker.finishAll();
        assertEquals(new ByteArray(value3), tracker.nextFinished());
        assertEquals(new ByteArray(value4), tracker.nextFinished());
        assertNull(tracker.nextFinished());
    }

    @Test
    public void testOnePartitionEndToEndBasedOnVersion() throws Exception {
        long now = System.currentTimeMillis();

        // setup four nodes with one store and one partition
        final SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                    10000,
                                                                                    100000,
                                                                                    32 * 1024);
        VoldemortServer[] servers = new VoldemortServer[4];
        int partitionMap[][] = { { 0 }, { 1 }, { 2 }, { 3 } };
        Cluster cluster = ServerTestUtils.startVoldemortCluster(4,
                                                                servers,
                                                                partitionMap,
                                                                socketStoreFactory,
                                                                true,
                                                                null,
                                                                STORES_XML,
                                                                new Properties());

        Node node = cluster.getNodeById(0);
        String bootstrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();
        AdminClient adminClient = new AdminClient(bootstrapUrl,
                                                  new AdminClientConfig(),
                                                  new ClientConfig());

        byte[] value = { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        byte[] value2 = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

        // make versions
        VectorClock vc1 = new VectorClock();
        VectorClock vc2 = new VectorClock();
        VectorClock vc3 = new VectorClock();
        vc1.incrementVersion(0, now); // [0:1]
        vc2.incrementVersion(1, now - 5000); // [1:1]
        vc3.incrementVersion(0, now - 89000000); // [0:1], over a day old

        ArrayList<Pair<ByteArray, Versioned<byte[]>>> n0store = new ArrayList<Pair<ByteArray, Versioned<byte[]>>>();
        ArrayList<Pair<ByteArray, Versioned<byte[]>>> n1store = new ArrayList<Pair<ByteArray, Versioned<byte[]>>>();
        ArrayList<Pair<ByteArray, Versioned<byte[]>>> n2store = new ArrayList<Pair<ByteArray, Versioned<byte[]>>>();
        ArrayList<Pair<ByteArray, Versioned<byte[]>>> n3store = new ArrayList<Pair<ByteArray, Versioned<byte[]>>>();
        ArrayList<ByteArray> keysHashedToPar0 = new ArrayList<ByteArray>();

        // find store
        Versioned<List<StoreDefinition>> storeDefinitions = adminClient.metadataMgmtOps.getRemoteStoreDefList(0);
        List<StoreDefinition> StoreDefitions = storeDefinitions.getValue();
        StoreDefinition storeDefinition = null;
        for(StoreDefinition def: StoreDefitions) {
            if(def.getName().equals(STORE_NAME)) {
                storeDefinition = def;
                break;
            }
        }
        assertNotNull("No such store found: " + STORE_NAME, storeDefinition);

        RoutingStrategy router = new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition,
                                                                                    cluster);
        while(keysHashedToPar0.size() < 7) {
            // generate random key
            Map<ByteArray, byte[]> map = ServerTestUtils.createRandomKeyValuePairs(1);
            ByteArray key = map.keySet().iterator().next();
            key.get()[0] = (byte) keysHashedToPar0.size();
            Integer masterPartition = router.getMasterPartition(key.get());
            if(masterPartition == 0) {
                keysHashedToPar0.add(key);
            } else {
                continue;
            }
        }
        ByteArray k6 = keysHashedToPar0.get(6);
        ByteArray k5 = keysHashedToPar0.get(5);
        ByteArray k4 = keysHashedToPar0.get(4);
        ByteArray k3 = keysHashedToPar0.get(3);
        ByteArray k2 = keysHashedToPar0.get(2);
        ByteArray k1 = keysHashedToPar0.get(1);
        ByteArray k0 = keysHashedToPar0.get(0);

        // insert K6 into node 0,1,2
        Versioned<byte[]> v6 = new Versioned<byte[]>(value, vc1);
        n0store.add(Pair.create(k6, v6));
        n1store.add(Pair.create(k6, v6));
        n2store.add(Pair.create(k6, v6));

        // insert K6(conflicting value and version) into node 0,1,2,3
        Versioned<byte[]> v6ConflictEarly = new Versioned<byte[]>(value2, vc2);
        n0store.add(Pair.create(k6, v6ConflictEarly));
        n1store.add(Pair.create(k6, v6ConflictEarly));
        n2store.add(Pair.create(k6, v6ConflictEarly));
        n3store.add(Pair.create(k6, v6ConflictEarly));

        // insert K4,K5 into four nodes
        Versioned<byte[]> v5 = new Versioned<byte[]>(value, vc1);
        Versioned<byte[]> v4 = new Versioned<byte[]>(value, vc1);
        n0store.add(Pair.create(k5, v5));
        n1store.add(Pair.create(k5, v5));
        n2store.add(Pair.create(k5, v5));
        n3store.add(Pair.create(k5, v5));
        n0store.add(Pair.create(k4, v4));
        n1store.add(Pair.create(k4, v4));
        n2store.add(Pair.create(k4, v4));
        n3store.add(Pair.create(k4, v4));

        // insert K3 into node 0,1,2
        Versioned<byte[]> v3 = new Versioned<byte[]>(value, vc2);
        n0store.add(Pair.create(k3, v3));
        n1store.add(Pair.create(k3, v3));
        n2store.add(Pair.create(k3, v3));

        // insert K3(conflicting but latest version) into node 0,1,2,3
        Versioned<byte[]> v3ConflictLate = new Versioned<byte[]>(value, vc1);
        n0store.add(Pair.create(k3, v3ConflictLate));
        n1store.add(Pair.create(k3, v3ConflictLate));
        n2store.add(Pair.create(k3, v3ConflictLate));
        n3store.add(Pair.create(k3, v3ConflictLate));

        // insert K2 into node 0,1
        Versioned<byte[]> v2 = new Versioned<byte[]>(value, vc1);
        n0store.add(Pair.create(k2, v2));
        n1store.add(Pair.create(k2, v2));

        // insert K1 into node 0
        Versioned<byte[]> v1 = new Versioned<byte[]>(value, vc1);
        n0store.add(Pair.create(k1, v1));

        // insert K0(out of retention) into node 0,1,2
        Versioned<byte[]> v0 = new Versioned<byte[]>(value, vc3);
        n0store.add(Pair.create(k0, v0));
        n1store.add(Pair.create(k0, v0));
        n2store.add(Pair.create(k0, v0));

        // stream to store
        adminClient.streamingOps.updateEntries(0, STORE_NAME, n0store.iterator(), null);
        adminClient.streamingOps.updateEntries(1, STORE_NAME, n1store.iterator(), null);
        adminClient.streamingOps.updateEntries(2, STORE_NAME, n2store.iterator(), null);
        adminClient.streamingOps.updateEntries(3, STORE_NAME, n3store.iterator(), null);

        // should have FULL:2(K4,K5), LATEST_CONSISTENT:1(K3),
        // INCONSISTENT:2(K6,K2), ignored(K1,K0)
        List<String> urls = new ArrayList<String>();
        urls.add(bootstrapUrl);
        ConsistencyCheck.ComparisonType[] comparisonTypes = ConsistencyCheck.ComparisonType.values();

        for(ConsistencyCheck.ComparisonType type : comparisonTypes)
        {
            StringWriter sw = new StringWriter();
            ConsistencyCheck checker = new ConsistencyCheck(urls, STORE_NAME, 0, sw, type);
            Reporter reporter = null;
            checker.connect();
            reporter = checker.execute();

            assertEquals(7 - 2, reporter.numTotalKeys);
            assertEquals(3, reporter.numGoodKeys);
        }
    }
}
