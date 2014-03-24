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

package voldemort.store.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.VoldemortException;
import voldemort.client.rebalance.RebalanceTaskInfo;
import voldemort.cluster.Cluster;
import voldemort.server.rebalance.RebalancerState;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.tools.admin.AddStoreTest;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Maps;

public class MetadataStoreTest {
    private Logger logger = Logger.getLogger(MetadataStore.class);

    public static String storesXmlWithBackwardIncompatibleSchema = "<stores>\n" + AddStoreTest.storeXmlWithBackwardIncompatibleSchema + "</stores>";

    public static String storesXmlWithBackwardCompatibleSchema = "<stores>\n" + AddStoreTest.storeXmlWithBackwardCompatibleSchema + "</stores>";

    private static int TEST_RUNS = 100;

    private MetadataStore metadataStore;
    private List<String> TEST_KEYS = Arrays.asList(MetadataStore.CLUSTER_KEY,
                                                   MetadataStore.STORES_KEY,
                                                   MetadataStore.REBALANCING_STEAL_INFO,
                                                   MetadataStore.SERVER_STATE_KEY,
                                                   MetadataStore.REBALANCING_SOURCE_CLUSTER_XML);

    @Before
    public void setUp() throws Exception {
        metadataStore = ServerTestUtils.createMetadataStore(ServerTestUtils.getLocalCluster(1),
                                                            ServerTestUtils.getStoreDefs(1));
    }

    public ByteArray getValidKey() {
        int i = (int) (Math.random() * TEST_KEYS.size());
        String key = TEST_KEYS.get(i);
        return new ByteArray(ByteUtils.getBytes(key, "UTF-8"));
    }

    public byte[] getValidValue(ByteArray key) {
        String keyString = ByteUtils.getString(key.get(), "UTF-8");
        if(MetadataStore.CLUSTER_KEY.equals(keyString)
           || MetadataStore.REBALANCING_SOURCE_CLUSTER_XML.equals(keyString)) {
            return ByteUtils.getBytes(new ClusterMapper().writeCluster(ServerTestUtils.getLocalCluster(1)),
                                      "UTF-8");
        } else if(MetadataStore.STORES_KEY.equals(keyString)) {
            return ByteUtils.getBytes(new StoreDefinitionsMapper().writeStoreList(ServerTestUtils.getStoreDefs(1)),
                                      "UTF-8");

        } else if(MetadataStore.SERVER_STATE_KEY.equals(keyString)) {
            int i = (int) (Math.random() * VoldemortState.values().length);
            return ByteUtils.getBytes(VoldemortState.values()[i].toString(), "UTF-8");
        } else if(MetadataStore.REBALANCING_STEAL_INFO.equals(keyString)) {
            int size = (int) (Math.random() * 10) + 1;
            List<Integer> partition = new ArrayList<Integer>();
            for(int i = 0; i < size; i++) {
                partition.add((int) Math.random() * 10);
            }

            List<Integer> partitionIds = partition;

            HashMap<String, List<Integer>> storeToReplicaToPartitionList = Maps.newHashMap();
            storeToReplicaToPartitionList.put("test", partitionIds);

            return ByteUtils.getBytes(new RebalancerState(Arrays.asList(new RebalanceTaskInfo(0,
                                                                                              (int) Math.random() * 5,
                                                                                              storeToReplicaToPartitionList,
                                                                                              ServerTestUtils.getLocalCluster(1)))).toJsonString(),
                                      "UTF-8");
        }

        throw new RuntimeException("Unhandled key:" + keyString + " passed");
    }

    @Test
    public void testSimpleGetAndPut() {
        for(int i = 0; i <= TEST_RUNS; i++) {
            ByteArray key = getValidKey();
            VectorClock clock = (VectorClock) metadataStore.get(key, null).get(0).getVersion();
            Versioned<byte[]> value = new Versioned<byte[]>(getValidValue(key),
                                                            clock.incremented(0, 1));

            metadataStore.put(key, value, null);
            checkValues(value, metadataStore.get(key, null), key);
        }
    }

    @Test
    public void testRepeatedPuts() {
        for(int i = 0; i <= TEST_RUNS; i++) {
            for(int j = 0; j <= 5; j++) {
                ByteArray key = getValidKey();

                VectorClock clock = (VectorClock) metadataStore.get(key, null).get(0).getVersion();
                Versioned<byte[]> value = new Versioned<byte[]>(getValidValue(key),
                                                                clock.incremented(0, 1));

                metadataStore.put(key, value, null);
                checkValues(value, metadataStore.get(key, null), key);
            }
        }
    }

    @Test
    public void testObsoletePut() {
        for(int i = 0; i <= TEST_RUNS; i++) {
            ByteArray key = getValidKey();
            VectorClock clock = (VectorClock) metadataStore.get(key, null).get(0).getVersion();
            Versioned<byte[]> value = new Versioned<byte[]>(getValidValue(key),
                                                            clock.incremented(0, 1));

            try {
                metadataStore.put(key, value, null);
                assertTrue(true);
                metadataStore.put(key, value, null);
                fail();
            } catch(ObsoleteVersionException e) {
                // expected ObsoleteVersionException
            }
        }
    }

    @Test
    public void testSynchronousPut() {
        for(int i = 0; i <= TEST_RUNS; i++) {
            ByteArray key = getValidKey();
            VectorClock clock = (VectorClock) metadataStore.get(key, null).get(0).getVersion();

            Versioned<byte[]> value1 = new Versioned<byte[]>(getValidValue(key),
                                                             clock.incremented(1, 1));
            Versioned<byte[]> value2 = new Versioned<byte[]>(getValidValue(key),
                                                             clock.incremented(2, 1));

            metadataStore.put(key, value1, null);
            metadataStore.put(key, value2, null);

            assertEquals("Only one metadata value should return", 1, metadataStore.get(key, null)
                                                                                  .size());
            checkValues(value2, metadataStore.get(key, null), key);
        }
    }

    @Test
    public void testCleanAllStates() {
        // put state entries.
        incrementVersionAndPut(metadataStore,
                               MetadataStore.SERVER_STATE_KEY,
                               MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER);

        assertEquals("Values should match.",
                     metadataStore.getServerStateUnlocked(),
                     VoldemortState.REBALANCING_MASTER_SERVER);

        // do clean
        metadataStore.cleanAllRebalancingState();

        // check all values revert back to default.
        assertEquals("Values should match.",
                     metadataStore.getServerStateUnlocked(),
                     VoldemortState.NORMAL_SERVER);
    }

    @Test
    public void testRebalacingSourceClusterXmlKey() {
        metadataStore.cleanAllRebalancingState();

        assertTrue("Should be null", null == metadataStore.getRebalancingSourceCluster());

        Cluster dummyCluster = ServerTestUtils.getLocalCluster(2);
        metadataStore.put(MetadataStore.REBALANCING_SOURCE_CLUSTER_XML, dummyCluster);
        assertEquals("Should be equal", dummyCluster, metadataStore.getRebalancingSourceCluster());

        metadataStore.put(MetadataStore.REBALANCING_SOURCE_CLUSTER_XML, (Object) null);
        assertTrue("Should be null", null == metadataStore.getRebalancingSourceCluster());

        List<Versioned<byte[]>> sourceClusterVersions = metadataStore.get(MetadataStore.REBALANCING_SOURCE_CLUSTER_XML,
                                                                          null);
        assertTrue("Just one version expected", 1 == sourceClusterVersions.size());
        assertEquals("Empty string should map to null",
                     "",
                     new String(sourceClusterVersions.get(0).getValue()));

    }

    /**
     * Test update stores.xml with incompatible avro versions. Should reject and throw exceptions
     */
    @Test
    public void testUpdateStoresXmlWithIncompatibleAvroSchema() {
        try{
            logger.info("Now inserting stores with non backward compatible schema. Should see exception");
            metadataStore.put(MetadataStore.STORES_KEY, new StoreDefinitionsMapper().readStoreList(new StringReader(storesXmlWithBackwardIncompatibleSchema)));
            Assert.fail("Did not throw exception");
        } catch(VoldemortException e) {

        }
        logger.info("Now inserting stores with backward compatible schema. Should not see exception");
        metadataStore.put(MetadataStore.STORES_KEY, new StoreDefinitionsMapper().readStoreList(new StringReader(storesXmlWithBackwardCompatibleSchema)));
    }

    private void checkValues(Versioned<byte[]> value, List<Versioned<byte[]>> list, ByteArray key) {
        assertEquals("should return exactly one value ", 1, list.size());

        assertEquals("should return the last saved version", value.getVersion(), list.get(0)
                                                                                     .getVersion());
        assertEquals("should return the last saved value (key:"
                     + ByteUtils.getString(key.get(), "UTF-8") + ")",
                     new String(value.getValue()),
                     new String(list.get(0).getValue()));
    }

    /**
     * helper function to auto update version and put()
     *
     * @param key
     * @param value
     */
    private void incrementVersionAndPut(MetadataStore metadataStore, String keyString, Object value) {
        ByteArray key = new ByteArray(ByteUtils.getBytes(keyString, "UTF-8"));
        VectorClock current = (VectorClock) metadataStore.getVersions(key).get(0);

        metadataStore.put(keyString,
                          new Versioned<Object>(value,
                                                current.incremented(0, System.currentTimeMillis())));
    }
}