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

package voldemort.store.metadata;

import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.metadata.MetadataStore.ServerState;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

public class MetadataStoreTest extends TestCase {

    private static int TEST_RUNS = 100;

    private InMemoryStorageEngine<String, String> innerStore;
    private MetadataStore metadataStore;
    private List<String> TEST_KEYS = Arrays.asList(MetadataStore.CLUSTER_KEY,
                                                   MetadataStore.STORES_KEY,
                                                   MetadataStore.SERVER_STATE_KEY,
                                                   MetadataStore.REBALANCING_PARTITIONS_LIST,
                                                   MetadataStore.REBALANCING_PROXY_DEST);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        innerStore = new InMemoryStorageEngine<String, String>("inner-store");
        innerStore.put(MetadataStore.CLUSTER_KEY,
                       new Versioned<String>(new ClusterMapper().writeCluster(ServerTestUtils.getLocalCluster(1))));
        innerStore.put(MetadataStore.STORES_KEY,
                       new Versioned<String>(new StoreDefinitionsMapper().writeStoreList(ServerTestUtils.getStoreDefs(1))));

        metadataStore = new MetadataStore(innerStore, 0);
    }

    public ByteArray getValidKey() {
        int i = (int) (Math.random() * TEST_KEYS.size());
        return new ByteArray(ByteUtils.getBytes(TEST_KEYS.get(i), "UTF-8"));
    }

    public byte[] getValidValue(ByteArray key) {
        String keyString = ByteUtils.getString(key.get(), "UTF-8");
        if(MetadataStore.CLUSTER_KEY.equals(keyString)) {
            return ByteUtils.getBytes(new ClusterMapper().writeCluster(ServerTestUtils.getLocalCluster(1)),
                                      "UTF-8");
        } else if(MetadataStore.STORES_KEY.equals(keyString)) {
            return ByteUtils.getBytes(new StoreDefinitionsMapper().writeStoreList(ServerTestUtils.getStoreDefs(1)),
                                      "UTF-8");

        } else if(MetadataStore.SERVER_STATE_KEY.equals(keyString)) {
            int i = (int) (Math.random() * ServerState.values().length);
            return ByteUtils.getBytes(ServerState.values()[i].toString(), "UTF-8");
        } else if(MetadataStore.REBALANCING_PARTITIONS_LIST.equals(keyString)) {
            int size = (int) (Math.random() * 10);
            String partitionsList = "";
            for(int i = 0; i < size; i++) {
                partitionsList += "" + ((int) Math.random() * 100);
                if(i < size - 1)
                    partitionsList += ",";
            }
            return ByteUtils.getBytes(partitionsList, "UTF-8");
        } else if(MetadataStore.REBALANCING_PROXY_DEST.equals(keyString)) {
            return ByteUtils.getBytes("" + ((int) Math.random() * 100), "UTF-8");
        }

        throw new RuntimeException("Unhandled key:" + keyString + " passed");
    }

    public void testSimpleGetAndPut() {
        for(int i = 0; i <= TEST_RUNS; i++) {
            ByteArray key = getValidKey();
            VectorClock clock = (VectorClock) metadataStore.get(key).get(0).getVersion();
            Versioned<byte[]> value = new Versioned<byte[]>(getValidValue(key),
                                                            clock.incremented(0, 1));

            metadataStore.put(key, value);
            checkValues(value, metadataStore.get(key), key);
        }
    }

    public void testObsoletePut() {
        for(int i = 0; i <= TEST_RUNS; i++) {
            ByteArray key = getValidKey();
            VectorClock clock = (VectorClock) metadataStore.get(key).get(0).getVersion();
            Versioned<byte[]> value = new Versioned<byte[]>(getValidValue(key), clock);

            try {
                metadataStore.put(key, value);
                metadataStore.put(key, value);
                fail();
            } catch(ObsoleteVersionException e) {
                // expected
            }
        }
    }

    public void testSynchronousPut() {
        for(int i = 0; i <= TEST_RUNS; i++) {
            ByteArray key = getValidKey();
            VectorClock clock = (VectorClock) metadataStore.get(key).get(0).getVersion();

            Versioned<byte[]> value1 = new Versioned<byte[]>(getValidValue(key),
                                                             clock.incremented(1, 1));
            Versioned<byte[]> value2 = new Versioned<byte[]>(getValidValue(key),
                                                             clock.incremented(2, 1));

            metadataStore.put(key, value1);
            metadataStore.put(key, value2);

            assertEquals("Only one metadata value should return", 1, metadataStore.get(key).size());
            checkValues(value2, metadataStore.get(key), key);
        }
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
}
