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

package voldemort.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

/**
 * 
 * Check the Filter class parameter in AdminServer stream API's <br>
 * TODO: Write a test which loads class in remote JVM to test network class
 * loader code.
 * 
 * @author bbansal
 * 
 */
public class AdminServiceFilterTest extends TestCase {

    private static String storeName = "test-replication-memory";
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";

    VoldemortConfig config;
    VoldemortServer server;
    Cluster cluster;

    @Override
    public void setUp() throws IOException {
        // start 2 node cluster with free ports
        int[] ports = ServerTestUtils.findFreePorts(2);
        Node node0 = new Node(0, "localhost", ports[0], ports[1], Arrays.asList(new Integer[] { 0,
                1 }));

        ports = ServerTestUtils.findFreePorts(2);
        Node node1 = new Node(1, "localhost", ports[0], ports[1], Arrays.asList(new Integer[] { 2,
                3 }));

        cluster = new Cluster("admin-service-test", Arrays.asList(new Node[] { node0, node1 }));
        config = ServerTestUtils.createServerConfig(0,
                                                    TestUtils.createTempDir().getAbsolutePath(),
                                                    null,
                                                    storesXmlfile);
        server = new VoldemortServer(config, cluster);
        server.start();
    }

    @Override
    public void tearDown() throws IOException, InterruptedException {
        server.stop();
    }

    private Set<Pair<ByteArray, Versioned<byte[]>>> createEntries() {
        Set<Pair<ByteArray, Versioned<byte[]>>> entrySet = new HashSet<Pair<ByteArray, Versioned<byte[]>>>();

        for(int i = 0; i <= 1000; i++) {
            ByteArray key = new ByteArray(ByteUtils.getBytes("" + i, "UTF-8"));
            Versioned<byte[]> value = new Versioned<byte[]>(ByteUtils.getBytes("value-" + i,
                                                                               "UTF-8"));
            entrySet.add(new Pair<ByteArray, Versioned<byte[]>>(key, value));
        }

        return entrySet;
    }

    private AdminClient getAdminClient() {
        return ServerTestUtils.getAdminClient(server.getMetadataStore().getCluster());
    }

    public void testFetchAsStreamWithFilter() {
        // user store should be present
        Store<ByteArray, byte[]> store = server.getStoreRepository().getStorageEngine(storeName);
        assertNotSame("Store '" + storeName + "' should not be null", null, store);

        VoldemortFilter filter = new VoldemortFilterImpl();
        int shouldFilterCount = 0;
        for(Pair<ByteArray, Versioned<byte[]>> pair: createEntries()) {
            store.put(pair.getFirst(), pair.getSecond());
            if(!filter.accept(pair.getFirst(), pair.getSecond())) {
                shouldFilterCount++;
            }
        }

        assertNotSame("should be filtered key count shoud not be 0.", 0, shouldFilterCount);

        // make fetch stream call with filter
        Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator = getAdminClient().fetchPartitionEntries(0,
                                                                                                            storeName,
                                                                                                            Arrays.asList(new Integer[] { 0 }),
                                                                                                            filter);

        // assert none of the filtered entries are returned.
        while(entryIterator.hasNext()) {
            Pair<ByteArray, Versioned<byte[]>> entry = entryIterator.next();
            if(!filter.accept(entry.getFirst(), entry.getSecond())) {
                fail();
            }
        }
    }

    public void testDeleteStreamWithFilter() {
        // user store should be present
        Store<ByteArray, byte[]> store = server.getStoreRepository().getStorageEngine(storeName);
        assertNotSame("Store '" + storeName + "' should not be null", null, store);

        Set<Pair<ByteArray, Versioned<byte[]>>> entrySet = createEntries();

        VoldemortFilter filter = new VoldemortFilterImpl();
        for(Pair<ByteArray, Versioned<byte[]>> pair: entrySet) {
            store.put(pair.getFirst(), pair.getSecond());
        }

        // make delete stream call with filter
        getAdminClient().deletePartitions(0,
                                          storeName,
                                          Arrays.asList(new Integer[] { 0, 1, 2, 3 }),
                                          filter);

        // assert none of the filtered entries are returned.
        for(Pair<ByteArray, Versioned<byte[]>> entry: entrySet) {
            if(filter.accept(entry.getFirst(), entry.getSecond())) {
                assertEquals("All entries should be deleted except the filtered ones.",
                             0,
                             store.get(entry.getFirst()).size());
            } else {
                assertNotSame("filtered entry should be still present.",
                              0,
                              store.get(entry.getFirst()).size());
                assertEquals("values should match",
                             entry.getSecond().getValue(),
                             store.get(entry.getFirst()).get(0).getValue());
            }
        }
    }

    public void testUpdateAsStreamWithFilter() {
        VoldemortFilter filter = new VoldemortFilterImpl();
        Set<Pair<ByteArray, Versioned<byte[]>>> entrySet = createEntries();

        // make update stream call with filter
        getAdminClient().updateEntries(0, storeName, entrySet.iterator(), filter);

        // assert none of the filtered entries are updated.
        // user store should be present
        Store<ByteArray, byte[]> store = server.getStoreRepository().getStorageEngine(storeName);
        assertNotSame("Store '" + storeName + "' should not be null", null, store);

        for(Pair<ByteArray, Versioned<byte[]>> entry: entrySet) {
            if(filter.accept(entry.getFirst(), entry.getSecond())) {
                assertEquals("Store should have this key/value pair",
                             1,
                             store.get(entry.getFirst()).size());
                assertEquals("Store should have this key/value pair",
                             entry.getSecond(),
                             store.get(entry.getFirst()).get(0));
            } else {
                assertEquals("Store should Not have this key/value pair",
                             0,
                             store.get(entry.getFirst()).size());
            }
        }
    }

    public static class VoldemortFilterImpl implements VoldemortFilter {

        public VoldemortFilterImpl() {
            System.out.println("instantiating voldemortFilter");
        }

        public boolean accept(Object key, Versioned<?> value) {
            String keyString = ByteUtils.getString(((ByteArray) key).get(), "UTF-8");
            if(Integer.parseInt(keyString) % 10 == 3) {
                return false;
            }
            return true;
        }
    }
}
