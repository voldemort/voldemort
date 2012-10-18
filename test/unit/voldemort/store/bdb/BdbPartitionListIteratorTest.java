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

package voldemort.store.bdb;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.io.FileDeleteStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.routing.RoutingStrategy;
import voldemort.server.VoldemortConfig;
import voldemort.store.PartitionListIterator;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;

/**
 * Tests the PartitionListIterator used in pidscan based rebalancing
 * 
 */
public class BdbPartitionListIteratorTest {

    private File bdbMasterDir;
    private BdbStorageConfiguration bdbStorage;
    private BdbStorageEngine store;
    private RoutingStrategy strategy;
    private HashMap<Integer, Set<String>> partitionEntries;

    @Before
    public void setUp() throws Exception {
        bdbMasterDir = TestUtils.createTempDir();
        FileDeleteStrategy.FORCE.delete(bdbMasterDir);

        // lets use all the default values.
        Props props = new Props();
        props.put("node.id", 1);
        props.put("voldemort.home", "test/common/voldemort/config");
        VoldemortConfig voldemortConfig = new VoldemortConfig(props);
        voldemortConfig.setBdbCacheSize(10 * 1024 * 1024);
        voldemortConfig.setBdbOneEnvPerStore(true);
        voldemortConfig.setBdbDataDirectory(bdbMasterDir.toURI().getPath());
        voldemortConfig.setBdbPrefixKeysWithPartitionId(true);
        bdbStorage = new BdbStorageConfiguration(voldemortConfig);
        StoreDefinition defA = TestUtils.makeStoreDefinition("storeA");
        store = (BdbStorageEngine) bdbStorage.getStore(defA,
                                                       (strategy = TestUtils.makeSingleNodeRoutingStrategy()));

        // load some data for non odd partitions, and note down how much data we
        // have for each partition.
        partitionEntries = new HashMap<Integer, Set<String>>();
        int numEntries = 0;
        while(numEntries++ < 10000) {
            String key = "entry_" + numEntries;
            int p = strategy.getMasterPartition(key.getBytes());
            // omit odd partitions
            if(p % 2 == 1)
                continue;

            if(!partitionEntries.containsKey(p))
                partitionEntries.put(p, new HashSet<String>());

            store.put(new ByteArray(key.getBytes()), new Versioned<byte[]>(key.getBytes()), null);
            partitionEntries.get(p).add(key);
        }
    }

    @After
    public void tearDown() throws Exception {
        store.close();
        bdbStorage.close();
        FileDeleteStrategy.FORCE.delete(bdbMasterDir);
    }

    @Test
    public void testEmptyPartitionList() {

        PartitionListIterator plistItr = new PartitionListIterator(store, new ArrayList<Integer>());
        assertEquals("Empty list cannot have a next element", false, plistItr.hasNext());
        try {
            plistItr.next();
            fail("Should have thrown an exception for next()");
        } catch(NoSuchElementException ne) {

        } finally {
            plistItr.close();
        }
    }

    @Test
    public void testEmptyPartition() {

        PartitionListIterator plistItr = new PartitionListIterator(store, Arrays.asList(1));
        assertEquals("No data loaded for odd partitions, so hasNext() should be false",
                     false,
                     plistItr.hasNext());
        try {
            plistItr.next();
            fail("Should have thrown an exception for next()");
        } catch(NoSuchElementException ne) {

        } finally {
            plistItr.close();
        }
    }

    @Test
    public void testSingletonPartitionList() {
        PartitionListIterator plistItr = new PartitionListIterator(store, Arrays.asList(4));
        Set<String> pentries = new HashSet<String>();
        while(plistItr.hasNext()) {
            pentries.add(new String(plistItr.next().getFirst().get()));
        }
        plistItr.close();
        assertEquals(partitionEntries.get(4), pentries);
    }

    @Test
    public void testPartitionListWithEmptyPartitions() {
        PartitionListIterator plistItr = new PartitionListIterator(store, Arrays.asList(2,
                                                                                        3,
                                                                                        4,
                                                                                        5,
                                                                                        6));
        HashMap<Integer, Set<String>> retrievedPartitionEntries = new HashMap<Integer, Set<String>>();
        while(plistItr.hasNext()) {
            String key = new String(plistItr.next().getFirst().get());
            int p = strategy.getMasterPartition(key.getBytes());

            if(!retrievedPartitionEntries.containsKey(p))
                retrievedPartitionEntries.put(p, new HashSet<String>());
            retrievedPartitionEntries.get(p).add(key);
        }
        plistItr.close();

        // should only have retrieved entries for even partitions
        assertEquals(3, retrievedPartitionEntries.size());
        for(Integer p: Arrays.asList(2, 3, 4, 5, 6)) {
            if(p % 2 == 0) {
                assertEquals(partitionEntries.get(p), retrievedPartitionEntries.get(p));
            } else {
                assertEquals(false, retrievedPartitionEntries.containsKey(p));
            }
        }
    }
}
