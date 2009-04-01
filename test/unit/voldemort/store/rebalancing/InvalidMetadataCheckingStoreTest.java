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

package voldemort.store.rebalancing;

import java.util.List;

import junit.framework.TestCase;
import voldemort.TestUtils;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.store.DoNothingStore;
import voldemort.store.InvalidMetadataException;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.Versioned;

/**
 * @author bbansal
 * 
 */
public class InvalidMetadataCheckingStoreTest extends TestCase {

    private static int LOOP_COUNT = 1000;

    public void testValidMetaData() {
        RoutingStrategy routingStrategy = TestUtils.getRoutingStrategy(new int[][] {
                { 0, 1, 2, 3 }, { 4, 5, 6, 7 }, { 8, 9, 10, 11 } }, 1);
        InvalidMetadataCheckingStore store = new InvalidMetadataCheckingStore(0,
                                                                              new DoNothingStore<ByteArray, byte[]>("test"),
                                                                              routingStrategy);

        try {
            doOperations(0, store, store.getRoutingStrategy());
        } catch(InvalidMetadataException e) {
            throw new RuntimeException("Should not see any InvalidMetaDataException", e);
        }
    }

    /**
     * NOTE: the total number of partitions should remain same for hash
     * consistency
     */
    public void testAddingPartition() {
        RoutingStrategy routingStrategy = TestUtils.getRoutingStrategy(new int[][] {
                { 0, 1, 2, 3 }, { 4, 5, 6, 7 }, { 8, 9, 10, 11 } }, 1);

        RoutingStrategy updatedRoutingStrategy = TestUtils.getRoutingStrategy(new int[][] {
                { 0, 1, 2, 3, 11 }, { 4, 5, 6, 7 }, { 8, 9, 10 } }, 1);

        InvalidMetadataCheckingStore store = new InvalidMetadataCheckingStore(0,
                                                                              new DoNothingStore<ByteArray, byte[]>("test"),
                                                                              updatedRoutingStrategy);
        try {
            doOperations(0, store, routingStrategy);
        } catch(InvalidMetadataException e) {
            throw new RuntimeException("Should not see any InvalidMetaDataException", e);
        }
    }

    public void testRemovingPartition() {
        boolean sawException = false;
        RoutingStrategy routingStrategy = TestUtils.getRoutingStrategy(new int[][] {
                { 0, 1, 2, 3 }, { 4, 5, 6, 7 }, { 8, 9, 10, 11 } }, 1);

        RoutingStrategy updatedRoutingStrategy = TestUtils.getRoutingStrategy(new int[][] {
                { 0, 1, 2 }, { 4, 5, 6, 7, 3 }, { 8, 9, 10, 11 } }, 1);

        InvalidMetadataCheckingStore store = new InvalidMetadataCheckingStore(0,
                                                                              new DoNothingStore<ByteArray, byte[]>("test"),
                                                                              updatedRoutingStrategy);
        try {
            doOperations(0, store, routingStrategy);
        } catch(InvalidMetadataException e) {
            sawException = true;
        }

        assertEquals("Should see InvalidMetaDataException", true, sawException);
    }

    private boolean containsNodeId(List<Node> nodes, int nodeId) {
        for(Node node: nodes) {
            if(nodeId == node.getId()) {
                return true;
            }
        }
        return false;
    }

    private void doOperations(int nodeId,
                              Store<ByteArray, byte[]> store,
                              RoutingStrategy routingStrategy) {
        for(int i = 0; i < LOOP_COUNT;) {
            ByteArray key = new ByteArray(ByteUtils.md5(Integer.toString((int) (Math.random() * Integer.MAX_VALUE))
                                                               .getBytes()));
            byte[] value = "value".getBytes();

            if(containsNodeId(routingStrategy.routeRequest(key.get()), nodeId)) {
                i++; // increment count
                switch(i % 3) {
                    case 0:
                        store.get(key);
                        break;
                    case 1:
                        store.delete(key, null);
                        break;
                    case 2:
                        store.put(key, new Versioned<byte[]>(value));
                        break;

                }
            }
        }

    }
}
