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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import voldemort.TestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.QueryKeyResult;
import voldemort.store.routed.NodeValue;
import voldemort.utils.ConsistencyFix.BadKey;
import voldemort.versioning.Versioned;

public class ConsistencyFixWorkerTest {

    public void testRepair(int putNodes[], boolean orphan) {
        byte[] bKey = TestUtils.randomBytes(10);
        String hexKey = ByteUtils.toHexString(bKey);
        ByteArray baKey = new ByteArray(bKey);

        BadKey badKey;
        QueryKeyResult queryKeyResult;
        if(!orphan) {
            badKey = new BadKey(hexKey, hexKey + "\n");
            queryKeyResult = null;
        } else {
            StringBuilder orphanInput = new StringBuilder();
            orphanInput.append(hexKey + "," + "1\n");
            List<Versioned<byte[]>> values = new ArrayList<Versioned<byte[]>>(0);
            int arbitraryNodeId = 2;
            Versioned<byte[]> versioned = TestUtils.getVersioned(TestUtils.randomBytes(25),
                                                                 arbitraryNodeId);
            orphanInput.append(ByteUtils.toHexString(versioned.getValue()));
            orphanInput.append("," + versioned.toString() + "\n");
            values.add(versioned);

            badKey = new BadKey(hexKey, orphanInput.toString());
            queryKeyResult = new QueryKeyResult(baKey, values);
        }

        Versioned<byte[]> value = TestUtils.getVersioned(TestUtils.randomBytes(25), 0);

        String url = ConsistencyFixTest.setUpCluster();
        ConsistencyFix consistencyFix = new ConsistencyFix(url,
                                                           ConsistencyFixTest.STORE_NAME,
                                                           100,
                                                           100,
                                                           false,
                                                           false);

        AdminClient adminClient = consistencyFix.getAdminClient();

        System.out.println("Initial get");
        for(int i = 0; i < 4; ++i) {
            List<Versioned<byte[]>> results;
            results = adminClient.storeOps.getNodeKey(ConsistencyFixTest.STORE_NAME, i, baKey);
            assertTrue(results.size() == 0);
        }

        System.out.println("Puts");
        for(int putNode: putNodes) {
            NodeValue<ByteArray, byte[]> nodeKeyValue;
            nodeKeyValue = new NodeValue<ByteArray, byte[]>(putNode, baKey, value);
            adminClient.storeOps.putNodeKeyValue(ConsistencyFixTest.STORE_NAME, nodeKeyValue);
        }

        // Construct normal consistency fix worker
        ConsistencyFixWorker consistencyFixWorker = null;
        if(!orphan) {
            consistencyFixWorker = new ConsistencyFixWorker(badKey, consistencyFix, null);
        } else {
            consistencyFixWorker = new ConsistencyFixWorker(badKey,
                                                            consistencyFix,
                                                            null,
                                                            queryKeyResult);
        }
        consistencyFixWorker.run();

        System.out.println("Second get");
        int expectedNumVersions = 0;
        if(putNodes.length > 0) {
            expectedNumVersions++;
        }
        if(orphan) {
            expectedNumVersions++;
        }
        for(int i = 0; i < 4; ++i) {
            System.out.println("Node : " + i);
            List<Versioned<byte[]>> results;
            results = adminClient.storeOps.getNodeKey(ConsistencyFixTest.STORE_NAME, i, baKey);
            for(Versioned<byte[]> v: results) {
                System.out.println("\t" + v.getVersion());
            }

            assertTrue(results.size() == expectedNumVersions);
        }
    }

    @Test
    public void repairPutOne() {
        int putNodes[] = { 1 };
        testRepair(putNodes, false);
    }

    @Test
    public void repairPutTwo() {
        int putNodes[] = { 0, 3 };
        testRepair(putNodes, false);
    }

    @Test
    public void repairPutThree() {
        int putNodes[] = { 1, 2, 3 };
        testRepair(putNodes, false);
    }

    @Test
    public void repairPutFour() {
        int putNodes[] = { 0, 1, 2, 3 };
        testRepair(putNodes, false);
    }

    @Test
    public void repairPutZero() {
        int putNodes[] = {};
        testRepair(putNodes, false);
    }

    @Test
    public void orphanPutOne() {
        int putNodes[] = { 1 };
        testRepair(putNodes, true);
    }

    @Test
    public void orphanPutTwo() {
        int putNodes[] = { 0, 3 };
        testRepair(putNodes, true);
    }

    @Test
    public void orphanPutThree() {
        int putNodes[] = { 1, 2, 3 };
        testRepair(putNodes, true);
    }

    @Test
    public void orphanPutFour() {
        int putNodes[] = { 0, 1, 2, 3 };
        testRepair(putNodes, true);
    }

    @Test
    public void orphanPutZero() {
        int putNodes[] = {};
        testRepair(putNodes, true);
    }
}
