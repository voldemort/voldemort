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

package voldemort.cluster;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.VoldemortException;
import voldemort.cluster.failuredetector.AsyncRecoveryFailureDetector;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.BasicFailureDetectorConfig;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.cluster.failuredetector.FailureDetectorConfig;
import voldemort.cluster.failuredetector.FailureDetectorUtils;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.utils.ByteArray;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.ImmutableList;

@RunWith(Parameterized.class)
public class TestCluster extends TestCase {

    private static final int BANNAGE_TIME = 1000;
    private String clusterName = "test";
    private List<Node> nodes;
    private Cluster cluster;
    private final Class<FailureDetector> failureDetectorClass;
    private FailureDetector failureDetector;
    private Time time;

    public TestCluster(Class<FailureDetector> failureDetectorClass) {
        this.failureDetectorClass = failureDetectorClass;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        this.nodes = ImmutableList.of(new Node(1, "test1", 1, 1, ImmutableList.of(1, 2, 3)),
                                      new Node(2, "test1", 2, 2, ImmutableList.of(3, 5, 6)),
                                      new Node(3, "test1", 3, 3, ImmutableList.of(7, 8, 9)),
                                      new Node(4, "test1", 4, 4, ImmutableList.of(10, 11, 12)));
        this.cluster = new Cluster(clusterName, nodes);
        this.time = SystemTime.INSTANCE;

        Map<Integer, Store<ByteArray, byte[]>> stores = new HashMap<Integer, Store<ByteArray, byte[]>>();

        for(Node node: nodes) {
            stores.put(node.getId(), new Store<ByteArray, byte[]>() {

                public void close() throws VoldemortException {}

                public boolean delete(ByteArray key, Version version) throws VoldemortException {
                    return false;
                }

                public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
                    return null;
                }

                public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
                        throws VoldemortException {
                    return null;
                }

                public Object getCapability(StoreCapabilityType capability) {
                    return null;
                }

                public String getName() {
                    return null;
                }

                public void put(ByteArray key, Versioned<byte[]> value) throws VoldemortException {}

            });
        }

        FailureDetectorConfig config = new BasicFailureDetectorConfig(failureDetectorClass.getName(),
                                                                      BANNAGE_TIME,
                                                                      stores,
                                                                      time);
        failureDetector = FailureDetectorUtils.create(config);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if(failureDetector != null)
            failureDetector.destroy();
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { AsyncRecoveryFailureDetector.class },
                { BannagePeriodFailureDetector.class } });
    }

    @Test
    public void testBasics() {
        assertEquals(nodes.size(), cluster.getNumberOfNodes());
        assertEquals(new HashSet<Node>(nodes), new HashSet<Node>(cluster.getNodes()));
        assertEquals(clusterName, cluster.getName());
        assertEquals(nodes.get(0), cluster.getNodeById(1));
    }

    @Test
    public void testStatusBeginsAsAvailable() {
        for(Node n: cluster.getNodes())
            assertTrue("Node " + n.getId() + " is not available.", failureDetector.isAvailable(n));
    }

    @Test
    public void testUnavailability() throws Exception {
        Node n = cluster.getNodeById(1);

        // begins available
        assertTrue(failureDetector.isAvailable(n));

        // if set unavailable, is unavailable
        failureDetector.recordException(n, null);

        assertFalse(failureDetector.isAvailable(n));

        // if we set it back to available then it is available again
        failureDetector.recordSuccess(n);

        // Some implementations require a sleep time, so let's wait a little
        // before checking.
        time.sleep(BANNAGE_TIME * 2);

        assertTrue(failureDetector.isAvailable(n));
    }

}
