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

import static voldemort.MutableStoreResolver.createFailureDetector;
import static voldemort.MutableStoreResolver.recordException;
import static voldemort.MutableStoreResolver.recordSuccess;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.cluster.failuredetector.AsyncRecoveryFailureDetector;
import voldemort.cluster.failuredetector.BannagePeriodFailureDetector;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;

import com.google.common.collect.ImmutableList;

@RunWith(Parameterized.class)
public class TestCluster extends TestCase {

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
        this.nodes = ImmutableList.of(new Node(1, "test1", 1, 1, 1, ImmutableList.of(1, 2, 3)),
                                      new Node(2, "test1", 2, 2, 2, ImmutableList.of(3, 5, 6)),
                                      new Node(3, "test1", 3, 3, 3, ImmutableList.of(7, 8, 9)),
                                      new Node(4, "test1", 4, 4, 4, ImmutableList.of(10, 11, 12)));
        this.cluster = new Cluster(clusterName, nodes);
        this.time = SystemTime.INSTANCE;

        failureDetector = createFailureDetector(failureDetectorClass,
                                                cluster.getNodes(),
                                                time,
                                                1000);
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
        recordException(failureDetector, n);

        assertFalse(failureDetector.isAvailable(n));

        // if we set it back to available then it is available again
        recordSuccess(failureDetector, n);

        assertTrue(failureDetector.isAvailable(n));
    }

}
