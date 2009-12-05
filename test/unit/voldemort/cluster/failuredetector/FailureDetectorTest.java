/*
 * Copyright 2009 LinkedIn, Inc
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

package voldemort.cluster.failuredetector;

import static voldemort.MutableFailureDetectorConfig.createFailureDetector;
import static voldemort.MutableFailureDetectorConfig.recordException;
import static voldemort.MutableFailureDetectorConfig.recordSuccess;
import static voldemort.VoldemortTestConstants.getNineNodeCluster;

import java.util.Arrays;
import java.util.Collection;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.cluster.Cluster;

import com.google.common.collect.Iterables;

@RunWith(Parameterized.class)
public class FailureDetectorTest extends TestCase {

    private Cluster cluster;
    private final Class<FailureDetector> failureDetectorClass;
    private FailureDetector failureDetector;

    public FailureDetectorTest(Class<FailureDetector> failureDetectorClass) {
        this.failureDetectorClass = failureDetectorClass;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { AsyncRecoveryFailureDetector.class },
                { BannagePeriodFailureDetector.class } });
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        cluster = getNineNodeCluster();
        setFailureDetector();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if(failureDetector != null)
            failureDetector.destroy();
    }

    @Test
    public void testBasic() throws Exception {
        recordException(failureDetector, Iterables.get(cluster.getNodes(), 8));
        assertEquals(8, failureDetector.getAvailableNodeCount());
        assertEquals(9, failureDetector.getNodeCount());

        recordSuccess(failureDetector, Iterables.get(cluster.getNodes(), 8));
        assertEquals(9, failureDetector.getAvailableNodeCount());
        assertEquals(9, failureDetector.getNodeCount());
    }

    private void setFailureDetector() throws Exception {
        // Destroy any previous failure detector before creating the next one
        // (the final one is destroyed in tearDown).
        if(failureDetector != null)
            failureDetector.destroy();

        failureDetector = createFailureDetector(failureDetectorClass, cluster.getNodes());
    }
}
