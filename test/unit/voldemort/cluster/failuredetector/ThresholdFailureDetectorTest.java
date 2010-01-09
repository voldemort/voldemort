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

import static voldemort.FailureDetectorTestUtils.recordException;
import static voldemort.FailureDetectorTestUtils.recordSuccess;
import static voldemort.MutableStoreVerifier.create;
import static voldemort.cluster.failuredetector.FailureDetectorUtils.create;

import org.junit.Test;

import voldemort.cluster.Node;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;

import com.google.common.collect.Iterables;

public class ThresholdFailureDetectorTest extends AbstractFailureDetectorTest {

    @Override
    public FailureDetector createFailureDetector() throws Exception {
        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig().setImplementationClassName(ThresholdFailureDetector.class.getName())
                                                                                 .setBannagePeriod(BANNAGE_MILLIS)
                                                                                 .setNodes(cluster.getNodes())
                                                                                 .setStoreVerifier(create(cluster.getNodes()))
                                                                                 .setTime(time)
                                                                                 .setJmxEnabled(true);
        return create(failureDetectorConfig);
    }

    @Override
    protected Time createTime() throws Exception {
        return SystemTime.INSTANCE;
    }

    @Test
    public void testCliff() throws Exception {
        int minimum = failureDetector.getConfig().getThresholdCountMinimum();

        Node node = Iterables.get(cluster.getNodes(), 8);

        for(int i = 0; i < minimum - 1; i++)
            recordException(failureDetector, node);

        assertAvailable(node);

        recordException(failureDetector, node);

        assertUnavailable(node);
        assertJmxEquals("availableNodes", "0,1,2,3,4,5,6,7");
        assertJmxEquals("unavailableNodes", "8");
        assertJmxEquals("availableNodeCount", 8);
        assertJmxEquals("nodeCount", 9);

        recordSuccess(failureDetector, node);
    }

    @Test
    public void testStartOffDownComeBackOnline() throws Exception {
        failureDetector.getConfig().setThreshold(80);
        failureDetector.getConfig().setThresholdCountMinimum(10);

        int failureCount = 20;

        Node node = Iterables.get(cluster.getNodes(), 8);

        // Force the first 20 as failed to achieve an offline node...
        for(int i = 0; i < failureCount; i++)
            recordException(failureDetector, node);

        assertUnavailable(node);

        // We go offline, but are then able to make contact with the server
        // which we mimic by recording a success.
        recordSuccess(failureDetector, node, false);

        failureDetector.waitForAvailability(node);

        assertAvailable(node);
    }

    @Test
    public void testBorder() throws Exception {
        Node node = Iterables.get(cluster.getNodes(), 8);

        for(int i = 0; i < failureDetector.getConfig().getThresholdCountMinimum(); i++)
            recordException(failureDetector, node);

        // Move to right before the new interval...
        time.sleep(failureDetector.getConfig().getThresholdInterval() - 1);
        assertUnavailable(node);
        recordSuccess(failureDetector, node);
    }

}
