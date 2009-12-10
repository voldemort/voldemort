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
import static voldemort.MutableStoreResolver.createMutableStoreResolver;
import static voldemort.cluster.failuredetector.FailureDetectorUtils.create;

import org.junit.Test;

import voldemort.cluster.Node;

import com.google.common.collect.Iterables;

public class ThresholdFailureDetectorTest extends AbstractFailureDetectorTest {

    @Override
    public FailureDetector setUpFailureDetector() throws Exception {
        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig().setImplementationClassName(ThresholdFailureDetector.class.getName())
                                                                                 .setBannagePeriod(BANNAGE_MILLIS)
                                                                                 .setNodes(cluster.getNodes())
                                                                                 .setStoreResolver(createMutableStoreResolver(cluster.getNodes()))
                                                                                 .setTime(time);
        return create(failureDetectorConfig);
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
    }

    @Test
    public void testStartOffDownComeBackOnline() throws Exception {
        failureDetector.getConfig().setThreshold(80);
        failureDetector.getConfig().setThresholdCountMinimum(10);

        int failureCount = 20;
        int successCount = 80;

        Node node = Iterables.get(cluster.getNodes(), 8);

        // Force the first 20 as failed to achieve an offline node...
        for(int i = 0; i < failureCount; i++)
            recordException(failureDetector, node);

        assertUnavailable(node);

        // Then mark the 79 (out of 80) to be success...
        for(int i = 0; i < successCount - 1; i++)
            recordSuccess(failureDetector, node, false);

        // ...which still isn't enough to bring us back up...
        assertUnavailable(node);

        // ...but force another success which will bring the node back up...
        recordSuccess(failureDetector, node, false);
        assertAvailable(node);

        // ...and another failure will bring the node back down...
        recordException(failureDetector, node);
        assertUnavailable(node);
    }

    @Test
    public void testBorder() throws Exception {
        Node node = Iterables.get(cluster.getNodes(), 8);

        for(int i = 0; i < failureDetector.getConfig().getThresholdCountMinimum(); i++)
            recordException(failureDetector, node);

        // Move to right before the new interval...
        time.sleep(failureDetector.getConfig().getThresholdInterval() - 1);
        assertUnavailable(node);

        // Move to the new interval...
        time.sleep(1);

        assertAvailable(node);
    }

}
