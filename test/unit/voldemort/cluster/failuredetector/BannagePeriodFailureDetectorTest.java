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

import static org.junit.Assert.assertEquals;
import static voldemort.MutableStoreResolver.createFailureDetector;
import static voldemort.MutableStoreResolver.recordException;
import static voldemort.MutableStoreResolver.recordSuccess;

import org.junit.Test;

import voldemort.cluster.Node;

import com.google.common.collect.Iterables;

public class BannagePeriodFailureDetectorTest extends AbstractFailureDetectorTest {

    private static final int BANNAGE_MILLIS = 10000;

    @Override
    public FailureDetector setUpFailureDetector() throws Exception {
        return createFailureDetector(BannagePeriodFailureDetector.class,
                                     cluster.getNodes(),
                                     time,
                                     BANNAGE_MILLIS);
    }

    @Test
    public void testTimeout() throws Exception {
        Node node = Iterables.get(cluster.getNodes(), 8);

        recordException(failureDetector, node);

        assertEquals(8, failureDetector.getAvailableNodeCount());
        assertEquals(9, failureDetector.getNodeCount());
        assertEquals(false, failureDetector.isAvailable(node));

        time.sleep(BANNAGE_MILLIS / 2);

        assertEquals(8, failureDetector.getAvailableNodeCount());
        assertEquals(9, failureDetector.getNodeCount());
        assertEquals(false, failureDetector.isAvailable(node));

        time.sleep((BANNAGE_MILLIS / 2) + 1);

        assertEquals(9, failureDetector.getAvailableNodeCount());
        assertEquals(9, failureDetector.getNodeCount());
        assertEquals(true, failureDetector.isAvailable(node));
    }

    @Test
    public void testCumulativeFailures() throws Exception {
        Node node = Iterables.get(cluster.getNodes(), 8);

        recordException(failureDetector, node);

        assertEquals(8, failureDetector.getAvailableNodeCount());
        assertEquals(9, failureDetector.getNodeCount());
        assertEquals(false, failureDetector.isAvailable(node));

        time.sleep(BANNAGE_MILLIS / 2);

        // OK, now record another exception
        recordException(failureDetector, node);

        assertEquals(8, failureDetector.getAvailableNodeCount());
        assertEquals(9, failureDetector.getNodeCount());
        assertEquals(false, failureDetector.isAvailable(node));

        // If it's not cumulative, it would pass after sleeping the rest of the
        // initial period...
        time.sleep((BANNAGE_MILLIS / 2) + 1);

        // ...but it's still unavailable...
        assertEquals(8, failureDetector.getAvailableNodeCount());
        assertEquals(9, failureDetector.getNodeCount());
        assertEquals(false, failureDetector.isAvailable(node));

        // ...so sleep for the whole bannage period
        time.sleep(BANNAGE_MILLIS);

        assertEquals(9, failureDetector.getAvailableNodeCount());
        assertEquals(9, failureDetector.getNodeCount());
        assertEquals(true, failureDetector.isAvailable(node));
    }

    @Test
    public void testForceSuccess() throws Exception {
        Node node = Iterables.get(cluster.getNodes(), 8);

        recordException(failureDetector, node);

        assertEquals(8, failureDetector.getAvailableNodeCount());
        assertEquals(9, failureDetector.getNodeCount());
        assertEquals(false, failureDetector.isAvailable(node));

        recordSuccess(failureDetector, node, false);

        assertEquals(9, failureDetector.getAvailableNodeCount());
        assertEquals(9, failureDetector.getNodeCount());
        assertEquals(true, failureDetector.isAvailable(node));
    }

}
