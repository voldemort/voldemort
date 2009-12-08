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

import java.net.SocketTimeoutException;

import voldemort.cluster.Node;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.Time;

import com.google.common.collect.Iterables;

public class TimedUnavailabilityTest extends FailureDetectorPerformanceTest {

    private final long unavailabilityMillis;

    public TimedUnavailabilityTest(long unavailabilityMillis) {
        this.unavailabilityMillis = unavailabilityMillis;
    }

    @Override
    public void test(final FailureDetector failureDetector,
                     final PerformanceTestFailureDetectorListener listener,
                     final Time time) throws Exception {
        final FailureDetectorConfig failureDetectorConfig = failureDetector.getConfig();
        final Node node = Iterables.get(failureDetectorConfig.getNodes(), 0);

        Thread t = new Thread(new Runnable() {

            public void run() {
                try {
                    updateNodeStoreAvailability(failureDetectorConfig, node, false);
                    failureDetector.recordException(node,
                                                    new UnreachableStoreException("This is part of the test",
                                                                                  new SocketTimeoutException("This is part of the test")));

                    time.sleep(unavailabilityMillis);
                    updateNodeStoreAvailability(failureDetectorConfig, node, true);

                    while(!failureDetector.isAvailable(node))
                        time.sleep(10);

                    failureDetector.recordSuccess(node);
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }

        });

        t.start();
        t.join();
    }

}
