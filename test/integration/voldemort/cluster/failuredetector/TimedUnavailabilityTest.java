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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import voldemort.cluster.Node;
import voldemort.utils.Time;

import com.google.common.collect.Iterables;

public class TimedUnavailabilityTest extends FailureDetectorPerformanceTest {

    private final long unavailabilityMillis;

    private TimedUnavailabilityTest(String[] args, long unavailabilityMillis) {
        super(args);
        this.unavailabilityMillis = unavailabilityMillis;
    }

    public static void main(String[] args) throws Throwable {
        FailureDetectorPerformanceTest test = new TimedUnavailabilityTest(args, 2522);
        test.test();
    }

    @Override
    public String test(FailureDetector failureDetector) throws Exception {
        Node node = Iterables.get(failureDetectorConfig.getNodes(), 0);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Listener listener = new Listener(failureDetectorConfig.getTime());
        failureDetector.addFailureDetectorListener(listener);

        ExecutorService threadPool = Executors.newFixedThreadPool(11);

        for(int i = 0; i < 10; i++)
            threadPool.submit(new NodeAccessorRunnable(failureDetector,
                                                       node,
                                                       countDownLatch,
                                                       null,
                                                       null,
                                                       null,
                                                       0,
                                                       10));

        threadPool.submit(new TimedUnavailability(failureDetector, node, countDownLatch));

        threadPool.shutdown();

        // If we get stuck, we should give the user the opportunity to get a
        // thread dump.
        if(!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
            System.out.println("Threads appear to be stuck");
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        }

        return Class.forName(failureDetectorConfig.getImplementationClassName()).getSimpleName()
               + ", " + listener.getDelta();
    }

    private class TimedUnavailability implements Runnable {

        private final FailureDetector failureDetector;

        private final Node node;

        private final CountDownLatch countDownLatch;

        public TimedUnavailability(FailureDetector failureDetector, Node node, CountDownLatch countDownLatch) {
            this.failureDetector = failureDetector;
            this.node = node;
            this.countDownLatch = countDownLatch;
        }

        public void run() {
            FailureDetectorConfig failureDetectorConfig = failureDetector.getConfig();

            try {
                updateNodeStoreAvailability(failureDetectorConfig, node, false);
                failureDetectorConfig.getTime().sleep(unavailabilityMillis);
                updateNodeStoreAvailability(failureDetectorConfig, node, true);

                failureDetector.waitForAvailability(node);

                countDownLatch.countDown();
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

    }

    private static class Listener implements FailureDetectorListener {

        private final Time time;

        private long markedAvailable;

        private long markedUnavailable;

        public Listener(Time time) {
            this.time = time;
        }

        public void nodeAvailable(Node node) {
            markedAvailable = time.getMilliseconds();
        }

        public void nodeUnavailable(Node node) {
            markedUnavailable = time.getMilliseconds();
        }

        public long getDelta() {
            return markedAvailable - markedUnavailable;
        }

    }

}
