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
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.cluster.Node;

import com.google.common.collect.Iterables;

public class FlappingTest extends FailureDetectorPerformanceTest {

    private final long[][] milliPairGroups;

    private FlappingTest(String[] args, long[][] milliPairGroups) {
        super(args);
        this.milliPairGroups = milliPairGroups;
    }

    public static void main(String[] args) throws Throwable {
        long[][] milliPairGroups = new long[][] { { 66, 710 }, { 11, 981 }, { 45, 734 },
                { 33, 309 }, { 19, 511 }, { 4, 445 }, { 5, 645 }, { 964, 1220 }, { 143, 346 },
                { 55, 260 } };
        FailureDetectorPerformanceTest test = new FlappingTest(args, milliPairGroups);
        test.test();
    }

    @Override
    protected String getTestHeaders() {
        return "FailureDetector Type, Milliseconds, Outages, Successes, Failures";
    }

    @Override
    public String test(FailureDetector failureDetector) throws Exception {
        Node node = Iterables.get(failureDetectorConfig.getNodes(), 0);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Listener listener = new Listener();
        failureDetector.addFailureDetectorListener(listener);

        long start = System.currentTimeMillis();

        AtomicInteger successCounter = new AtomicInteger();
        AtomicInteger failureCounter = new AtomicInteger();

        ExecutorService threadPool = Executors.newFixedThreadPool(11);

        for(int i = 0; i < 10; i++)
            threadPool.submit(new NodeAccessorRunnable(failureDetector,
                                                       node,
                                                       countDownLatch,
                                                       successCounter,
                                                       failureCounter,
                                                       null,
                                                       0,
                                                       10));

        threadPool.submit(new NodeAvailability(failureDetector, node, countDownLatch));

        threadPool.shutdown();

        // If we get stuck, we should give the user the opportunity to get a
        // thread dump.
        if(!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
            System.out.println("Threads appear to be stuck");
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        }

        long end = System.currentTimeMillis();

        if(listener.getAvailableCount() != listener.getUnavailableCount())
            throw new Exception("Node " + node + " should be back up");

        return Class.forName(failureDetectorConfig.getImplementationClassName()).getSimpleName()
               + ", " + (end - start) + ", " + listener.getUnavailableCount() + ", "
               + successCounter + ", " + failureCounter;
    }

    private class NodeAvailability implements Runnable {

        private final FailureDetector failureDetector;

        private final Node node;

        private final CountDownLatch countDownLatch;

        public NodeAvailability(FailureDetector failureDetector,
                                Node node,
                                CountDownLatch countDownLatch) {
            this.failureDetector = failureDetector;
            this.node = node;
            this.countDownLatch = countDownLatch;
        }

        public void run() {
            try {
                for(long[] milliPairs: milliPairGroups) {
                    updateNodeStoreAvailability(failureDetectorConfig, node, false);
                    failureDetectorConfig.getTime().sleep(milliPairs[0]);

                    updateNodeStoreAvailability(failureDetectorConfig, node, true);
                    failureDetectorConfig.getTime().sleep(milliPairs[1]);
                }

                failureDetector.waitForAvailability(node);

                countDownLatch.countDown();
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

    }

    private class Listener implements FailureDetectorListener {

        private AtomicInteger availableCount = new AtomicInteger();

        private AtomicInteger unavailableCount = new AtomicInteger();

        public void nodeAvailable(Node node) {
            availableCount.incrementAndGet();
        }

        public void nodeUnavailable(Node node) {
            unavailableCount.incrementAndGet();
        }

        public int getAvailableCount() {
            return availableCount.get();
        }

        public int getUnavailableCount() {
            return unavailableCount.get();
        }

    }

}
