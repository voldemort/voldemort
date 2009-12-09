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

import static voldemort.MutableStoreResolver.createMutableStoreResolver;
import static voldemort.VoldemortTestConstants.getNineNodeCluster;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.utils.Time;

import com.google.common.collect.Iterables;

public class FlappingTest extends FailureDetectorPerformanceTest {

    private final long[][] milliPairGroups;

    private FlappingTest(long[][] milliPairGroups) {
        this.milliPairGroups = milliPairGroups;
    }

    public static void main(String[] args) throws Throwable {
        Cluster cluster = getNineNodeCluster();

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig().setNodes(cluster.getNodes())
                                                                                 .setStoreResolver(createMutableStoreResolver(cluster.getNodes()))
                                                                                 .setAsyncScanInterval(1000)
                                                                                 .setNodeBannagePeriod(1000)
                                                                                 .setThresholdInterval(1000);

        Class<?>[] classes = new Class[] { AsyncRecoveryFailureDetector.class,
                BannagePeriodFailureDetector.class, ThresholdFailureDetector.class };

        // classes = new Class[] { ThresholdFailureDetector.class };

        long[][] milliPairGroups = new long[][] { { 66, 710 }, { 11, 981 }, { 45, 734 },
                { 33, 309 }, { 19, 511 }, { 4, 445 }, { 5, 645 }, { 964, 1220 }, { 143, 346 },
                { 55, 260 } };

        System.out.println("FailureDetector Type, Milliseconds, Outages, Successes, Failures");

        for(Class<?> implClass: classes) {
            failureDetectorConfig.setImplementationClassName(implClass.getName());
            FlappingTest flappingTest = new FlappingTest(milliPairGroups);
            String result = null;

            try {
                result = flappingTest.run(failureDetectorConfig);
            } catch(Exception e) {
                result = "ERROR: " + e.getMessage();
            }

            System.out.println(result);
        }
    }

    @Override
    public String test(FailureDetector failureDetector, Time time) throws Exception {
        FailureDetectorConfig failureDetectorConfig = failureDetector.getConfig();
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
                                                       5));

        threadPool.submit(new NodeAvailability(failureDetector, node, countDownLatch));

        threadPool.shutdown();

        if(!threadPool.awaitTermination(60, TimeUnit.SECONDS))
            System.out.println("Threads appear to be stuck");

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
            FailureDetectorConfig failureDetectorConfig = failureDetector.getConfig();

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

    private static class Listener implements FailureDetectorListener {

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
