/*
 * Copyright 2012 LinkedIn, Inc
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
package voldemort.utils.pool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

public class QueuedKeyedResourcePoolContentionTest extends KeyedResourcePoolContentionTest {

    protected QueuedKeyedResourcePool<String, TestResource> queuedPool;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        this.queuedPool = new QueuedKeyedResourcePool<String, TestResource>(factory, config);
        super.pool = queuedPool;

        TestResourceRequest.usedResourceCount.set(0);
        TestResourceRequest.handledTimeoutCount.set(0);
        TestResourceRequest.handledExceptionCount.set(0);
    }

    @Test
    public void contendForQueue() throws Exception {
        // Over ride some set up
        super.config = new ResourcePoolConfig().setMaxPoolSize(POOL_SIZE)
                                               .setTimeout(TIMEOUT_MS * 50, TimeUnit.MILLISECONDS);
        this.queuedPool = new QueuedKeyedResourcePool<String, TestResource>(factory, config);
        super.pool = queuedPool;

        int numEnqueuers = POOL_SIZE * 2;
        int numEnqueues = 10 * 1000;
        String key = "Key";
        float invalidationRate = (float) 0.25;
        CountDownLatch waitForThreads = new CountDownLatch(numEnqueuers);
        CountDownLatch waitForEnqueuers = new CountDownLatch(numEnqueuers);
        for(int i = 0; i < numEnqueuers; ++i) {
            new Thread(new Enqueuers(waitForThreads,
                                     waitForEnqueuers,
                                     key,
                                     numEnqueues,
                                     invalidationRate)).start();
        }

        try {
            waitForEnqueuers.await();
            assertEquals(POOL_SIZE, this.queuedPool.getTotalResourceCount());
            assertEquals(POOL_SIZE, this.queuedPool.getCheckedInResourceCount());
            assertEquals(0, this.queuedPool.getRegisteredResourceRequestCount());

            assertEquals(numEnqueuers * numEnqueues, TestResourceRequest.usedResourceCount.get());
            assertEquals(0, TestResourceRequest.handledTimeoutCount.get());
            assertEquals(0, TestResourceRequest.handledExceptionCount.get());

        } catch(InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void contendForQueueAndPool() throws Exception {
        // Over ride some set up
        super.config = new ResourcePoolConfig().setMaxPoolSize(POOL_SIZE)
                                               .setTimeout(TIMEOUT_MS * 100, TimeUnit.MILLISECONDS);
        this.queuedPool = new QueuedKeyedResourcePool<String, TestResource>(factory, config);
        super.pool = queuedPool;

        int numEnqueuers = POOL_SIZE;
        int numCheckers = POOL_SIZE;
        int numEnqueues = 10 * 1000;
        String key = "Key";
        float invalidationRate = (float) 0.25;
        CountDownLatch waitForThreadsStart = new CountDownLatch(numEnqueuers + numCheckers);
        CountDownLatch waitForThreadsEnd = new CountDownLatch(numEnqueuers + numCheckers);
        for(int i = 0; i < numEnqueuers; ++i) {
            new Thread(new Enqueuers(waitForThreadsStart,
                                     waitForThreadsEnd,
                                     key,
                                     numEnqueues,
                                     invalidationRate)).start();
        }
        for(int i = 0; i < numCheckers; ++i) {
            new Thread(new Checkers(waitForThreadsStart,
                                    waitForThreadsEnd,
                                    key,
                                    numEnqueues,
                                    invalidationRate)).start();
        }

        try {
            waitForThreadsEnd.await();
            assertEquals(this.queuedPool.getCheckedInResourceCount(),
                         this.queuedPool.getTotalResourceCount());
            assertEquals(0, this.queuedPool.getRegisteredResourceRequestCount());

            assertEquals(numEnqueuers * numEnqueues, TestResourceRequest.usedResourceCount.get());
            assertEquals(0, TestResourceRequest.handledTimeoutCount.get());
            assertEquals(0, TestResourceRequest.handledExceptionCount.get());

        } catch(InterruptedException e) {
            e.printStackTrace();
        }

    }

    public class Enqueuers implements Runnable {

        private final CountDownLatch startSignal;
        private final CountDownLatch doneSignal;

        private final String key;
        private final int enqueues;
        private int used;
        Queue<TestResource> resources;

        private Random random;
        private float invalidationRate;

        Enqueuers(CountDownLatch startSignal,
                  CountDownLatch doneSignal,
                  String key,
                  int enqueues,
                  float invalidationRate) {
            this.startSignal = startSignal;
            this.doneSignal = doneSignal;

            this.key = key;
            this.enqueues = enqueues;
            this.used = 0;
            resources = new ConcurrentLinkedQueue<TestResource>();

            this.random = new Random();
            this.invalidationRate = invalidationRate;
        }

        private void processAtMostOneEnqueuedResource() throws Exception {
            TestResource tr = resources.poll();
            if(tr != null) {
                this.used++;
                assertTrue(tr.isValid());

                // Invalidate some resources (except on last few check ins)
                float f = random.nextFloat();
                if(f < invalidationRate && this.used < this.enqueues - POOL_SIZE) {
                    tr.invalidate();
                }
                Thread.yield();

                queuedPool.checkin(key, tr);
                Thread.yield();
            }
        }

        @Override
        public void run() {
            startSignal.countDown();
            try {
                startSignal.await();
            } catch(InterruptedException e) {
                e.printStackTrace();
            }

            try {
                for(int i = 0; i < enqueues; ++i) {
                    long deadlineNs = System.nanoTime()
                                      + TimeUnit.MILLISECONDS.toNanos(config.getTimeout(TimeUnit.NANOSECONDS));

                    queuedPool.registerResourceRequest(key, new TestResourceRequest(deadlineNs,
                                                                                    resources));
                    Thread.yield();

                    processAtMostOneEnqueuedResource();
                }
                while(this.used < enqueues) {
                    processAtMostOneEnqueuedResource();
                    Thread.yield();
                }
            } catch(Exception e) {
                fail(e.toString());
            }
            doneSignal.countDown();
        }
    }
}
