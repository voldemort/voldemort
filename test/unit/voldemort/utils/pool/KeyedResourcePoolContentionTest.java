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

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

public class KeyedResourcePoolContentionTest extends KeyedResourcePoolTestBase {

    protected static int POOL_SIZE = 5;
    protected static long TIMEOUT_MS = 500;

    @Before
    public void setUp() {
        factory = new TestResourceFactory();
        config = new ResourcePoolConfig().setMaxPoolSize(POOL_SIZE)
                                         .setTimeout(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        this.pool = new KeyedResourcePool<String, TestResource>(factory, config);
    }

    // This method was helpful when developing contendForResources
    public void printStats(String key) {
        System.err.println("");
        System.err.println("getCreated: " + this.factory.getCreated());
        System.err.println("getDestroyed: " + this.factory.getDestroyed());
        System.err.println("getTotalResourceCount(key): " + this.pool.getTotalResourceCount(key));
        System.err.println("getTotalResourceCount(): " + this.pool.getTotalResourceCount());
        System.err.println("getCheckedInResourcesCount(key): "
                           + this.pool.getCheckedInResourcesCount(key));
        System.err.println("getCheckedInResourceCount(): " + this.pool.getCheckedInResourceCount());
    }

    @Test
    public void contendForResources() throws Exception {
        int numCheckers = POOL_SIZE * 2;
        int numChecks = 10 * 1000;
        String key = "Key";
        float invalidationRate = (float) 0.25;
        CountDownLatch waitForThreads = new CountDownLatch(numCheckers);
        CountDownLatch waitForCheckers = new CountDownLatch(numCheckers);
        for(int i = 0; i < numCheckers; ++i) {
            new Thread(new Checkers(waitForThreads,
                                    waitForCheckers,
                                    key,
                                    numChecks,
                                    invalidationRate)).start();
        }

        try {
            waitForCheckers.await();
            assertEquals(this.pool.getCheckedInResourceCount(), this.pool.getTotalResourceCount());
        } catch(InterruptedException e) {
            e.printStackTrace();
        }

    }

    public class Checkers implements Runnable {

        private final CountDownLatch startSignal;
        private final CountDownLatch doneSignal;

        private final String key;
        private final int checks;

        private Random random;
        private float invalidationRate;

        Checkers(CountDownLatch startSignal,
                 CountDownLatch doneSignal,
                 String key,
                 int checks,
                 float invalidationRate) {
            this.startSignal = startSignal;
            this.doneSignal = doneSignal;

            this.key = key;
            this.checks = checks;

            this.random = new Random();
            this.invalidationRate = invalidationRate;
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
                TestResource tr = null;
                for(int i = 0; i < checks; ++i) {
                    tr = pool.checkout(key);
                    assertTrue(tr.isValid());

                    // Invalid some resources (except on last checkin)
                    float f = random.nextFloat();
                    if(f < invalidationRate && i != checks - 1) {
                        tr.invalidate();
                    }
                    Thread.yield();

                    pool.checkin(key, tr);
                    Thread.yield();

                    // if(i % 1000 == 0) { printStats(key); }
                }
            } catch(Exception e) {
                System.err.println(e.toString());
                fail(e.toString());
            }
            doneSignal.countDown();
        }
    }
}
