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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import voldemort.utils.Time;

public class KeyedResourcePoolTest {

    protected static int POOL_SIZE = 5;
    protected static long TIMEOUT_MS = 500;

    protected TestResourceFactory factory;
    protected KeyedResourcePool<String, TestResource> pool;
    protected ResourcePoolConfig config;

    @Before
    public void setUp() {
        factory = new TestResourceFactory();
        config = new ResourcePoolConfig().setMaxPoolSize(POOL_SIZE)
                                         .setTimeout(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        this.pool = new KeyedResourcePool<String, TestResource>(factory, config);
    }

    @Test
    public void testResourcePoolConfigTimeout() {
        // Issue 343
        assertEquals(config.getTimeout(TimeUnit.MILLISECONDS), TIMEOUT_MS);
        assertEquals(config.getTimeout(TimeUnit.NANOSECONDS), TIMEOUT_MS * Time.NS_PER_MS);
    }

    @Test
    public void testPoolingOccurs() throws Exception {
        TestResource r1 = this.pool.checkout("a");
        this.pool.checkin("a", r1);
        TestResource r2 = this.pool.checkout("a");
        assertTrue("Checked out value should equal checked in value (found " + r1 + ", " + r2 + ")",
                   r1 == r2);
    }

    @Test
    public void testFullPoolBlocks() throws Exception {
        for(int i = 0; i < POOL_SIZE; i++)
            this.pool.checkout("a");

        try {
            this.pool.checkout("a");
            fail("Checking out more items than in the pool should timeout.");
        } catch(TimeoutException e) {
            // this is good
        }
    }

    @Test
    public void testExceptionOnDestroy() throws Exception {
        assertTrue("POOL_SIZE is not big enough", POOL_SIZE >= 2);

        Exception toThrow = new Exception("An exception! (This exception is expected and so will print out some output to stderr.)");
        this.factory.setDestroyException(toThrow);

        assertEquals(0, this.pool.getTotalResourceCount());
        try {
            TestResource checkedOut = this.pool.checkout("a");
            assertFalse(checkedOut.isDestroyed());
            assertEquals(1, this.factory.getCreated());
            assertEquals(1, this.pool.getTotalResourceCount());
            assertEquals(0, this.pool.getCheckedInResourceCount());

            this.pool.checkin("a", checkedOut);
            assertEquals(1, this.factory.getCreated());
            assertEquals(1, this.pool.getTotalResourceCount());
            assertEquals(1, this.pool.getCheckedInResourceCount());

            this.pool.checkout("a");
            assertEquals(1, this.factory.getCreated());
            assertEquals(1, this.pool.getTotalResourceCount());
            assertEquals(0, this.pool.getCheckedInResourceCount());

            for(int i = 0; i < POOL_SIZE - 1; i++) {
                checkedOut = this.pool.checkout("a");
                assertFalse(checkedOut.isDestroyed());
            }
            assertEquals(POOL_SIZE, this.factory.getCreated());
            assertEquals(POOL_SIZE, this.pool.getTotalResourceCount());
            assertEquals(0, this.pool.getCheckedInResourceCount());
            assertEquals(0, this.factory.getDestroyed());

            checkedOut.invalidate();
            try {
                // pool.checkin should catch and print out the destroy
                // exception.
                this.pool.checkin("a", checkedOut);
            } catch(Exception caught) {
                fail("No exception expected.");
            }
            assertEquals(POOL_SIZE - 1, this.pool.getTotalResourceCount());
            assertEquals(0, this.pool.getCheckedInResourceCount());
            assertEquals(0, this.factory.getDestroyed());

            this.pool.checkout("a");
            assertEquals(POOL_SIZE + 1, this.factory.getCreated());
            assertEquals(POOL_SIZE, this.pool.getTotalResourceCount());
            assertEquals(0, this.pool.getCheckedInResourceCount());
            assertEquals(0, this.factory.getDestroyed());
        } catch(Exception caught) {
            fail("No exception expected.");
        }
    }

    @Test
    public void testExceptionOnCreate() throws Exception {
        Exception toThrow = new Exception("An exception!");

        assertEquals(0, this.pool.getTotalResourceCount());
        this.factory.setCreateException(toThrow);
        try {
            this.pool.checkout("b");
            fail("Expected exception!");
        } catch(Exception caught) {
            assertEquals("The exception thrown by the factory should propagate to the caller.",
                         toThrow,
                         caught);
        }
        // failed checkout shouldn't affect count
        assertEquals(0, this.pool.getTotalResourceCount());
        assertEquals(0, this.pool.getCheckedInResourceCount());
    }

    // NEGATIVE Test. See comment in line.
    @Test
    public void repeatedCheckins() throws Exception {
        assertEquals(0, this.pool.getTotalResourceCount());

        TestResource resource = this.pool.checkout("a");
        assertEquals(1, this.factory.getCreated());
        assertEquals(1, this.pool.getTotalResourceCount());
        assertEquals(0, this.pool.getCheckedInResourceCount());

        this.pool.checkin("a", resource);
        assertEquals(1, this.factory.getCreated());
        assertEquals(1, this.pool.getTotalResourceCount());
        assertEquals(1, this.pool.getCheckedInResourceCount());

        this.pool.checkin("a", resource);
        assertEquals(1, this.factory.getCreated());
        assertEquals(1, this.pool.getTotalResourceCount());
        // KeyedResourcePool does not protect against repeated checkins. It
        // Should. If it did, then the commented out test below would be
        // correct.
        assertEquals(2, this.pool.getCheckedInResourceCount());
        // assertEquals(1, this.pool.getCheckedInResourceCount());
    }

    // NEGATIVE Test. See comment in line.
    @Test
    public void testExceptionOnFullCheckin() throws Exception {
        assertEquals(0, this.pool.getTotalResourceCount());

        Queue<TestResource> resources = new LinkedList<TestResource>();
        for(int i = 0; i < POOL_SIZE; i++) {
            TestResource resource = this.pool.checkout("a");
            resources.add(resource);
        }
        assertEquals(POOL_SIZE, this.pool.getTotalResourceCount());

        for(int i = 0; i < POOL_SIZE; i++) {
            this.pool.checkin("a", resources.poll());
        }
        assertEquals(POOL_SIZE, this.pool.getTotalResourceCount());

        TestResource extraResource = this.factory.create("a");
        try {
            this.pool.checkin("a", extraResource);
            fail("Checking in an extra resource should throw an exception.");
        } catch(IllegalStateException ise) {
            // this is good
        }

        // KeyedResourcePool does not protect against repeated or extraneous
        // checkins. If an extraneous checkin occurs, then the checked in
        // resource is destroyed and the size of the resource pool is reduced by
        // one (even though it should not be in this exceptional case).
        assertEquals(POOL_SIZE - 1, this.pool.getTotalResourceCount());
        // assertEquals(POOL_SIZE, this.pool.getTotalResourceCount());
    }

    // NEGATIVE Test. See comment in line.
    @Test
    public void testCheckinExtraneousResource() throws Exception {
        assertEquals(0, this.pool.getTotalResourceCount());

        TestResource resource = this.pool.checkout("a");
        this.pool.checkin("a", resource);

        TestResource extraResource = this.factory.create("a");
        // KeyedResourcePool should not permit random resources to be checked
        // in. Until it protects against arbitrary resources being checked in,
        // it is possible to checkin an extraneous resource.
        this.pool.checkin("a", extraResource);
        assertEquals(1, this.pool.getTotalResourceCount());
        assertEquals(2, this.pool.getCheckedInResourceCount());
    }

    // NEGATIVE Test. See comment in line.
    @Test
    public void testNeverCheckin() throws Exception {
        assertEquals(0, this.pool.getTotalResourceCount());

        {
            this.pool.checkout("a");
        }
        // KeyedResourcePool does not protect against resources being checked
        // out and never checked back in (or destroyed).
        assertEquals(1, this.pool.getTotalResourceCount());
        assertEquals(0, this.pool.getCheckedInResourceCount());
    }

    @Test
    public void testInvalidIsDestroyed() throws Exception {
        TestResource r1 = this.pool.checkout("a");
        r1.invalidate();
        this.pool.checkin("a", r1);
        TestResource r2 = this.pool.checkout("a");
        assertTrue("Invalid objects should be destroyed.", r1 != r2);
        assertTrue("Invalid objects should be destroyed.", r1.isDestroyed());
        assertEquals(1, this.factory.getDestroyed());
    }

    @Test
    public void testMaxInvalidCreations() throws Exception {
        this.factory.setCreatedValid(false);
        try {
            this.pool.checkout("a");
            fail("Exceeded max failed attempts without exception.");
        } catch(ExcessiveInvalidResourcesException e) {
            // this is expected
        }
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

    protected static class TestResource {

        private String value;
        private AtomicBoolean isValid;
        private AtomicBoolean isDestroyed;

        public TestResource(String value) {
            this.value = value;
            this.isValid = new AtomicBoolean(true);
            this.isDestroyed = new AtomicBoolean(false);
        }

        public boolean isValid() {
            return isValid.get();
        }

        public void invalidate() {
            this.isValid.set(false);
        }

        public boolean isDestroyed() {
            return isDestroyed.get();
        }

        public void destroy() {
            this.isDestroyed.set(true);
        }

        @Override
        public String toString() {
            return "TestResource(" + value + ")";
        }

    }

    protected static class TestResourceFactory implements ResourceFactory<String, TestResource> {

        private final AtomicInteger created = new AtomicInteger(0);
        private final AtomicInteger destroyed = new AtomicInteger(0);
        private Exception createException;
        private Exception destroyException;
        private boolean isCreatedValid = true;

        public TestResource create(String key) throws Exception {
            if(createException != null)
                throw createException;
            TestResource r = new TestResource(Integer.toString(created.getAndIncrement()));
            if(!isCreatedValid)
                r.invalidate();
            return r;
        }

        public void destroy(String key, TestResource obj) throws Exception {
            if(destroyException != null)
                throw destroyException;
            destroyed.incrementAndGet();
            obj.destroy();
        }

        public boolean validate(String key, TestResource value) {
            return value.isValid();
        }

        public int getCreated() {
            return this.created.get();
        }

        public int getDestroyed() {
            return this.destroyed.get();
        }

        public void setDestroyException(Exception e) {
            this.destroyException = e;
        }

        public void setCreateException(Exception e) {
            this.createException = e;
        }

        public void setCreatedValid(boolean isValid) {
            this.isCreatedValid = isValid;
        }

        public void close() {}

    }

}
