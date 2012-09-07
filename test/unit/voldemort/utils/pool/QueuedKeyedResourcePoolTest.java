package voldemort.utils.pool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

public class QueuedKeyedResourcePoolTest extends KeyedResourcePoolTest {

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
    public void testQueuingOccurs() throws Exception {
        Queue<TestResource> resources = new LinkedList<TestResource>();
        Queue<TestResourceRequest> resourceRequests = new LinkedList<TestResourceRequest>();

        long deadlineNs = System.nanoTime()
                          + TimeUnit.MILLISECONDS.toNanos(KeyedResourcePoolTest.TIMEOUT_MS);
        for(int i = 0; i < POOL_SIZE * 2; i++) {
            resourceRequests.add(new TestResourceRequest(deadlineNs, resources));
        }

        assertEquals(0, this.factory.getCreated());
        assertEquals(0, this.queuedPool.getTotalResourceCount());
        assertEquals(0, this.queuedPool.getCheckedInResourceCount());
        assertEquals(0, this.queuedPool.getQueuedResourceRequestCount());
        assertEquals(0, resources.size());

        // Submit initial POOL_SIZE requests
        for(int i = 0; i < POOL_SIZE; i++) {
            this.queuedPool.requestResource("a", resourceRequests.poll());
        }

        // Confirm initial requests were handled in nonblocking manner
        assertEquals(POOL_SIZE, this.factory.getCreated());
        assertEquals(POOL_SIZE, this.queuedPool.getTotalResourceCount());
        assertEquals(0, this.queuedPool.getCheckedInResourceCount());
        assertEquals(0, this.queuedPool.getQueuedResourceRequestCount());
        assertEquals(POOL_SIZE, resources.size());

        // Submit additional POOL_SIZE requests
        for(int i = 0; i < POOL_SIZE; i++) {
            this.queuedPool.requestResource("a", resourceRequests.poll());
        }

        // Confirm additional requests are queued
        assertEquals(POOL_SIZE, this.factory.getCreated());
        assertEquals(POOL_SIZE, this.queuedPool.getTotalResourceCount());
        assertEquals(0, this.queuedPool.getCheckedInResourceCount());
        assertEquals(POOL_SIZE, this.queuedPool.getQueuedResourceRequestCount());
        assertEquals(POOL_SIZE, resources.size());

        // Check in initial resources and confirm that this consumes queued
        // resource requests
        for(int i = 0; i < POOL_SIZE; i++) {
            this.queuedPool.checkin("a", resources.poll());

            assertEquals(POOL_SIZE, this.factory.getCreated());
            assertEquals(POOL_SIZE, this.queuedPool.getTotalResourceCount());
            assertEquals(0, this.queuedPool.getCheckedInResourceCount());
            assertEquals(POOL_SIZE - i - 1, this.queuedPool.getQueuedResourceRequestCount());
            assertEquals(POOL_SIZE, resources.size());
        }

        // Check in additional resources
        for(int i = 0; i < POOL_SIZE; i++) {
            this.queuedPool.checkin("a", resources.poll());

            assertEquals(POOL_SIZE, this.factory.getCreated());
            assertEquals(POOL_SIZE, this.queuedPool.getTotalResourceCount());
            assertEquals(i + 1, this.queuedPool.getCheckedInResourceCount());
            assertEquals(0, this.queuedPool.getQueuedResourceRequestCount());
            assertEquals(POOL_SIZE - i - 1, resources.size());
        }

        assertEquals(POOL_SIZE, this.factory.getCreated());
        assertEquals(POOL_SIZE, this.queuedPool.getTotalResourceCount());
        assertEquals(POOL_SIZE, this.queuedPool.getCheckedInResourceCount());
        assertEquals(0, this.queuedPool.getQueuedResourceRequestCount());
        assertEquals(0, resources.size());

        assertEquals(POOL_SIZE * 2, TestResourceRequest.usedResourceCount.get());
        assertEquals(0, TestResourceRequest.handledTimeoutCount.get());
        assertEquals(0, TestResourceRequest.handledExceptionCount.get());
    }

    @Test
    public void testTimeoutInQueue() throws Exception {
        Queue<TestResource> resources = new LinkedList<TestResource>();
        Queue<TestResourceRequest> resourceRequests = new LinkedList<TestResourceRequest>();

        long deadlineNs = System.nanoTime()
                          + TimeUnit.MILLISECONDS.toNanos(KeyedResourcePoolTest.TIMEOUT_MS);
        for(int i = 0; i < POOL_SIZE * 2; i++) {
            resourceRequests.add(new TestResourceRequest(deadlineNs, resources));
        }

        assertEquals(0, this.factory.getCreated());
        assertEquals(0, this.queuedPool.getTotalResourceCount());
        assertEquals(0, this.queuedPool.getCheckedInResourceCount());
        assertEquals(0, this.queuedPool.getQueuedResourceRequestCount());
        assertEquals(0, resources.size());

        // Submit initial POOL_SIZE requests
        for(int i = 0; i < POOL_SIZE; i++) {
            this.queuedPool.requestResource("a", resourceRequests.poll());
        }

        // Submit additional POOL_SIZE requests that queue up.
        for(int i = 0; i < POOL_SIZE; i++) {
            this.queuedPool.requestResource("a", resourceRequests.poll());
        }

        // Force deadline (timeout) to expire
        TimeUnit.MILLISECONDS.sleep(KeyedResourcePoolTest.TIMEOUT_MS * 2);

        // Check in one initial resource and confirm that this causes all
        // enqueued requests to be timed out.
        this.queuedPool.checkin("a", resources.poll());
        assertEquals(POOL_SIZE, this.factory.getCreated());
        assertEquals(POOL_SIZE, this.queuedPool.getTotalResourceCount());
        assertEquals(1, this.queuedPool.getCheckedInResourceCount());
        assertEquals(0, this.queuedPool.getQueuedResourceRequestCount());
        assertEquals(POOL_SIZE - 1, resources.size());

        assertEquals(POOL_SIZE, TestResourceRequest.usedResourceCount.get());
        assertEquals(POOL_SIZE, TestResourceRequest.handledTimeoutCount.get());
        assertEquals(0, TestResourceRequest.handledExceptionCount.get());

        // Check in remaining (initial) resources
        for(int i = 0; i < POOL_SIZE - 1; i++) {
            this.queuedPool.checkin("a", resources.poll());

        }

        assertEquals(POOL_SIZE, this.factory.getCreated());
        assertEquals(POOL_SIZE, this.queuedPool.getTotalResourceCount());
        assertEquals(POOL_SIZE, this.queuedPool.getCheckedInResourceCount());
        assertEquals(0, this.queuedPool.getQueuedResourceRequestCount());
        assertEquals(0, resources.size());

        assertEquals(POOL_SIZE, TestResourceRequest.usedResourceCount.get());
        assertEquals(POOL_SIZE, TestResourceRequest.handledTimeoutCount.get());
        assertEquals(0, TestResourceRequest.handledExceptionCount.get());
    }

    @Test
    public void testExceptionInQueue() throws Exception {
        Exception toThrow = new Exception("An exception!");
        this.factory.setCreateException(toThrow);

        Queue<TestResource> resources = new LinkedList<TestResource>();

        long deadlineNs = System.nanoTime()
                          + TimeUnit.MILLISECONDS.toNanos(KeyedResourcePoolTest.TIMEOUT_MS);
        TestResourceRequest trr = new TestResourceRequest(deadlineNs, resources);

        assertEquals(0, this.factory.getCreated());
        assertEquals(0, this.queuedPool.getTotalResourceCount());
        assertEquals(0, this.queuedPool.getCheckedInResourceCount());
        assertEquals(0, this.queuedPool.getQueuedResourceRequestCount());
        assertEquals(0, resources.size());

        this.queuedPool.requestResource("a", trr);

        assertEquals(0, this.factory.getCreated());
        assertEquals(0, this.queuedPool.getTotalResourceCount());
        assertEquals(0, this.queuedPool.getCheckedInResourceCount());
        assertEquals(0, this.queuedPool.getQueuedResourceRequestCount());
        assertEquals(0, resources.size());

        assertEquals(0, TestResourceRequest.usedResourceCount.get());
        assertEquals(0, TestResourceRequest.handledTimeoutCount.get());
        assertEquals(1, TestResourceRequest.handledExceptionCount.get());
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
            assertEquals(0, this.queuedPool.getQueuedResourceRequestCount());

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
            assertEquals(POOL_SIZE, this.queuedPool.getTotalResourceCount());
            assertEquals(POOL_SIZE, this.queuedPool.getCheckedInResourceCount());
            assertEquals(0, this.queuedPool.getQueuedResourceRequestCount());

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

                    queuedPool.requestResource(key, new TestResourceRequest(deadlineNs, resources));
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

    protected static class TestResourceRequest implements ResourceRequest<TestResource> {

        private AtomicBoolean usedResource;
        private AtomicBoolean handledTimeout;
        private AtomicBoolean handledException;

        static AtomicInteger usedResourceCount = new AtomicInteger(0);
        static AtomicInteger handledTimeoutCount = new AtomicInteger(0);
        static AtomicInteger handledExceptionCount = new AtomicInteger(0);

        long deadlineNs;
        final Queue<TestResource> doneQueue;

        TestResourceRequest(long deadlineNs, Queue<TestResource> doneQueue) {
            this.usedResource = new AtomicBoolean(false);
            this.handledTimeout = new AtomicBoolean(false);
            this.handledException = new AtomicBoolean(false);
            this.deadlineNs = deadlineNs;
            this.doneQueue = doneQueue;
        }

        public void useResource(TestResource tr) {
            // System.err.println("useResource " +
            // Thread.currentThread().getName());
            assertFalse(this.handledTimeout.get());
            assertFalse(this.handledException.get());
            usedResource.set(true);
            usedResourceCount.getAndIncrement();
            doneQueue.add(tr);
        }

        public void handleTimeout() {
            // System.err.println("handleTimeout " +
            // Thread.currentThread().getName());
            assertFalse(this.usedResource.get());
            assertFalse(this.handledException.get());
            handledTimeout.set(true);
            handledTimeoutCount.getAndIncrement();
        }

        public void handleException(Exception e) {
            // System.err.println("handleException " +
            // Thread.currentThread().getName());
            assertFalse(this.usedResource.get());
            assertFalse(this.handledTimeout.get());
            handledException.set(true);
            handledExceptionCount.getAndIncrement();
        }

        public long getDeadlineNs() {
            return deadlineNs;
        }
    }
}
