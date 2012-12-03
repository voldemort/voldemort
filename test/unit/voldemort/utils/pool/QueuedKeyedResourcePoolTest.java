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

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

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
        assertEquals(0, this.queuedPool.getRegisteredResourceRequestCount());
        assertEquals(0, resources.size());

        // Submit initial POOL_SIZE requests
        for(int i = 0; i < POOL_SIZE; i++) {
            this.queuedPool.registerResourceRequest("a", resourceRequests.poll());
        }

        // Confirm initial requests were handled in nonblocking manner
        assertEquals(POOL_SIZE, this.factory.getCreated());
        assertEquals(POOL_SIZE, this.queuedPool.getTotalResourceCount());
        assertEquals(0, this.queuedPool.getCheckedInResourceCount());
        assertEquals(0, this.queuedPool.getRegisteredResourceRequestCount());
        assertEquals(POOL_SIZE, resources.size());

        // Submit additional POOL_SIZE requests
        for(int i = 0; i < POOL_SIZE; i++) {
            this.queuedPool.registerResourceRequest("a", resourceRequests.poll());
        }

        // Confirm additional requests are queued
        assertEquals(POOL_SIZE, this.factory.getCreated());
        assertEquals(POOL_SIZE, this.queuedPool.getTotalResourceCount());
        assertEquals(0, this.queuedPool.getCheckedInResourceCount());
        assertEquals(POOL_SIZE, this.queuedPool.getRegisteredResourceRequestCount());
        assertEquals(POOL_SIZE, resources.size());

        // Check in initial resources and confirm that this consumes queued
        // resource requests
        for(int i = 0; i < POOL_SIZE; i++) {
            this.queuedPool.checkin("a", resources.poll());

            assertEquals(POOL_SIZE, this.factory.getCreated());
            assertEquals(POOL_SIZE, this.queuedPool.getTotalResourceCount());
            assertEquals(0, this.queuedPool.getCheckedInResourceCount());
            assertEquals(POOL_SIZE - i - 1, this.queuedPool.getRegisteredResourceRequestCount());
            assertEquals(POOL_SIZE, resources.size());
        }

        // Check in additional resources
        for(int i = 0; i < POOL_SIZE; i++) {
            this.queuedPool.checkin("a", resources.poll());

            assertEquals(POOL_SIZE, this.factory.getCreated());
            assertEquals(POOL_SIZE, this.queuedPool.getTotalResourceCount());
            assertEquals(i + 1, this.queuedPool.getCheckedInResourceCount());
            assertEquals(0, this.queuedPool.getRegisteredResourceRequestCount());
            assertEquals(POOL_SIZE - i - 1, resources.size());
        }

        assertEquals(POOL_SIZE, this.factory.getCreated());
        assertEquals(POOL_SIZE, this.queuedPool.getTotalResourceCount());
        assertEquals(POOL_SIZE, this.queuedPool.getCheckedInResourceCount());
        assertEquals(0, this.queuedPool.getRegisteredResourceRequestCount());
        assertEquals(0, resources.size());

        assertEquals(POOL_SIZE * 2, TestResourceRequest.usedResourceCount.get());
        assertEquals(0, TestResourceRequest.handledTimeoutCount.get());
        assertEquals(0, TestResourceRequest.handledExceptionCount.get());
    }

    @Test
    public void testQueuingStats() throws Exception {
        Queue<TestResource> resources = new LinkedList<TestResource>();
        Queue<TestResourceRequest> resourceRequests = new LinkedList<TestResourceRequest>();

        long deadlineNs = System.nanoTime()
                          + TimeUnit.MILLISECONDS.toNanos(KeyedResourcePoolTest.TIMEOUT_MS);
        for(int i = 0; i < POOL_SIZE * 10001; i++) {
            resourceRequests.add(new TestResourceRequest(deadlineNs, resources));
        }

        assertEquals(0, this.factory.getCreated());
        assertEquals(0, this.queuedPool.getTotalResourceCount());
        assertEquals(0, this.queuedPool.getCheckedInResourceCount());
        assertEquals(0, this.queuedPool.getRegisteredResourceRequestCount());
        assertEquals(0, resources.size());

        // Submit initial POOL_SIZE requests
        for(int i = 0; i < POOL_SIZE; i++) {
            this.queuedPool.registerResourceRequest("a", resourceRequests.poll());
        }

        // Confirm initial requests were handled in nonblocking manner
        assertEquals(POOL_SIZE, this.factory.getCreated());
        assertEquals(POOL_SIZE, this.queuedPool.getTotalResourceCount());
        assertEquals(0, this.queuedPool.getCheckedInResourceCount());
        assertEquals(0, this.queuedPool.getRegisteredResourceRequestCount());
        assertEquals(POOL_SIZE, resources.size());

        // Register five order of magnitude more resource requests.
        for(int i = 0; i < POOL_SIZE * 10000; i++) {
            this.queuedPool.registerResourceRequest("a", resourceRequests.poll());
        }

        long startNs = System.nanoTime();
        assertEquals(POOL_SIZE * 10000, this.queuedPool.getRegisteredResourceRequestCount());
        long durationNs = System.nanoTime() - startNs;
        // 20 ms is an arbitrary time limit:
        // POOL_SIZE * 10000 resource requests / 20 ms
        // = 50000 resource requests/20ms
        // = 2500 resource requests/ms
        assertTrue("O(n) count of queue is too slow: " + durationNs + " ns.",
                   durationNs < TimeUnit.MILLISECONDS.toNanos(20));

        // Confirm additional requests are queued
        assertEquals(POOL_SIZE, this.factory.getCreated());
        assertEquals(POOL_SIZE, this.queuedPool.getTotalResourceCount());
        assertEquals(0, this.queuedPool.getCheckedInResourceCount());
        assertEquals(POOL_SIZE, resources.size());

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
        assertEquals(0, this.queuedPool.getRegisteredResourceRequestCount());
        assertEquals(0, resources.size());

        // Submit initial POOL_SIZE requests
        for(int i = 0; i < POOL_SIZE; i++) {
            this.queuedPool.registerResourceRequest("a", resourceRequests.poll());
        }

        // Submit additional POOL_SIZE requests that queue up.
        for(int i = 0; i < POOL_SIZE; i++) {
            this.queuedPool.registerResourceRequest("a", resourceRequests.poll());
        }

        // Force deadline (timeout) to expire
        TimeUnit.MILLISECONDS.sleep(KeyedResourcePoolTest.TIMEOUT_MS * 2);

        // Check in one initial resource and confirm that this causes all
        // enqueued requests to be timed out.
        this.queuedPool.checkin("a", resources.poll());
        assertEquals(POOL_SIZE, this.factory.getCreated());
        assertEquals(POOL_SIZE, this.queuedPool.getTotalResourceCount());
        assertEquals(1, this.queuedPool.getCheckedInResourceCount());
        assertEquals(0, this.queuedPool.getRegisteredResourceRequestCount());
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
        assertEquals(0, this.queuedPool.getRegisteredResourceRequestCount());
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
        assertEquals(0, this.queuedPool.getRegisteredResourceRequestCount());
        assertEquals(0, resources.size());

        this.queuedPool.registerResourceRequest("a", trr);

        assertEquals(0, this.factory.getCreated());
        assertEquals(0, this.queuedPool.getTotalResourceCount());
        assertEquals(0, this.queuedPool.getCheckedInResourceCount());
        assertEquals(0, this.queuedPool.getRegisteredResourceRequestCount());
        assertEquals(0, resources.size());

        assertEquals(0, TestResourceRequest.usedResourceCount.get());
        assertEquals(0, TestResourceRequest.handledTimeoutCount.get());
        assertEquals(1, TestResourceRequest.handledExceptionCount.get());
    }
}
