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

import static org.junit.Assert.assertFalse;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class KeyedResourcePoolTestBase {

    protected TestResourceFactory factory;
    protected KeyedResourcePool<String, TestResource> pool;
    protected ResourcePoolConfig config;

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

        @Override
        public TestResource create(String key) throws Exception {
            if(createException != null)
                throw createException;
            TestResource r = new TestResource(Integer.toString(created.getAndIncrement()));
            if(!isCreatedValid)
                r.invalidate();
            return r;
        }

        @Override
        public void destroy(String key, TestResource obj) throws Exception {
            if(destroyException != null)
                throw destroyException;
            destroyed.incrementAndGet();
            obj.destroy();
        }

        @Override
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

        @Override
        public void close() {}

    }

    // TestResourceRequest is only need for the QueuedResourcePool tests, but it
    // is easier/cleaner to define here with the other test resources.
    protected static class TestResourceRequest implements AsyncResourceRequest<TestResource> {

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

        @Override
        public void useResource(TestResource tr) {
            // System.err.println("useResource " +
            // Thread.currentThread().getName());
            assertFalse(this.handledTimeout.get());
            assertFalse(this.handledException.get());
            usedResource.set(true);
            usedResourceCount.getAndIncrement();
            doneQueue.add(tr);
        }

        @Override
        public void handleTimeout() {
            // System.err.println("handleTimeout " +
            // Thread.currentThread().getName());
            assertFalse(this.usedResource.get());
            assertFalse(this.handledException.get());
            handledTimeout.set(true);
            handledTimeoutCount.getAndIncrement();
        }

        @Override
        public void handleException(Exception e) {
            // System.err.println("handleException " +
            // Thread.currentThread().getName());
            assertFalse(this.usedResource.get());
            assertFalse(this.handledTimeout.get());
            handledException.set(true);
            handledExceptionCount.getAndIncrement();
        }

        @Override
        public long getDeadlineNs() {
            return deadlineNs;
        }
    }

}
