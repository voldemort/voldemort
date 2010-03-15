/*
 * Copyright 2008-2010 LinkedIn, Inc
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

package voldemort.server.protocol.admin;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;
import voldemort.server.scheduler.SchedulerService;
import voldemort.utils.SystemTime;

/**
 */
public class AsyncOperationTest extends TestCase {

    public void testAsyncOperationService() throws Exception {
        SchedulerService schedulerService = new SchedulerService(2, SystemTime.INSTANCE);
        AsyncOperationService asyncOperationService = new AsyncOperationService(schedulerService, 10);

        final AtomicBoolean completedOp0 = new AtomicBoolean(false);
        final AtomicBoolean completedOp1 = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);

        int opId0 = asyncOperationService.getUniqueRequestId();
        asyncOperationService.submitOperation(opId0,
                                             new AsyncOperation(opId0, "op0") {
                                                 @Override
                                                 public void operate() throws Exception {
                                                     Thread.sleep(1000);
                                                     completedOp0.set(true);
                                                 }

                                                 @Override
                                                 public void stop() {

                                                 }
                                             });

        int opId1 = asyncOperationService.getUniqueRequestId();
        asyncOperationService.submitOperation(opId1,
                                             new AsyncOperation(opId1, "op1") {
                                                 @Override
                                                 public void operate() throws Exception {
                                                     completedOp1.set(true);
                                                     latch.countDown();
                                                 }

                                                 @Override
                                                 public void stop() {

                                                 }
                                             });
        latch.await();
        List<Integer> opList = asyncOperationService.getAsyncOperationList(false);
        assertFalse("doesn't list completed operations", opList.contains(1));
        assertTrue("lists a pending operation", opList.contains(0));
        opList = asyncOperationService.getAsyncOperationList(true);
        assertTrue("lists all operations", opList.containsAll(Arrays.asList(0,1)));

        Thread.sleep(1000);

        assertTrue("operation 0 finished", completedOp0.get());
        assertTrue("operation 1 finished", completedOp1.get());
    }

    @SuppressWarnings("unchecked")
    public void testAsyncOperationRepository() {
        Map<Integer, AsyncOperation> operations = Collections.synchronizedMap(new AsyncOperationCache(2));
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        AsyncOperation completeLater = new AsyncOperation(0, "test") {

            @Override
            public void stop() {}

            @Override
            public void operate() {
                try {
                    Thread.sleep(1500);
                    countDownLatch.countDown();
                } catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(5);

        AsyncOperation completeNow = new AsyncOperation(1, "test 2") {
            @Override
            public void stop() {}

            @Override
            public void operate() {}
        };

        AsyncOperation completeSoon = new AsyncOperation(2, "test3") {
            @Override
            public void stop() {}
            @Override
            public void operate() {
                try {
                    Thread.sleep(500);
                } catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        executorService.submit(completeLater);
        executorService.submit(completeNow);
        executorService.submit(completeSoon);

        operations.put(0, completeLater);
        operations.put(1, completeNow);
        operations.put(2, completeSoon);

        assertTrue("Handles overflow okay", operations.containsKey(2));

        try {
            countDownLatch.await();
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        completeSoon = new AsyncOperation(2, "test3") {
            @Override
            public void stop() {}
            @Override
            public void operate() {
                try {
                    Thread.sleep(500);
                } catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        operations.put(3, completeSoon);

        assertFalse("Actually does LRU heuristics", operations.containsKey(0));
    }
}
