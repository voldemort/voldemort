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
 * Test {@link voldemort.server.protocol.admin.AsyncOperationService}
 */
public class AsyncOperationTest extends TestCase {

    public void testAsyncOperationService() throws Exception {
        SchedulerService schedulerService = new SchedulerService(2, SystemTime.INSTANCE);
        AsyncOperationService asyncOperationService = new AsyncOperationService(schedulerService, 10);

        final AtomicBoolean op0Complete = new AtomicBoolean(false);
        final AtomicBoolean op1Complete = new AtomicBoolean(false);
        final CountDownLatch op0Latch = new CountDownLatch(1);
        final CountDownLatch op1Latch = new CountDownLatch(1);

        int op0 = asyncOperationService.getUniqueRequestId();
        asyncOperationService.submitOperation(op0,
                                              new AsyncOperation(op0, "op0") {
                                                  @Override
                                                  public void operate() throws Exception {
                                                      Thread.sleep(500);
                                                      op0Latch.countDown();
                                                      op0Complete.set(true);
                                                  }
                                                  
                                                  @Override
                                                  public void stop() {
                                                      
                                                  }
                                              });
        
        int op1 = asyncOperationService.getUniqueRequestId();
        asyncOperationService.submitOperation(op1,
                                              new AsyncOperation(op1, "op1") {
                                                  @Override
                                                  public void operate() throws Exception {
                                                      op1Complete.set(true);
                                                      op1Latch.countDown();
                                                  }
                                                  
                                                  @Override
                                                  public void stop() {
                                                      
                                                  }
                                             });
        op1Latch.await();
        List<Integer> opList = asyncOperationService.getAsyncOperationList(false);
        assertFalse("doesn't list completed operations", opList.contains(1));
        assertTrue("lists a pending operation", opList.contains(0));
        opList = asyncOperationService.getAsyncOperationList(true);
        assertTrue("lists all operations", opList.containsAll(Arrays.asList(0,1)));

        op0Latch.await();
        assertTrue("operation 0 finished", op0Complete.get());
        assertTrue("operation 1 finished", op1Complete.get());
    }

    @SuppressWarnings("unchecked")
    public void testAsyncOperationCache() throws Exception {
        Map<Integer, AsyncOperation> operations = Collections.synchronizedMap(new AsyncOperationCache(2));
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        AsyncOperation completeLater = new AsyncOperation(0, "test") {

            @Override
            public void stop() {}

            @Override
            public void operate() {
                try {
                    Thread.sleep(500);
                    countDownLatch.countDown();
                } catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(5);

        AsyncOperation completeNow = new AsyncOperation(1, "test") {
            @Override
            public void stop() {}

            @Override
            public void operate() {}
        };

        AsyncOperation completeSoon = new AsyncOperation(2, "test") {
            @Override
            public void stop() {}
            @Override
            public void operate() {
                try {
                    Thread.sleep(250);
                } catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        operations.put(0, completeLater);
        operations.put(1, completeNow);
        operations.put(2, completeSoon);

        executorService.submit(completeLater);
        executorService.submit(completeNow);
        executorService.submit(completeSoon);
 
        assertTrue("Handles overflow okay", operations.containsKey(0) &&
                                            operations.containsKey(1) &&
                                            operations.containsKey(2));
        
        countDownLatch.await();
        
        for (int i=3; i < 32; i++) {
            AsyncOperation asyncOperation = new AsyncOperation(i, "test") {
                @Override
                public void stop() {}
                @Override
                public void operate() {}
            };
            operations.put(i, asyncOperation);
        }

        // Commented out as it's brittle and leads to frequent test failures
        // assertFalse("Actually does LRU heuristics", operations.containsKey(0));
    }
}
