package voldemort.server.protocol.admin;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;
import voldemort.server.scheduler.SchedulerService;
import voldemort.utils.SystemTime;
import voldemort.utils.impl.CommandOutputListener;

/**
 * @author afeinberg
 */
public class AsyncOperationTest extends TestCase {

    public void testAsyncOperationRunner() throws Exception {
        SchedulerService schedulerService = new SchedulerService(2, SystemTime.INSTANCE);
        AsyncOperationRunner asyncOperationRunner = new AsyncOperationRunner(schedulerService, 10);

        final AtomicBoolean completedOp0 = new AtomicBoolean(false);
        final AtomicBoolean completedOp1 = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);

        int opId0 = asyncOperationRunner.getUniqueRequestId();
        asyncOperationRunner.submitOperation(opId0,
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

        int opId1 = asyncOperationRunner.getUniqueRequestId();
        asyncOperationRunner.submitOperation(opId1,
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
        List<Integer> opList = asyncOperationRunner.getAsyncOperationList(false);
        assertFalse("doesn't list completed operations", opList.contains(1));
        assertTrue("lists a pending operation", opList.contains(0));
        opList = asyncOperationRunner.getAsyncOperationList(true);
        assertTrue("lists all operations", opList.containsAll(Arrays.asList(0,1)));

        Thread.sleep(1000);

        assertTrue("operation 0 finished", completedOp0.get());
        assertTrue("operation 1 finished", completedOp1.get());
    }

    @SuppressWarnings("unchecked")
    public void testAsyncOperationRepository() {
        Map<String, AsyncOperation> operations = new AsyncOperationRepository(2);

        AsyncOperation completeLater = new AsyncOperation(0, "test") {

            @Override
            public void stop() {}

            @Override
            public void operate() {
                try {
                    Thread.sleep(2000);
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

        operations.put("foo1", completeLater);
        operations.put("foo2", completeNow);
        operations.put("foo3", completeSoon);
        operations.put("foo4", completeLater);
        operations.put("foo5", completeLater);

        assertTrue("Handles overflow okay", operations.containsKey("foo4"));

        try {
            Thread.sleep(1000);
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        operations.put("foo5", completeLater);

        assertTrue(operations.containsKey("foo5"));

        for (int i = 0; i < 10; i++) { 
            operations.put("foo" + 5 + i, completeLater);
        }

        assertFalse("Actually does LRU heuristics", operations.containsKey("foo2"));
    }
}
