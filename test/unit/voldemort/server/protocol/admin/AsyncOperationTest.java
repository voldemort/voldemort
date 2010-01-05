package voldemort.server.protocol.admin;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

/**
 * @author afeinberg
 */
public class AsyncOperationTest extends TestCase {

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
