package voldemort.server.protocol.admin;

import junit.framework.TestCase;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author afeinberg
 */
public class AsyncOperationTest extends TestCase {
    @SuppressWarnings("unchecked")
    public void testAsyncOperationRepository() {
        Map<String, AsyncOperation> operations = new AsyncOperationRepository(2);


        AsyncOperation completeLater = new AsyncOperation(0, "Complete later") {
            public void apply() {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };


        ExecutorService executorService = Executors.newFixedThreadPool(5);
        executorService.submit(completeLater);

        AsyncOperation completeNow = new AsyncOperation(1, "Complete now") {
            public void apply () {
            }
        };
        executorService.submit(completeNow);

        operations.put("foo1", completeLater);
        operations.put("foo3", completeNow);
        operations.put("foo4", completeLater);


        operations.put("foo5", completeLater);


        assertTrue("Handles overflow okay", operations.containsKey("foo4"));

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        operations.put("foo5", completeLater);
        assertTrue(operations.containsKey("foo5"));
        assertFalse("Actually does LRU heuristics", operations.containsKey("foo3"));
    }
}
