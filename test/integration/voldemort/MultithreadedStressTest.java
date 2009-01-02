package voldemort;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.store.ObsoleteVersionException;
import voldemort.store.Store;
import voldemort.versioning.Versioned;

/**
 * Big honkin multithreaded stress test
 * 
 * @author jay
 *
 */
public class MultithreadedStressTest {
    
    private final ExecutorService service;
    private final Store<byte[],byte[]> store;
    private final AtomicInteger value;
    private final int numberOfValues;
    private final int numberOfRequests;
    
    @SuppressWarnings("unchecked")
    public MultithreadedStressTest(Store<byte[],byte[]> store, int numberOfValues, int numberOfRequests, int numberOfThreads) {
        this.numberOfValues = numberOfValues;
        this.numberOfRequests = numberOfRequests;
        this.service = Executors.newFixedThreadPool(numberOfThreads);
        this.store = store;
        this.value = new AtomicInteger(0);
        for(int i = 0; i < numberOfValues; i++)
            this.store.put(Integer.toString(i).getBytes(), new Versioned<byte[]>(Integer.toString(i).getBytes()));
    }
    
    public void testGetAndPut() throws Exception {
        final AtomicInteger obsoletes = new AtomicInteger(0);
        final CountDownLatch isDone = new CountDownLatch(numberOfRequests);
        for(int i = 0; i < numberOfRequests; i++) {
            final int index = i % numberOfValues;
            service.execute(
            new Runnable() {
                public void run() {
                    boolean done = false;
                    while(!done) {
                        try {
                            byte[] key = Integer.toString(index).getBytes();
                            List<Versioned<byte[]>> found = store.get(key);
                            if(found.size() > 1) {
                                throw new RuntimeException("Found multiple versions: " + found);
                            } else if(found.size() == 1) {
                                Versioned<byte[]> versioned = found.get(0);
                                byte[] valueBytes = Integer.toString(MultithreadedStressTest.this.value.getAndIncrement()).getBytes();
                                versioned.setObject(valueBytes);
                                store.put(key, versioned);
                                done = true;
                            } else if(found.size() == 0) {
                                throw new RuntimeException("No values found!");
                            }
                        } catch(ObsoleteVersionException e) {
                            obsoletes.getAndIncrement();
                        } finally {
                            isDone.countDown();
                        }
                    }
                }
            });
        }
        isDone.await();
        System.err.println("Number of obsoletes: " + obsoletes.get());
        System.exit(0);
    }
    
}
