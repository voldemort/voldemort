package voldemort.performance;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.TestUtils;
import voldemort.utils.Time;

public abstract class PerformanceTest {
    
    private final AtomicInteger numberOfFailures = new AtomicInteger(0);
    private long elapsedTimeNs;
    private long[] operationTimes;
    private volatile boolean hasCompleted;
    private int numberOfThreads;
    
    public abstract void doOperation(int index) throws Exception;
    
    public void setUp() {
        // override me to do stuff
    }
    
    public void tearDown() {
        // override me to do stuff
    }
    
    public void run(int numRequests, int numThreads) {
        setUp();
        try {
            this.numberOfThreads = numThreads;
            this.hasCompleted = false;
            this.numberOfFailures.set(0);
            this.operationTimes = new long[numRequests];
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            final CountDownLatch latch = new CountDownLatch(numRequests);
            final AtomicInteger index = new AtomicInteger(0);
            
            long start = System.nanoTime();
            for(int i = 0; i < numRequests; i++) {
                executor.execute(
                        new Runnable() {
                            public void run() {
                                int current = index.getAndIncrement();
                                long begin = System.nanoTime();
                                try {
                                    doOperation(current);
                                } catch(Exception e) {
                                    numberOfFailures.getAndIncrement();
                                    e.printStackTrace();
                                } finally {
                                    operationTimes[current] = System.nanoTime() - begin;
                                    latch.countDown();
                                }
                            }
                        });
            }
            
            try {
                latch.await();
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
            
            this.hasCompleted = true;
            this.elapsedTimeNs = System.nanoTime() - start;
            executor.shutdownNow();
            try {
                executor.awaitTermination(1, TimeUnit.SECONDS);
            } catch(InterruptedException e) {}
        } finally {
            tearDown();
        }
    }
    
    public void printStats() {
        if(!hasCompleted)
            throw new RuntimeException("Hasn't finished running yet!");
        System.out.println("Total number of operations: " + this.operationTimes.length);
        System.out.println("Total elapsed seconds: " + this.elapsedTimeNs / (double) Time.NS_PER_SECOND);
        System.out.println("Number of failures: " + this.numberOfFailures.get());
        System.out.println("Number of threads: " + this.numberOfThreads);
        System.out.println("Avg. operations/second: " + getOperationsPerSecond());
        System.out.println("Average time: " + getAverageOperationTimeMs() + " ms");
        System.out.println("Median time: " + getOperationTimeMsQuantile(0.5d) + " ms");
        System.out.println("1st percentile: " + getOperationTimeMsQuantile(0.01d) + " ms");      
        System.out.println("99th percentile: " + getOperationTimeMsQuantile(0.99d) + " ms");       
    }
    
    public double getOperationsPerSecond() {
        if(!hasCompleted)
            throw new RuntimeException("Hasn't finished running yet!");
        double elapsedSeconds = this.elapsedTimeNs / (double) Time.NS_PER_SECOND;
        return this.operationTimes.length / elapsedSeconds;
    }
    
    public double getOperationTimeMsQuantile(double quantile) {
        if(!hasCompleted)
            throw new RuntimeException("Hasn't finished running yet!");
        return TestUtils.quantile(this.operationTimes, quantile) / (double) Time.NS_PER_MS;
    }
    
    public double getAverageOperationTimeMs() {
        if(!hasCompleted)
            throw new RuntimeException("Hasn't finished running yet!");
        
        double sum = 0.0;
        for(int i = 0; i < this.operationTimes.length; i++)
            sum += this.operationTimes[i];
        sum /= Time.NS_PER_MS;
        return sum / this.operationTimes.length;
    }
    
}
