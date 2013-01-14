package voldemort.store.stats;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;

import voldemort.utils.Time;

public class SimpleCounterTest {

    final private static int COUNTER_RESET_INTERVAL_MS = 100;
    private SimpleCounter simpleCounter;

    @Before
    public void setUp() {
        simpleCounter = new SimpleCounter(COUNTER_RESET_INTERVAL_MS);
    }

    private static void sleepForResetInterval(long startTimeMs) {
        try {
            long stopTimeMs = startTimeMs + COUNTER_RESET_INTERVAL_MS;
            Thread.sleep(stopTimeMs - System.currentTimeMillis() + 1);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSingleThread() {
        // Interval 0
        assertEquals(0.0, simpleCounter.getAvgEventValue(), 0.0);
        assertEquals(0.0, simpleCounter.getEventRate(), 0.0);

        // Interval 1- add some samples
        long startTimeMs = System.currentTimeMillis();
        for(int i = 0; i < 10; i++)
            simpleCounter.count();
        sleepForResetInterval(startTimeMs);

        // Interval 2
        startTimeMs = System.currentTimeMillis();
        for(int i = 0; i < 10; i++)
            simpleCounter.count(100);
        // verify the stats returned are for the first interval
        assertEquals(0.0, simpleCounter.getAvgEventValue(), 0.0);
        assertEquals(10 / ((COUNTER_RESET_INTERVAL_MS * 1.0) / Time.MS_PER_SECOND),
                     simpleCounter.getEventRate(),
                     0.0);
        sleepForResetInterval(startTimeMs);

        // Interval 3
        // verify the stats returned are for the second interval and that
        // multiple calls during the current interval will always provide the
        // same result
        startTimeMs = System.currentTimeMillis();
        for(int i = 0; i < 10; i++) {
            assertEquals(100.0, simpleCounter.getAvgEventValue(), 0.0);
            assertEquals(10 / ((COUNTER_RESET_INTERVAL_MS * 1.0) / Time.MS_PER_SECOND),
                         simpleCounter.getEventRate(),
                         0.0);
        }
        sleepForResetInterval(startTimeMs);

        // No activity
        assertEquals(0.0, simpleCounter.getAvgEventValue(), 0.0);
        assertEquals(0.0, simpleCounter.getEventRate(), 0.0);
    }

    @Test
    public void testMultipleThreads() throws InterruptedException {
        ExecutorService executorService = null;
        try {
            final int NUM_THREADS = 5;
            final int NUM_OPS = 10000;

            long startTimeMs = System.currentTimeMillis();
            executorService = Executors.newFixedThreadPool(NUM_THREADS);
            final CountDownLatch latch1 = new CountDownLatch(NUM_THREADS);
            final CountDownLatch latch0 = new CountDownLatch(1);

            for(int i = 0; i < NUM_THREADS; i++) {
                final int threadId = i;
                executorService.submit(new Runnable() {

                    public void run() {
                        try {
                            latch0.await();
                            for(int j = 0; j < NUM_OPS; j++) {
                                simpleCounter.count(100 * (threadId + 1));
                            }
                        } catch(InterruptedException e) {
                            e.printStackTrace();
                        } finally {
                            latch1.countDown();
                        }
                    }
                });
            }
            latch0.countDown();
            latch1.await();
            // one more sleep so we expire the current interval where all the
            // action happened
            sleepForResetInterval(startTimeMs);

            startTimeMs = System.currentTimeMillis();
            assertEquals(300.0, simpleCounter.getAvgEventValue(), 0.0);
            assertEquals((NUM_OPS * NUM_THREADS)
                                 / ((COUNTER_RESET_INTERVAL_MS * 1.0) / Time.MS_PER_SECOND),
                         simpleCounter.getEventRate(),
                         0.0);
            sleepForResetInterval(startTimeMs);

            // Run for a long period spannning multiple intervals and see if we
            // observe if we see consitent metrics
            final ConcurrentLinkedQueue<Double> observedEventRate = new ConcurrentLinkedQueue<Double>();
            final ConcurrentLinkedQueue<Double> observedEventValueAvg = new ConcurrentLinkedQueue<Double>();
            final int NUM_INTERVALS = 30;
            final CountDownLatch latch2 = new CountDownLatch(NUM_THREADS);
            for(int i = 0; i < NUM_THREADS; i++) {
                executorService.submit(new Runnable() {

                    public void run() {
                        try {
                            for(int interval = 0; interval < NUM_INTERVALS; interval++) {
                                long startTimeMs = System.currentTimeMillis();
                                for(int j = 0; j < NUM_OPS; j++) {
                                    simpleCounter.count(100);
                                }
                                sleepForResetInterval(startTimeMs);
                            }
                            observedEventRate.add(simpleCounter.getEventRate());
                            observedEventValueAvg.add(simpleCounter.getAvgEventValue());
                        } finally {
                            latch2.countDown();
                        }
                    }
                });
            }

            latch2.await();
            Object[] actualEventRates = new Object[NUM_THREADS];
            Object[] actualEventValueAvgs = new Object[NUM_THREADS];
            for(int i = 0; i < NUM_THREADS; i++) {
                actualEventRates[i] = (NUM_OPS * NUM_THREADS)
                                      / ((COUNTER_RESET_INTERVAL_MS * 1.0) / Time.MS_PER_SECOND);
                actualEventValueAvgs[i] = 100.0;
            }
            assertEquals(Arrays.equals(observedEventRate.toArray(), actualEventRates), true);
            assertEquals(Arrays.equals(observedEventValueAvg.toArray(), actualEventValueAvgs), true);

        } finally {
            if(executorService != null)
                executorService.shutdown();
        }
    }
}
