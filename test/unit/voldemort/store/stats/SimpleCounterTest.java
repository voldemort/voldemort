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

    final private static int COUNTER_RESET_INTERVAL_MS = 50;
    private SimpleCounter simpleCounter;

    @Before
    public void setUp() {
        simpleCounter = new SimpleCounter(COUNTER_RESET_INTERVAL_MS);
    }

    private static void sleepForResetInterval() {
        try {
            Thread.sleep(COUNTER_RESET_INTERVAL_MS);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSingleThread() {
        // Interval 0
        assertEquals(0.0, simpleCounter.getAvgEventValue(), 0.0);
        assertEquals(0.0, simpleCounter.getEventRate(), 0.0);

        // Interval 1
        // add some samples
        for(int i = 0; i < 10; i++)
            simpleCounter.count();
        sleepForResetInterval();

        // Interval 2
        for(int i = 0; i < 10; i++)
            simpleCounter.count(100);
        // verify the stats returned are for the first interval
        assertEquals(0.0, simpleCounter.getAvgEventValue(), 0.0);
        assertEquals(10 / ((COUNTER_RESET_INTERVAL_MS * 1.0) / Time.MS_PER_SECOND),
                     simpleCounter.getEventRate(),
                     0.0);
        sleepForResetInterval();

        // Interval 3
        // verify the stats returned are for the second interval and that
        // multiple calls during the current interval will always provide the
        // same result
        for(int i = 0; i < 10; i++) {
            assertEquals(100.0, simpleCounter.getAvgEventValue(), 0.0);
            assertEquals(10 / ((COUNTER_RESET_INTERVAL_MS * 1.0) / Time.MS_PER_SECOND),
                         simpleCounter.getEventRate(),
                         0.0);
        }
        sleepForResetInterval();

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
            executorService = Executors.newFixedThreadPool(NUM_THREADS);
            final CountDownLatch latch1 = new CountDownLatch(NUM_THREADS);
            for(int i = 0; i < NUM_THREADS; i++) {
                final int threadId = i;
                executorService.submit(new Runnable() {

                    public void run() {
                        // additional sleep so they all start together and run
                        // concurrently
                        try {
                            sleepForResetInterval();
                            for(int j = 0; j < NUM_OPS; j++) {
                                simpleCounter.count(100 * (threadId + 1));
                            }
                        } finally {
                            latch1.countDown();
                        }
                    }
                });
            }
            latch1.await();
            // one more sleep so we expire the current interval where all the
            // action happened
            sleepForResetInterval();

            assertEquals(300.0, simpleCounter.getAvgEventValue(), 0.0);
            assertEquals((NUM_OPS * NUM_THREADS)
                                 / ((COUNTER_RESET_INTERVAL_MS * 1.0) / Time.MS_PER_SECOND),
                         simpleCounter.getEventRate(),
                         0.0);
            sleepForResetInterval();

            // Run for a long period spannning multiple intervals and see if we
            // observe if we see consitent metrics
            final ConcurrentLinkedQueue<Double> observedEventRate = new ConcurrentLinkedQueue<Double>();
            final ConcurrentLinkedQueue<Double> observedEventValueAvg = new ConcurrentLinkedQueue<Double>();
            final int NUM_INTERVALS = 30;
            final CountDownLatch latch2 = new CountDownLatch(NUM_THREADS);

            for(int i = 0; i < NUM_THREADS; i++) {
                final int threadId = i;
                executorService.submit(new Runnable() {

                    public void run() {
                        try {
                            for(int interval = 0; interval < NUM_INTERVALS; interval++) {
                                sleepForResetInterval();
                                for(int j = 0; j < NUM_OPS; j++) {
                                    simpleCounter.count(100);
                                }
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
