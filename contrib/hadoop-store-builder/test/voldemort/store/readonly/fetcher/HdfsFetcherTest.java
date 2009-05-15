package voldemort.store.readonly.fetcher;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import junit.framework.TestCase;

import org.apache.commons.io.IOUtils;

import voldemort.MockTime;
import voldemort.utils.ByteUtils;
import voldemort.utils.Time;

/**
 * Tests for the HDFS-based fetcher
 * 
 * @author jay
 * 
 */
public class HdfsFetcherTest extends TestCase {

    public void testFetch() throws IOException {
        File testFile = File.createTempFile("test", ".dat");
        HdfsFetcher fetcher = new HdfsFetcher();
        File fetchedFile = fetcher.fetch(testFile.getAbsolutePath());
        InputStream orig = new FileInputStream(testFile);
        byte[] origBytes = IOUtils.toByteArray(orig);
        InputStream fetched = new FileInputStream(fetchedFile);
        byte[] fetchedBytes = IOUtils.toByteArray(fetched);
        assertTrue("Fetched bytes not equal to original bytes.",
                   0 == ByteUtils.compare(origBytes, fetchedBytes));
    }

    public void testThrottler() {
        // 100 reads of 1000 bytes in 1 sec
        // natural rate is 100k/sec, should be throttled to 5k
        testThrottler(1000, 100, 10, 5000);

        // 100 reads of 30000 bytes in 0.5 sec
        // natural rate is 6m/sec, should be throttled to 25k
        // testThrottler(30000, 100, 5, 25000);

        // 100 reads of 100 bytes in 5 sec
        // natural rate is 2k, no throttling
        testThrottler(100, 100, 50, 5000);
    }

    public void testThrottler(int readSize, int numReads, long readTime, long throttledRate) {
        long startTime = 1000;
        MockTime time = new MockTime(startTime);
        IoThrottler throttler = new IoThrottler(time, throttledRate, 50);
        for(int i = 0; i < numReads; i++) {
            time.addMilliseconds(readTime);
            throttler.maybeThrottle(readSize);
        }
        long doneTime = time.getMilliseconds();
        long bytesRead = numReads * readSize;
        double unthrottledSecs = readTime * numReads / (double) Time.MS_PER_SECOND;
        double ellapsedSecs = (double) (doneTime - startTime) / Time.MS_PER_SECOND;
        double observedRate = bytesRead / ellapsedSecs;
        double unthrottledRate = bytesRead / unthrottledSecs;
        if(unthrottledRate < throttledRate) {
            assertEquals(unthrottledRate, observedRate, 0.01);
        } else {
            double percentMiss = Math.abs(observedRate - throttledRate) / throttledRate;
            assertTrue("Observed I/O rate should be within 10% of throttle limit: observed = "
                       + observedRate + ", expected = " + throttledRate, percentMiss < 0.2);
        }
    }
}
