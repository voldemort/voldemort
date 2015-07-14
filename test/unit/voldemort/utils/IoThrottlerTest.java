/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.utils;

import junit.framework.TestCase;
import io.tehuti.utils.MockTime;

public class IoThrottlerTest extends TestCase {

    public void testThrottler() {
        // 100 reads of 1000 bytes in 1 sec
        // natural rate is 100 KB/sec, should be throttled to 5 KB/sec
        testThrottler(1000, 100, 10, 5000);

        // 100 reads of 1 MB in 1 sec
        // natural rate is 100 MB/sec, should be throttled to 1 MB/sec
        testThrottler(1000000, 100, 10, 1000000);

        // 100 reads of 30000 bytes in 0.5 sec
        // natural rate is 6 MB/sec, should be throttled to 25 KB/sec
        testThrottler(30000, 100, 5, 25000);

        // 100 reads of 100 bytes in 5 sec
        // natural rate is 2 KB/sec, no throttling
        testThrottler(100, 100, 50, 5000);
    }

    public void testThrottler(int readSize, int numReads, long readTime, long throttledRate) {
        MockTime time = new MockTime();
        long startTime = time.milliseconds();
        EventThrottler throttler = new EventThrottler(time, throttledRate, 1000, null);
        for(int i = 0; i < numReads; i++) {
            time.sleep(readTime);
            throttler.maybeThrottle(readSize);
        }
        long doneTime = time.milliseconds();
        long bytesRead = numReads * (long) readSize;
        double unthrottledSecs = readTime * numReads / (double) Time.MS_PER_SECOND;
        double elapsedSecs = (double) (doneTime - startTime) / Time.MS_PER_SECOND;
        double observedRate = bytesRead / elapsedSecs;
        double unthrottledRate = bytesRead / unthrottledSecs;
        if (unthrottledRate < throttledRate) {
            // System.out.println("unthrottledRate (" + unthrottledRate + ") < throttledRate (" + throttledRate + ")");
            assertEquals(unthrottledRate, observedRate, 0.01);
        } else {
            double percentMiss = Math.abs(observedRate - throttledRate) / throttledRate;
            String message = "Observed I/O rate should be within 20% of throttle limit: " +
                    "percent difference = " + percentMiss * 100 + "%" +
                    ", unthrottled = " + unthrottledRate +
                    ", observed (throttled) = " + observedRate +
                    ", expected = " + throttledRate;
            // System.out.println(message);
            assertTrue(message,
                    percentMiss < 0.2);
        }
    }
}