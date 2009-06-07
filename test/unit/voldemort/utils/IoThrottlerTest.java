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
import voldemort.MockTime;

public class IoThrottlerTest extends TestCase {

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
        long bytesRead = numReads * (long) readSize;
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
