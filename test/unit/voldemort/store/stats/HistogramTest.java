/*
 * Copyright 2012 LinkedIn, Inc
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

package voldemort.store.stats;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class HistogramTest {

    private Histogram histogram;

    @Before
    public void setUp() {
        histogram = new Histogram(10, 5);
        histogram.insert(1);
        histogram.insert(6);
        histogram.insert(11);
        histogram.insert(16);
        histogram.insert(21);
        histogram.insert(26);
        histogram.insert(31);
        histogram.insert(36);
        histogram.insert(41);
        histogram.insert(46);
        histogram.insert(56);

        // Test that exceeding the size of the structure merely increments
        // the last bucket
        histogram.insert(66);
        histogram.insert(76);
    }

    @Test
    public void test50thQuartile() {
        assertEquals(histogram.getQuantile(0.50), 30);
    }

    @Test
    public void test95thQuartile() {
        assertEquals(histogram.getQuantile(0.95), 45);
    }

    @Test
    public void test99thQuartile() {
        assertEquals(histogram.getQuantile(0.99), 45);
    }

    @Test
    public void testResetHistogram() {

        Histogram resetingHistogram = new Histogram(10, 1, 10);
        // tests that the functionality is still working
        for(long data = 0; data < 5; data++) {
            for(int loop = 0; loop <= data; loop++) {
                resetingHistogram.insert(data);
            }
        }
        assertEquals(3, resetingHistogram.getQuantile(0.50));
        assertEquals(4, resetingHistogram.getQuantile(0.99));
        assertEquals(2.67, resetingHistogram.getAverage(), 0.01);

        // tests that once enough time passes, old data will be discarded
        try {
            Thread.sleep(10);
        } catch(InterruptedException ie) {}

        assertEquals(0, resetingHistogram.getQuantile(0.50));
        assertEquals(0, resetingHistogram.getQuantile(0.99));
        assertEquals(0.0, resetingHistogram.getAverage(), 0.0);
    }

    @Test
    public void testUpperBoundaryCondition() {
        Histogram h = new Histogram(100, 1);
        h.insert(98);
        h.insert(99);
        h.insert(100); // Should bucket with 99
        h.insert(101); // Should bucket with 99

        assertEquals(h.getQuantile(0.24), 98);
        assertEquals(h.getQuantile(0.26), 99);
    }

    @Test
    public void testLowerBoundaryCondition() {
        Histogram h = new Histogram(100, 1);
        h.insert(-1); // Should not be bucketed
        h.insert(0);
        h.insert(1);

        assertEquals(h.getQuantile(0.49), 0);
        assertEquals(h.getQuantile(0.51), 1);
    }
}
