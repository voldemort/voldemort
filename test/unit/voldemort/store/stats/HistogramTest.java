package voldemort.store.stats;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
    }
    
    @Test
    public void testAverage() {
        assertEquals(histogram.getQuantile(0.50), 20);
    }
    
    @Test
    public void test95thQuartile() {
        assertEquals(histogram.getQuantile(0.95), 45);
    }
    
    @Test
    public void test99thQuartile() {
        assertEquals(histogram.getQuantile(0.95), 45);
    }
    
}
