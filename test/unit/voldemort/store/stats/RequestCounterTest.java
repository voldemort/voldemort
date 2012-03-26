package voldemort.store.stats;

import org.junit.Before;
import org.junit.Test;

public class RequestCounterTest {

    private RequestCounter requestCounter;

    @Before
    public void setUp() {
        // Initialize the RequestCounter with a histogram
        requestCounter = new RequestCounter(10000, true);
    }

    @Test
    public void test() {
        long val = 234;
        requestCounter.addRequest(val);
    }

    @Test
    public void testLargeValues() {
        long val = 999999992342756424l;
        requestCounter.addRequest(val);
    }

}
