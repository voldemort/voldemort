package voldemort.client.rebalance;

import java.util.Arrays;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Run a version of RebalanceTests with a lot more keys.
 * 
 */
@RunWith(Parameterized.class)
public class RebalanceLongTest extends RebalanceTest {

    private final int NUM_KEYS = 10100;

    public RebalanceLongTest(boolean useNio, boolean useDonorBased) {
        super(useNio, useDonorBased);
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true, true }, { true, false }, { false, true },
                { false, false } });
    }

    @Override
    protected int getNumKeys() {
        return NUM_KEYS;
    }

}
