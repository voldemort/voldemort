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
// TODO: rename this to NonZonedRebalanceLongTest (or some such, whatever the
// pattern is). And, add a long ZonedRebalance equivalent test.
@RunWith(Parameterized.class)
public class RebalanceLongTest extends RebalanceTest {

    private final int NUM_KEYS = 10100;

    // TODO: Add back donor-based tests. These tests are broken because it is
    // near impossible to get the replica-type handshake correct between the
    // client & server. Once replicaTypes are removed from the fetchEntries code
    // paths (e.g.,
    // DonorBasedRebalanceAsyncOperation.fetchEntriesForStealersPartitionScan),
    // then donor-based code should work again.
    // public RebalanceLongTest(boolean useNio, boolean useDonorBased) {
    public RebalanceLongTest(boolean useNio) {
        super(useNio);
    }

    @Parameters
    public static Collection<Object[]> configs() {
        /*-
        return Arrays.asList(new Object[][] { { true, true }, { true, false }, { false, true },
                { false, false } });
         */
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    @Override
    protected int getNumKeys() {
        return NUM_KEYS;
    }

}
