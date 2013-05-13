/*
 * Copyright 2013 LinkedIn, Inc
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
public class ZonedRebalanceLongTest extends AbstractZonedRebalanceTest {

    private final int NUM_KEYS = 10100;

    // TODO: Add back donor-based tests. These tests are broken because it is
    // near impossible to get the replica-type handshake correct between the
    // client & server. Once replicaTypes are removed from the fetchEntries code
    // paths (e.g.,
    // DonorBasedRebalanceAsyncOperation.fetchEntriesForStealersPartitionScan),
    // then donor-based code should work again.
    // public RebalanceLongTest(boolean useNio, boolean useDonorBased) {
    public ZonedRebalanceLongTest(boolean useNio) {
        super(useNio, false);
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
