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
package voldemort.store.stats;

import org.junit.Test;

import java.util.EnumSet;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class StoreStatsJmxTest {
    @Test
    public void getThroughPutShouldBeSumOfThroughputOperations() {
        StoreStats stats = mock(StoreStats.class);

        when(stats.getThroughput(eq(Tracked.GET))    ).thenReturn(   1.0f);
        when(stats.getThroughput(eq(Tracked.GET_ALL))).thenReturn(  20.0f);
        when(stats.getThroughput(eq(Tracked.DELETE)) ).thenReturn( 300.0f);
        when(stats.getThroughput(eq(Tracked.PUT))    ).thenReturn(4000.0f);

        StoreStatsJmx storeStatsJmx = new StoreStatsJmx(stats);

        assertEquals(4321.0f, storeStatsJmx.getOperationThroughput(), 0.1f);

        for(Tracked op : EnumSet.of(Tracked.GET, Tracked.GET_ALL, Tracked.DELETE, Tracked.PUT)) {
            verify(stats, times(1)).getThroughput(op);
        }
    }

}
