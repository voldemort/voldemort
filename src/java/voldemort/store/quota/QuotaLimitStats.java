/*
 * Copyright 2008-2014 LinkedIn, Inc
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

package voldemort.store.quota;

import java.util.concurrent.atomic.AtomicLong;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.store.stats.Tracked;

public class QuotaLimitStats {

    private final AtomicLong rateLimitedGets;

    private final AtomicLong rateLimitedPuts;

    private final AtomicLong rateLimitedGetAlls;

    private final AtomicLong rateLimitedDeletes;

    private final QuotaLimitStats parent;

    public QuotaLimitStats(QuotaLimitStats parent) {
        rateLimitedGets = new AtomicLong();
        rateLimitedPuts = new AtomicLong();
        rateLimitedGetAlls = new AtomicLong();
        rateLimitedDeletes = new AtomicLong();
        this.parent = parent;
    }

    private void reportRateLimitedGet() {
        rateLimitedGets.incrementAndGet();
        if(parent != null) {
            parent.reportRateLimitedGet();
        }
    }

    private void reportRateLimitedPut() {
        rateLimitedPuts.incrementAndGet();
        if(parent != null) {
            parent.reportRateLimitedPut();
        }
    }

    private void reportRateLimitedGetAll() {
        rateLimitedGetAlls.incrementAndGet();
        if(parent != null) {
            parent.reportRateLimitedGetAll();
        }
    }

    private void reportRateLimitedDelete() {
        rateLimitedDeletes.incrementAndGet();
        if(parent != null) {
            parent.reportRateLimitedDelete();
        }
    }

    public void reportRateLimitedOp(Tracked op) {
        if(Tracked.GET == op) {
            reportRateLimitedGet();
        } else if(Tracked.PUT == op) {
            reportRateLimitedPut();
        } else if(Tracked.GET_ALL == op) {
            reportRateLimitedGetAll();
        } else if(Tracked.DELETE == op) {
            reportRateLimitedDelete();
        }
    }

    @JmxGetter(name = "rateLimitedGets", description = "Counter of Number of GETs rate limited")
    public long getRateLimitedGets() {
        return rateLimitedGets.get();
    }

    @JmxGetter(name = "rateLimitedPuts", description = "Counter of Number of PUTs rate limited")
    public long getRateLimitedPuts() {
        return rateLimitedPuts.get();
    }

    @JmxGetter(name = "rateLimitedGetAlls", description = "Counter of Number of GET_ALLs rate limited")
    public long getRateLimitedGetAlls() {
        return rateLimitedGetAlls.get();
    }

    @JmxGetter(name = "rateLimitedDeletes", description = "Counter of Number of DELETEs rate limited")
    public long getRateLimitedDeletes() {
        return rateLimitedDeletes.get();
    }
}
