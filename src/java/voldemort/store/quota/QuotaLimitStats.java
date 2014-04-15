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
import voldemort.store.stats.SimpleCounter;
import voldemort.store.stats.Tracked;

public class QuotaLimitStats {

    private static final int QUOTA_STATS_RESET_INTERVAL_MS = 60000;

    private final AtomicLong rateLimitedGets;

    private final AtomicLong rateLimitedPuts;

    private final AtomicLong rateLimitedGetAlls;

    private final AtomicLong rateLimitedDeletes;

    private final SimpleCounter pctGetQuotaUsed;

    private final SimpleCounter pctPutQuotaUsed;

    private final SimpleCounter pctGetAllQuotaUsed;

    private final SimpleCounter pctDeleteQuotaUsed;

    private final QuotaLimitStats parent;

    public QuotaLimitStats(QuotaLimitStats parent) {
        this(parent, QUOTA_STATS_RESET_INTERVAL_MS);
    }

    public QuotaLimitStats(QuotaLimitStats parent, long resetIntervalMs) {
        rateLimitedGets = new AtomicLong();
        rateLimitedPuts = new AtomicLong();
        rateLimitedGetAlls = new AtomicLong();
        rateLimitedDeletes = new AtomicLong();

        pctGetQuotaUsed = new SimpleCounter(resetIntervalMs);
        pctPutQuotaUsed = new SimpleCounter(resetIntervalMs);
        pctDeleteQuotaUsed = new SimpleCounter(resetIntervalMs);
        pctGetAllQuotaUsed = new SimpleCounter(resetIntervalMs);

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

    private void reportGetQuotaUsedPct(long pctUsed) {
        pctGetQuotaUsed.count(pctUsed);
        if(parent != null) {
            parent.reportGetQuotaUsedPct(pctUsed);
        }
    }

    private void reportPutQuotaUsedPct(long pctUsed) {
        pctPutQuotaUsed.count(pctUsed);
        if(parent != null) {
            parent.reportPutQuotaUsedPct(pctUsed);
        }
    }

    private void reportGetAllQuotaUsedPct(long pctUsed) {
        pctGetAllQuotaUsed.count(pctUsed);
        if(parent != null) {
            parent.reportGetAllQuotaUsedPct(pctUsed);
        }
    }

    private void reportDeleteQuotaUsedPct(long pctUsed) {
        pctDeleteQuotaUsed.count(pctUsed);
        if(parent != null) {
            parent.reportDeleteQuotaUsedPct(pctUsed);
        }
    }

    public void reportQuotaUsed(Tracked op, long pctUsed) {
        if(Tracked.GET == op) {
            reportGetQuotaUsedPct(pctUsed);
        } else if(Tracked.PUT == op) {
            reportPutQuotaUsedPct(pctUsed);
        } else if(Tracked.GET_ALL == op) {
            reportGetAllQuotaUsedPct(pctUsed);
        } else if(Tracked.DELETE == op) {
            reportDeleteQuotaUsedPct(pctUsed);
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

    /**
     * Defensive checks against measured usage pct
     * 
     * @param pctUsedVal
     * @return
     */
    private long computePctUsed(Double pctUsedVal) {
        if(pctUsedVal <= 1.0) {
            return 0;
        } else {
            return Math.round(pctUsedVal);
        }
    }

    @JmxGetter(name = "GetQuotaUsedPct", description = "Average usage of Get quota in the last 60 secs")
    public long getQuotaPctUsedGet() {
        return computePctUsed(pctGetQuotaUsed.getAvgEventValue());
    }

    @JmxGetter(name = "GetAllQuotaUsedPct", description = "Average usage of GetAll quota in the last 60 secs")
    public long getQuotaPctUsedGetAll() {
        return computePctUsed(pctGetAllQuotaUsed.getAvgEventValue());
    }

    @JmxGetter(name = "PutQuotaUsedPct", description = "Average usage of Put quota in the last 60 secs")
    public long getQuotaPctUsedPut() {
        return computePctUsed(pctPutQuotaUsed.getAvgEventValue());
    }

    @JmxGetter(name = "DeleteQuotaUsedPct", description = "Average usage of Delete quota in the last 60 secs")
    public long getQuotaPctUsedDelete() {
        return computePctUsed(pctDeleteQuotaUsed.getAvgEventValue());
    }
}
