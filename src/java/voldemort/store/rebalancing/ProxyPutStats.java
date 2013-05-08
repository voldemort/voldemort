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

package voldemort.store.rebalancing;

import java.util.concurrent.atomic.AtomicLong;

import voldemort.annotations.jmx.JmxGetter;

/**
 * Statistics on Proxy puts issued from the redirecting store
 * 
 */
public class ProxyPutStats {

    private AtomicLong numProxyPutFailures;

    private AtomicLong numPendingProxyPuts;

    private ProxyPutStats parent;

    public ProxyPutStats(ProxyPutStats parent) {
        this.numPendingProxyPuts = new AtomicLong();
        this.numProxyPutFailures = new AtomicLong();
        this.parent = parent;
    }

    public void reportProxyPutSubmission() {
        this.numPendingProxyPuts.incrementAndGet();
        if(this.parent != null) {
            this.parent.reportProxyPutSubmission();
        }
    }

    public void reportProxyPutCompletion() {
        this.numPendingProxyPuts.decrementAndGet();
        if(this.parent != null) {
            this.parent.reportProxyPutCompletion();
        }
    }

    public void reportProxyPutFailure() {
        this.reportProxyPutCompletion();
        this.numProxyPutFailures.incrementAndGet();
        if(this.parent != null) {
            this.parent.reportProxyPutFailure();
        }
    }

    @JmxGetter(name = "numProxyPutFailures")
    public long getNumProxyPutFailures() {
        return numProxyPutFailures.get();
    }

    @JmxGetter(name = "numPendingProxyPuts")
    public long getNumPendingProxyPuts() {
        return numPendingProxyPuts.get();
    }
}
