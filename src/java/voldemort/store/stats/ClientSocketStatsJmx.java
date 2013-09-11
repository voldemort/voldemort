/*
 * Copyright 2008-2012 LinkedIn, Inc
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

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxSetter;

/**
 * 
 * A wrapper class to expose client socket stats via JMX
 * 
 */

@JmxManaged(description = "Voldemort socket pool.")
public class ClientSocketStatsJmx {

    private final ClientSocketStats stats;

    /**
     * Class for JMX
     */
    public ClientSocketStatsJmx(ClientSocketStats stats) {
        this.stats = stats;
    }

    @JmxGetter(name = "socketsCreated", description = "Number of sockets created. Aggregate measure based on current monitoring interval.")
    public int getConnectionsCreated() {
        return stats.getConnectionsCreated();
    }

    @JmxGetter(name = "socketsDestroyed", description = "Number of sockets destroyed. Aggregate measure based on current monitoring interval.")
    public int getConnectionsDestroyed() {
        return stats.getConnectionsDestroyed();
    }

    @JmxGetter(name = "socketsCheckedout", description = "Number of sockets checked out. Aggregate measure based on current monitoring interval.")
    public int getConnectionsCheckinout() {
        return stats.getCheckoutCount();
    }

    @JmxGetter(name = "waitMsAverage", description = "Average ms wait to do a synchronous socket checkout. Aggregate measure based on current monitoring interval.")
    public double getWaitMsAverage() {
        return stats.getAvgCheckoutWaitMs();
    }

    @JmxGetter(name = "waitMsQ50th", description = "50th percentile wait time (ms) to get a connection. Aggregate measure based on current monitoring interval.")
    public double getWaitMsQ50th() {
        return stats.getCheckoutTimeMsQ50th();
    }

    @JmxGetter(name = "waitMsQ99th", description = "99th percentile wait time (ms) to get a connection. Aggregate measure based on current monitoring interval.")
    public double getWaitMsQ99th() {
        return stats.getCheckoutTimeMsQ99th();
    }

    @JmxGetter(name = "checkoutQueueLengthQ50th", description = "50th percentile blocking queue length to get a connection. Aggregate measure based on current monitoring interval.")
    public double getCheckoutQueueLengthQ50th() {
        return stats.getCheckoutQueueLengthHistogram().getQuantile(0.5);
    }

    @JmxGetter(name = "checkoutQueueLength99th", description = "99th percentile blocking queue length to get a connection. Aggregate measure based on current monitoring interval.")
    public double getCheckoutQueueLengthQ99th() {
        return stats.getCheckoutQueueLengthHistogram().getQuantile(0.99);
    }
  
    @JmxGetter(name = "resourceRequestCount", description = "Number of resource requests made. Aggregate measure based on current monitoring interval.")
    public int getResourceRequestCount() {
        return stats.resourceRequestCount();
    }

    @JmxGetter(name = "resourceRequestWaitMsAverage", description = "Average ms wait to do an asynchronous socket checkout. Aggregate measure based on current monitoring interval.")
    public double getResourceRequestWaitMsAverage() {
        return stats.getAvgResourceRequestTimeMs();
    }

    @JmxGetter(name = "resourceRequestWaitMsQ50th", description = "50th percentile wait time (ms) to do an asynchronous socket checkout. Aggregate measure based on current monitoring interval.")
    public double getResourceRequestWaitMsQ50th() {
        return  stats.getResourceRequestTimeMsQ50th();
    }

    @JmxGetter(name = "resourceRequestWaitMsQ99th", description = "99th percentile wait time (ms) to do an asynchronous socket checkout. Aggregate measure based on current monitoring interval.")
    public double getResourceRequestWaitMsQ99th() {
        return stats.getResourceRequestTimeMsQ99th();
    }
    
    @JmxGetter(name = "connectionEstablishmentMsAverage", description = "Average time (ms) to establish a connection. Aggregate measure based on current monitoring interval.")
    public double getConnectionEstablishmentMsAverage() {
        return stats.getAvgConnectionEstablishmentMs();
    }
    
    @JmxGetter(name = "connectionEstablishmentMsQ99th", description = "99th percentile wait time (ms) to establish a connection. Aggregate measure based on current monitoring interval.")
    public double getConnectionEstablishmentQ99th() {
        return stats.getConnectionEstablishmentQ99LatencyMs();
    }
    
    @JmxGetter(name = "opTimeMsAverage", description = "Average time (ms) to establish a connection. Aggregate measure based on current monitoring interval.")
    public double getopTimeMsAverage() {
        return stats.getAvgOpTimeMs();
    }
    
    @JmxGetter(name = "opTimeMsQ95th", description = "95th percentile time (ms) to do an operation. Aggregate measure based on current monitoring interval.")
    public double getopTimeMsQ95th() {
        return stats.getopTimeMsQ95th();
    }
    
    @JmxGetter(name = "opTimeMsQ99th", description = "99th percentile time (ms) to do an operation. Aggregate measure based on current monitoring interval.")
    public double getopTimeMsQ99th() {
        return stats.getopTimeMsQ99th();
    }

    @JmxGetter(name = "resourceRequestQueueLengthQ50th", description = "50th percentile asynchronous queue length to get a connection. Aggregate measure based on current monitoring interval.")
    public double getResourceRequestQueueLengthQ50th() {
        return stats.getResourceRequestQueueLengthHistogram().getQuantile(0.5);
    }

    @JmxGetter(name = "resourceRequestQueueLengthQ99th", description = "99th percentile asynchronous queue length to get a connection. Aggregate measure based on current monitoring interval.")
    public double getResourceRequestQueueLengthQ99th() {
        return stats.getResourceRequestQueueLengthHistogram().getQuantile(0.99);
    }

    @JmxGetter(name = "socketsActive", description = "Total number of sockets, checkedin and checkout. Instantaneous measure (i.e., object is polled for current value).")
    public int getConnActive() {
        int result = -1;
        try {
            result = stats.getConnectionsActive(stats.getDestination());
        } catch(Exception e) {}
        return result;
    }

    @JmxGetter(name = "socketsInPool", description = "Total number of sockets in the pool. Instantaneous measure (i.e., object is polled for current value).")
    public int getConnAvailable() {
        int result = -1;
        try {
            result = stats.getConnectionsInPool(stats.getDestination());
        } catch(Exception e) {}
        return result;
    }

    @JmxGetter(name = "monitoringInterval", description = "The maximum number of checkouts plus resource requests over which performance statistics are calculated.")
    public int getMonitoringInterval() {
        return stats.getMonitoringInterval();
    }

    @JmxGetter(name = "monitoringCheckoutSampleSize", description = "The number of checkout samples currently included in (pertinent) aggregate measures.")
    public int getMonitoringCheckoutSampleSize() {
        return stats.getCheckoutCount();
    }

    @JmxGetter(name = "monitoringResourceRequestSampleSize", description = "The number of resource request samples currently included in (pertinent) aggregate measures.")
    public int getMonitoringResourceRequestSampleSize() {
        return stats.resourceRequestCount();
    }

    @JmxSetter(name = "monitoringInterval", description = "The number of checkouts over which performance statistics are calculated.")
    public void setMonitoringInterval(int count) {
        if(count <= 0)
            throw new IllegalArgumentException("Monitoring interval must be a positive number.");
        stats.setMonitoringInterval(count);
    }
}
