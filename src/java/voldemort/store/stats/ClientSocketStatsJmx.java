/*
 * Copyright 2008-2011 LinkedIn, Inc
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
import voldemort.utils.Time;

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

    @JmxGetter(name = "socketsCreated", description = "Number of sockets created.")
    public int getConnectionsCreated() {
        return stats.getConnectionsCreated();
    }

    @JmxGetter(name = "socketsDestroyed", description = "Number of sockets destroyed.")
    public int getConnectionsDestroyed() {
        return stats.getConnectionsDestroyed();
    }

    @JmxGetter(name = "socketsCheckedout", description = "Number of sockets checked out.")
    public int getConnectionsCheckinout() {
        return stats.getConnectionsCheckedout();
    }

    @JmxGetter(name = "waitMsAverage", description = "Average ms to wait to get a socket.")
    public double getWaitMsAverage() {
        return (double) stats.getAveWaitUs() / Time.US_PER_MS;
    }

    @JmxGetter(name = "waitMsQ50th", description = "50th percentile wait time to get a connection.")
    public double get() {
        return (double) stats.getWaitHistogram().getQuantile(0.5) / Time.US_PER_MS;
    }

    @JmxGetter(name = "waitMsQ99th", description = "99th percentile wait time to get a connection.")
    public double getWaitMsQ99th() {
        return (double) stats.getWaitHistogram().getQuantile(0.99) / Time.US_PER_MS;
    }

    @JmxGetter(name = "socketsActive", description = "Total number of sockets, checkedin and checkout.")
    public int getConnActive() {
        int result = -1;
        try {
            result = stats.getConnectionsActive(stats.getDestination());
        } catch(Exception e) {}
        return result;
    }

    @JmxGetter(name = "socketsInPool", description = "Total number of sockets in the pool.")
    public int getConnAvailable() {
        int result = -1;
        try {
            result = stats.getConnectionsInPool(stats.getDestination());
        } catch(Exception e) {}
        return result;
    }

    @JmxGetter(name = "monitoringInterval", description = "The number of checkouts over which performance statics are calculated.")
    public int getMonitoringInterval() {
        return stats.getMonitoringInterval();
    }

    @JmxSetter(name = "monitoringInterval", description = "The number of checkouts over which performance statics are calculated.")
    public void setMonitoringInterval(int count) {
        if(count <= 0)
            throw new IllegalArgumentException("Monitoring interval must be a positive number.");
        stats.setMonitoringInterval(count);
    }
}
