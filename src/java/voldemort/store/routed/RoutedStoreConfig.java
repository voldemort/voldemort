package voldemort.store.routed;

import voldemort.client.ClientConfig;
import voldemort.client.TimeoutConfig;
import voldemort.cluster.Zone;
import voldemort.server.VoldemortConfig;

public class RoutedStoreConfig {

    private boolean repairReads = true;
    private TimeoutConfig timeoutConfig = new TimeoutConfig();
    private boolean isJmxEnabled = false;
    private int jmxId = 0;
    private int clientZoneId = Zone.DEFAULT_ZONE_ID;

    public RoutedStoreConfig() {}

    public RoutedStoreConfig(ClientConfig clientConfig) {
        this.isJmxEnabled = clientConfig.isJmxEnabled();
        this.clientZoneId = clientConfig.getClientZoneId();
        this.timeoutConfig = clientConfig.getTimeoutConfig();
    }

    public RoutedStoreConfig(VoldemortConfig voldemortConfig) {
        this.isJmxEnabled = voldemortConfig.isJmxEnabled();
        this.timeoutConfig = voldemortConfig.getTimeoutConfig();
    }

    public boolean getRepairReads() {
        return repairReads;
    }

    public RoutedStoreConfig setRepairReads(boolean repairReads) {
        this.repairReads = repairReads;
        return this;
    }

    public TimeoutConfig getTimeoutConfig() {
        return timeoutConfig;
    }

    public RoutedStoreConfig setTimeoutConfig(TimeoutConfig timeoutConfig) {
        this.timeoutConfig = timeoutConfig;
        return this;
    }

    public int getClientZoneId() {
        return clientZoneId;
    }

    public RoutedStoreConfig setClientZoneId(int clientZoneId) {
        this.clientZoneId = clientZoneId;
        return this;
    }

    public boolean isJmxEnabled() {
        return isJmxEnabled;
    }

    public RoutedStoreConfig setJmxEnabled(boolean isJmxEnabled) {
        this.isJmxEnabled = isJmxEnabled;
        return this;
    }

    public int getJmxId() {
        return jmxId;
    }

    public RoutedStoreConfig setJmxId(int jmxId) {
        this.jmxId = jmxId;
        return this;
    }
}
