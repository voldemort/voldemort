package voldemort.server;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.store.metadata.MetadataStore;

public class ServerState {
    private final MetadataStore store;

    public ServerState(MetadataStore store) {
        this.store = store;
    }

    private int isEnabled(boolean val) {
        // It mimics the Unix return codes, 0 -> success, 1 -> false
        return val ? 0 : 1;
    }

    @JmxGetter(name = "state", description = "State of the Server 0 -> normal/ 1 -> offline/ 2 -> rebalancing")
    public int getState() {
        return store.getServerStateLocked().ordinal();
    }

    @JmxGetter(name = "slopStreamingState", description = "Slop Streaming 0 -> enabled, 1 -> disabled ")
    public int getSlopStreamingState() {
        return isEnabled(store.getSlopStreamingEnabledLocked());
    }

    @JmxGetter(name = "partitionStreamingState", description = " Partition streaming 0 -> enabled, 1 -> disabled ")
    public int getPartitionStreamingState() {
        return isEnabled(store.getPartitionStreamingEnabledLocked());
    }
    
    @JmxGetter(name = "readOnlyFetchingState", description = " read only fetch  0 -> enabled, 1 -> disabled ")
    public int getReadOnlyFetchState() {
        return isEnabled(store.getReadOnlyFetchEnabledLocked());
    }

    @JmxGetter(name = "quotaEnforcingState", description = " quota enforcing  0 -> enabled, 1 -> disabled ")
    public int getQuotaEnforcingState() {
        return isEnabled(store.getQuotaEnforcingEnabledLocked());
    }
}
