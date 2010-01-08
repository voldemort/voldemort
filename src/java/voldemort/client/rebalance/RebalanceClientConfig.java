package voldemort.client.rebalance;

import java.util.Properties;

import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.utils.Props;

public class RebalanceClientConfig extends AdminClientConfig {

    private int maxParallelRebalancing = 1;
    private int rebalancingClientTimeoutSeconds = 7 * 24 * 60 * 60;
    private boolean deleteAfterRebalancingEnabled;

    public static String MaxParallelRebalancingString = "max.parallel.rebalancing";
    public static String RebalancingClientTimeoutSeconds = "rebalancing.client.timeout.seconds";
    public static String EnableDeleteAfterRebalancing = "enable.delete.after.rebalancing";

    public RebalanceClientConfig(Properties properties) {
        super(properties);
        Props props = new Props(properties);

        if(props.containsKey(MaxParallelRebalancingString))
            this.setMaxParallelRebalancing(props.getInt(MaxParallelRebalancingString));

        if(props.containsKey(RebalancingClientTimeoutSeconds))
            this.setRebalancingClientTimeoutSeconds(props.getInt(RebalancingClientTimeoutSeconds));

        if(props.containsKey(EnableDeleteAfterRebalancing))
            this.setDeleteAfterRebalancingEnabled(props.getBoolean(EnableDeleteAfterRebalancing));

    }

    public RebalanceClientConfig() {
        this(new Properties());
    }

    public void setMaxParallelRebalancing(int maxParallelRebalancing) {
        this.maxParallelRebalancing = maxParallelRebalancing;
    }

    public int getMaxParallelRebalancing() {
        return maxParallelRebalancing;
    }

    public void setRebalancingClientTimeoutSeconds(int rebalancingTimeoutSeconds) {
        this.rebalancingClientTimeoutSeconds = rebalancingTimeoutSeconds;
    }

    public int getRebalancingClientTimeoutSeconds() {
        return rebalancingClientTimeoutSeconds;
    }

    public boolean isDeleteAfterRebalancingEnabled() {
        return deleteAfterRebalancingEnabled;
    }

    public void setDeleteAfterRebalancingEnabled(boolean deleteAfterRebalancingEnabled) {
        this.deleteAfterRebalancingEnabled = deleteAfterRebalancingEnabled;
    }

}
